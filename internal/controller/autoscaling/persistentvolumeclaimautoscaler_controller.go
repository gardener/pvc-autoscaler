// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package autoscaling

import (
	"context"
	"errors"
	"fmt"
	"math"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	v1alpha1 "github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/metrics"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

// ErrNoStorageRequests is an error which is returned in case a PVC does not
// have the .spec.resources.requests.storage field.
var ErrNoStorageRequests = errors.New("no .spec.resources.requests.storage field")

// ErrNoStorageStatus is an error which is returned in case a PVC does not have
// the .status.capacity.storage field.
var ErrNoStorageStatus = errors.New("no .status.capacity.storage field")

// PersistentVolumeClaimAutoscalerReconciler reconciles a
// [v1alpha1.PersistentVolumeClaimAutoscaler] object.
type PersistentVolumeClaimAutoscalerReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	eventCh       chan event.GenericEvent
	eventRecorder record.EventRecorder
}

var _ reconcile.Reconciler = &PersistentVolumeClaimAutoscalerReconciler{}

// Option is a function which configures the
// [PersistentVolumeClaimAutoscalerReconciler].
type Option func(r *PersistentVolumeClaimAutoscalerReconciler)

// New creates a new [PersistentVolumeClaimAutoscalerReconciler] and configures
// it with the given options.
func New(opts ...Option) (*PersistentVolumeClaimAutoscalerReconciler, error) {
	r := &PersistentVolumeClaimAutoscalerReconciler{}
	for _, opt := range opts {
		opt(r)
	}

	if r.eventRecorder == nil {
		return nil, common.ErrNoEventRecorder
	}

	if r.eventCh == nil {
		return nil, common.ErrNoEventChannel
	}

	return r, nil
}

// WithClient configures the [PersistentVolumeClaimAutoscalerReconciler] with
// the given [client.Client].
func WithClient(c client.Client) Option {
	opt := func(r *PersistentVolumeClaimAutoscalerReconciler) {
		r.client = c
	}

	return opt
}

// WithScheme configures the [PersistentVolumeClaimAutoscalerReconciler] with
// the given [runtime.Scheme].
func WithScheme(s *runtime.Scheme) Option {
	opt := func(r *PersistentVolumeClaimAutoscalerReconciler) {
		r.scheme = s
	}

	return opt
}

// WithEventChannel configures the [PersistentVolumeClaimAutoscalerReconciler]
// to use the given [event.Generic] channel for receiving reconcile events.
func WithEventChannel(ch chan event.GenericEvent) Option {
	opt := func(r *PersistentVolumeClaimAutoscalerReconciler) {
		r.eventCh = ch
	}

	return opt
}

// WithEventRecorder configures the [PersistentVolumeClaimAutoscalerReconciler]
// to use the given event [record.EventRecorder].
func WithEventRecorder(recorder record.EventRecorder) Option {
	opt := func(r *PersistentVolumeClaimAutoscalerReconciler) {
		r.eventRecorder = recorder
	}

	return opt
}

// +kubebuilder:rbac:groups=autoscaling.gardener.cloud,resources=persistentvolumeclaimautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling.gardener.cloud,resources=persistentvolumeclaimautoscalers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=autoscaling.gardener.cloud,resources=persistentvolumeclaimautoscalers/finalizers,verbs=update
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims/status,verbs=get
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
//+kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch

// Reconcile implements the
// [sigs.k8s.io/controller-runtime/pkg/reconcile.Reconciler] interface.
func (r *PersistentVolumeClaimAutoscalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pvca := &v1alpha1.PersistentVolumeClaimAutoscaler{}
	err := r.client.Get(ctx, req.NamespacedName, pvca)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	pvcObjKey := client.ObjectKey{Namespace: pvca.Namespace, Name: pvca.Spec.ScaleTargetRef.Name}
	pvcObj := &corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, pvcObjKey, pvcObj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	logger := log.FromContext(ctx).WithValues("pvc", pvcObj.Name)
	currSpecSize := pvcObj.Spec.Resources.Requests.Storage()
	currStatusSize := pvcObj.Status.Capacity.Storage()

	// Make sure that the PVC is not being modified at the moment.  Note,
	// that we are not treating the following status conditions as errors,
	// as these are transient conditions.
	if utils.IsPersistentVolumeClaimConditionTrue(pvcObj, corev1.PersistentVolumeClaimResizing) {
		logger.Info("resize has been started")
		condition := metav1.Condition{
			Type:    utils.ConditionTypeHealthy,
			Status:  metav1.ConditionFalse,
			Reason:  "Reconciling",
			Message: "Resize has been started",
		}
		return ctrl.Result{}, pvca.SetCondition(ctx, r.client, condition)
	}

	if utils.IsPersistentVolumeClaimConditionTrue(pvcObj, corev1.PersistentVolumeClaimFileSystemResizePending) {
		logger.Info("filesystem resize is pending")
		condition := metav1.Condition{
			Type:    utils.ConditionTypeHealthy,
			Status:  metav1.ConditionFalse,
			Reason:  "Reconciling",
			Message: "File system resize is pending",
		}
		return ctrl.Result{}, pvca.SetCondition(ctx, r.client, condition)
	}

	if utils.IsPersistentVolumeClaimConditionTrue(pvcObj, corev1.PersistentVolumeClaimVolumeModifyingVolume) {
		logger.Info("volume is being modified")
		condition := metav1.Condition{
			Type:    utils.ConditionTypeHealthy,
			Status:  metav1.ConditionFalse,
			Reason:  "Reconciling",
			Message: "Volume is being modified",
		}
		return ctrl.Result{}, pvca.SetCondition(ctx, r.client, condition)
	}

	// If previously recorded size is equal to the current status it means
	// we are still waiting for the resize to complete
	if pvca.Status.PrevSize.Equal(*currStatusSize) {
		logger.Info("persistent volume claim is still being resized")
		condition := metav1.Condition{
			Type:    utils.ConditionTypeHealthy,
			Status:  metav1.ConditionFalse,
			Reason:  "Reconciling",
			Message: "Persistent volume claim is still being resized",
		}
		return ctrl.Result{}, pvca.SetCondition(ctx, r.client, condition)
	}

	// Calculate the new size
	increaseBy, err := utils.ParsePercentage(pvca.Spec.IncreaseBy)
	if err != nil {
		eerr := fmt.Errorf("cannot parse increase-by value: %w", err)
		condition := metav1.Condition{
			Type:    utils.ConditionTypeHealthy,
			Status:  metav1.ConditionUnknown,
			Reason:  "Reconciling",
			Message: eerr.Error(),
		}
		return ctrl.Result{}, pvca.SetCondition(ctx, r.client, condition)
	}

	increment := float64(currSpecSize.Value()) * (increaseBy / 100.0)
	newSizeBytes := int64(math.Ceil((float64(currSpecSize.Value())+increment)/1073741824)) * 1073741824
	newSize := resource.NewQuantity(newSizeBytes, resource.BinarySI)

	// Check that we've got a valid new size. If we end up in any of these
	// cases below, it pretty much means the logic is broken, so we don't
	// want to retry any of them.
	cmp := newSize.Cmp(*currSpecSize)
	switch cmp {
	case 0:
		logger.Info("new and current size are the same")
		return ctrl.Result{}, nil
	case -1:
		logger.Info("new size is less than current")
		return ctrl.Result{}, nil
	}

	// We don't want to exceed the max capacity
	if newSize.Value() > pvca.Spec.MaxCapacity.Value() {
		r.eventRecorder.Eventf(
			pvcObj,
			corev1.EventTypeWarning,
			"MaxCapacityReached",
			"max capacity (%s) has been reached, will not resize",
			pvca.Spec.MaxCapacity.String(),
		)
		logger.Info("max capacity reached")
		metrics.MaxCapacityReachedTotal.WithLabelValues(pvcObj.Namespace, pvcObj.Name).Inc()
		condition := metav1.Condition{
			Type:    utils.ConditionTypeHealthy,
			Status:  metav1.ConditionFalse,
			Reason:  "Reconciling",
			Message: "Max capacity reached",
		}

		return ctrl.Result{}, pvca.SetCondition(ctx, r.client, condition)
	}

	// And finally we should be good to resize now
	logger.Info("resizing persistent volume claim", "from", currSpecSize.String(), "to", newSize.String())
	metrics.ResizedTotal.WithLabelValues(pvcObj.Namespace, pvcObj.Name).Inc()
	r.eventRecorder.Eventf(
		pvcObj,
		corev1.EventTypeNormal,
		"ResizingStorage",
		"resizing storage from %s to %s",
		currSpecSize.String(),
		newSize.String(),
	)

	// Update PVC and PVCA resources
	pvcPatch := client.MergeFrom(pvcObj.DeepCopy())
	pvcObj.Spec.Resources.Requests[corev1.ResourceStorage] = *newSize
	if err := r.client.Patch(ctx, pvcObj, pvcPatch); err != nil {
		return ctrl.Result{}, err
	}

	pvcaPatch := client.MergeFrom(pvca.DeepCopy())
	pvca.Status.PrevSize = *currStatusSize
	pvca.Status.NewSize = *newSize
	if err := r.client.Status().Patch(ctx, pvca, pvcaPatch); err != nil {
		return ctrl.Result{}, err
	}

	condition := metav1.Condition{
		Type:    utils.ConditionTypeHealthy,
		Status:  metav1.ConditionFalse,
		Reason:  "Reconciling",
		Message: fmt.Sprintf("Resizing from %s to %s", currSpecSize.String(), newSize.String()),
	}

	return ctrl.Result{}, pvca.SetCondition(ctx, r.client, condition)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeClaimAutoscalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	h := &handler.EnqueueRequestForObject{}
	src := source.Channel(r.eventCh, h)

	return ctrl.NewControllerManagedBy(mgr).
		Named(common.ControllerName).
		WatchesRawSource(src).
		Complete(r)
}
