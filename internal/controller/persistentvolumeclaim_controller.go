/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/utils"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

// ErrNoStorageRequests is an error which is returned in case a PVC does not
// have (should not happen, but still) the .spec.resources.requests.storage
// field.
var ErrNoStorageRequests = errors.New("no .spec.resources.requests.storage field")

// ErrNoStorageStatus is an error which is returned in case a PVC does not have
// (should not happen, but still) the .status.capacity.storage field.
var ErrNoStorageStatus = errors.New("no .status.capacity.storage field")

// PersistentVolumeClaimReconciler reconciles a PersistentVolumeClaim object
type PersistentVolumeClaimReconciler struct {
	client        client.Client
	scheme        *runtime.Scheme
	eventCh       chan event.GenericEvent
	eventRecorder record.EventRecorder
}

// Option is a function which configures the [PersistentVolumeClaimReconciler].
type Option func(r *PersistentVolumeClaimReconciler)

// New creates a new [PersistentVolumeClaimReconciler] and configures it with
// the given options.
func New(opts ...Option) (*PersistentVolumeClaimReconciler, error) {
	r := &PersistentVolumeClaimReconciler{}
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

// WithClient configures the [PersistentVolumeClaimReconciler] with the given
// client.
func WithClient(c client.Client) Option {
	opt := func(r *PersistentVolumeClaimReconciler) {
		r.client = c
	}

	return opt
}

// WithScheme configures the [PersistentVolumeClaimReconciler] with the given scheme
func WithScheme(s *runtime.Scheme) Option {
	opt := func(r *PersistentVolumeClaimReconciler) {
		r.scheme = s
	}

	return opt
}

// WithEventChannel configures the [PersistentVolumeClaimReconciler] to use the
// given channel for receiving reconcile events.
func WithEventChannel(ch chan event.GenericEvent) Option {
	opt := func(r *PersistentVolumeClaimReconciler) {
		r.eventCh = ch
	}

	return opt
}

// WithEventRecorder configures the [PersistentVolumeClaimReconciler] to use the
// given event recorder.
func WithEventRecorder(recorder record.EventRecorder) Option {
	opt := func(r *PersistentVolumeClaimReconciler) {
		r.eventRecorder = recorder
	}

	return opt
}

//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims/status,verbs=get
//+kubebuilder:rbac:groups=core,resources=events,verbs=create;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the PersistentVolumeClaim object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.17.0/pkg/reconcile
func (r *PersistentVolumeClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var obj corev1.PersistentVolumeClaim
	err := r.client.Get(ctx, req.NamespacedName, &obj)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// This kind of an error is something we should not retry on
	if err := utils.ValidatePersistentVolumeClaimAnnotations(&obj); err != nil {
		logger.Info("refusing to proceed with reconciling", "reason", err.Error())
		return ctrl.Result{}, nil
	}

	// Make sure that the PVC is not being modified at the moment.  Note,
	// that we are not treating the following status conditions as errors,
	// as these are transient conditions.
	if utils.IsPersistentVolumeClaimConditionTrue(&obj, corev1.PersistentVolumeClaimResizing) {
		logger.Info("resize has been started")
		return ctrl.Result{}, nil
	}

	if utils.IsPersistentVolumeClaimConditionTrue(&obj, corev1.PersistentVolumeClaimFileSystemResizePending) {
		logger.Info("filesystem resize is pending")
		return ctrl.Result{}, nil
	}

	if utils.IsPersistentVolumeClaimConditionTrue(&obj, corev1.PersistentVolumeClaimVolumeModifyingVolume) {
		logger.Info("volume is being modified")
		return ctrl.Result{}, nil
	}

	prevSizeVal := utils.GetAnnotation(&obj, annotation.PrevSize, "0Gi")
	prevSize, err := resource.ParseQuantity(prevSizeVal)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cannot parse prev-size: %w", err)
	}

	currSpecSize := obj.Spec.Resources.Requests.Storage()
	if currSpecSize.IsZero() {
		return ctrl.Result{}, ErrNoStorageRequests
	}

	currStatusSize := obj.Status.Capacity.Storage()
	if currStatusSize.IsZero() {
		return ctrl.Result{}, ErrNoStorageStatus
	}

	// If previously recorded size is equal to the current status it means
	// we are still waiting for the resize to complete
	if prevSize.Equal(*currStatusSize) {
		logger.Info("persistent volume claim is still being resized")
		return ctrl.Result{}, nil
	}

	// Calculate the new size
	increaseByVal := utils.GetAnnotation(&obj, annotation.IncreaseBy, common.DefaultIncreaseByValue)
	increaseBy, err := utils.ParsePercentage(increaseByVal)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cannot parse increase-by value: %w", err)
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
	maxCapacityVal := utils.GetAnnotation(&obj, annotation.MaxCapacity, "0Gi")
	maxCapacity, err := resource.ParseQuantity(maxCapacityVal)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("cannot parse max-capacity: %w", err)
	}

	if newSize.Value() > maxCapacity.Value() {
		r.eventRecorder.Eventf(
			&obj,
			corev1.EventTypeWarning,
			"MaxCapacityReached",
			"max capacity has been reached, will not resize",
			maxCapacity.String(),
		)
		logger.Info("max capacity reached")
		return ctrl.Result{}, nil
	}

	// And finally we should be good to resize now
	logger.Info("resizing persistent volume claim", "from", currSpecSize.String(), "to", newSize.String())
	r.eventRecorder.Eventf(
		&obj,
		corev1.EventTypeNormal,
		"ResizingStorage",
		"resizing storage from %s to %s",
		currSpecSize.String(),
		newSize.String(),
	)

	patch := client.MergeFrom(obj.DeepCopy())
	obj.Spec.Resources.Requests[corev1.ResourceStorage] = *newSize
	obj.Annotations[annotation.PrevSize] = currStatusSize.String()

	return ctrl.Result{}, r.client.Patch(ctx, &obj, patch)
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	src := &source.Channel{
		Source: r.eventCh,
	}
	handler := &handler.EnqueueRequestForObject{}

	return ctrl.NewControllerManagedBy(mgr).
		Named(common.ControllerName).
		WatchesRawSource(src, handler).
		Complete(r)
}
