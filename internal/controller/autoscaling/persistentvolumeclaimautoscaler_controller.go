// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package autoscaling

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	autoscalingv1alpha1 "github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
)

// ErrNoStorageRequests is an error which is returned in case a PVC does not
// have the .spec.resources.requests.storage field.
var ErrNoStorageRequests = errors.New("no .spec.resources.requests.storage field")

// ErrNoStorageStatus is an error which is returned in case a PVC does not have
// the .status.capacity.storage field.
var ErrNoStorageStatus = errors.New("no .status.capacity.storage field")

// PersistentVolumeClaimAutoscalerReconciler reconciles a
// [autoscalingv1alpha1.PersistentVolumeClaimAutoscaler] object.
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

// Reconcile implements the
// [sigs.k8s.io/controller-runtime/pkg/reconcile.Reconciler] interface.
func (r *PersistentVolumeClaimAutoscalerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// TODO(dnaeon): implement the logic

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeClaimAutoscalerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&autoscalingv1alpha1.PersistentVolumeClaimAutoscaler{}).
		Complete(r)
}
