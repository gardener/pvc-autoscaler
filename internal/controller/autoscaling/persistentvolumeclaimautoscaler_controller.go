// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package autoscaling

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	autoscalingv1alpha1 "github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
)

// PersistentVolumeClaimAutoscalerReconciler reconciles a
// [autoscalingv1alpha1.PersistentVolumeClaimAutoscaler] object.
type PersistentVolumeClaimAutoscalerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
