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

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const (
	// Name is the name of the controller
	Name = "pvc_autoscaler"
)

// PersistentVolumeClaimReconciler reconciles a PersistentVolumeClaim object
type PersistentVolumeClaimReconciler struct {
	client  client.Client
	scheme  *runtime.Scheme
	eventCh chan event.GenericEvent
}

// Option is a function which configures the [PersistentVolumeClaimReconciler].
type Option func(r *PersistentVolumeClaimReconciler)

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

//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=core,resources=persistentvolumeclaims/finalizers,verbs=update

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

	logger.Info("reconcile")

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PersistentVolumeClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	src := &source.Channel{
		Source: r.eventCh,
	}
	handler := &handler.EnqueueRequestForObject{}

	return ctrl.NewControllerManagedBy(mgr).
		Named(Name).
		WatchesRawSource(src, handler).
		Complete(r)
}
