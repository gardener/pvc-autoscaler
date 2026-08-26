// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package offlineresize

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

// ControllerName is the name of the offline-resize recovery controller.
const ControllerName = "offline-resize"

// Event reasons emitted by the controller.
const (
	// ReasonEvicting indicates that the controller is evicting a Pod so its volume
	// can be detached and resized offline.
	ReasonEvicting = "EvictingForOfflineResize"
	// ReasonEvictionBlocked indicates that a Pod eviction was blocked, e.g. by a
	// PodDisruptionBudget, and will be retried.
	ReasonEvictionBlocked = "EvictionBlocked"
	// ReasonGateRemoved indicates that the offline-resize scheduling gate was removed
	// from a Pod so it can be scheduled again.
	ReasonGateRemoved = "OfflineResizeGateRemoved"
)

// ErrNoClient is returned when the [Reconciler] is configured without a client.
var ErrNoClient = errors.New("no client provided")

// Reconciler recovers PersistentVolumeClaims from failed online volume resizes.
type Reconciler struct {
	client         client.Client
	eventRecorder  record.EventRecorder
	autoscalerName string
}

var _ reconcile.Reconciler = (*Reconciler)(nil)

// Option is a function which configures the [Reconciler].
type Option func(*Reconciler)

// New creates a new [Reconciler] with the given options.
func New(opts ...Option) (*Reconciler, error) {
	r := &Reconciler{}
	for _, opt := range opts {
		opt(r)
	}

	if r.client == nil {
		return nil, ErrNoClient
	}

	if r.eventRecorder == nil {
		return nil, common.ErrNoEventRecorder
	}

	return r, nil
}

// WithClient configures the [Reconciler] with the given client.
func WithClient(c client.Client) Option {
	return func(r *Reconciler) {
		r.client = c
	}
}

// WithEventRecorder configures the [Reconciler] with the given event recorder.
func WithEventRecorder(recorder record.EventRecorder) Option {
	return func(r *Reconciler) {
		r.eventRecorder = recorder
	}
}

// WithAutoscalerName configures the [Reconciler] to act only on PersistentVolumeClaims
// managed by a [v1alpha1.PersistentVolumeClaimAutoscaler] whose spec.autoscalerName
// matches the given value. An empty string (the default) matches PVCAs with an empty
// autoscalerName.
func WithAutoscalerName(name string) Option {
	return func(r *Reconciler) {
		r.autoscalerName = name
	}
}

// SetupWithManager wires the [Reconciler] into the given manager. It watches
// PersistentVolumeClaims and filters, via a predicate, to those which are managed by
// the pvc-autoscaler (carry the previous-size annotation) and have one of the
// conditions relevant to offline-resize recovery.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(ControllerName).
		For(&corev1.PersistentVolumeClaim{}, builder.WithPredicates(managedPVCPredicate())).
		Complete(r)
}

// managedPVCPredicate limits reconciliation to PersistentVolumeClaims that the
// pvc-autoscaler has resized (they carry the previous-size annotation) and which
// have either the ControllerResizeError or FileSystemResizePending condition.
func managedPVCPredicate() predicate.Predicate {
	relevant := func(obj client.Object) bool {
		pvc, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			return false
		}

		if _, ok := pvc.Annotations[common.AnnotationPreviousSize]; !ok {
			return false
		}

		return utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimControllerResizeError) ||
			utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimFileSystemResizePending)
	}

	return predicate.NewPredicateFuncs(relevant)
}

// Reconcile drives offline-resize recovery for a single PersistentVolumeClaim.
func (r *Reconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx, "controller", ControllerName, "pvc", req.NamespacedName)

	pvc := &corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, req.NamespacedName, pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}

		return reconcile.Result{}, fmt.Errorf("failed to get PersistentVolumeClaim: %w", err)
	}

	// Gate removal is intentionally unconditional: it is not gated on ownership or
	// the PreferInPlace strategy. A gate could only ever have been added while the
	// PVC was owned and the strategy was PreferInPlace, so if the PVC is no longer
	// owned (e.g. the PVCA was deleted) or the strategy was flipped away since then,
	// we must still remove the gate so the Pod is not stranded unschedulable forever.
	if utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimFileSystemResizePending) {
		return reconcile.Result{}, r.removeSchedulingGates(ctx, logger, pvc)
	}

	// Eviction is opt-in: only act on PVCs managed by a PVCA that belongs to this
	// autoscaler instance and whose effective policy enables offline-resize recovery.
	owner, policy, err := utils.FindOwningPVCAAndPolicy(ctx, r.client, r.autoscalerName, pvc)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to determine ownership of PersistentVolumeClaim: %w", err)
	}
	if owner == nil || policy == nil {
		logger.Info("persistentvolumeclaim is not managed by this autoscaler instance, skipping")

		return reconcile.Result{}, nil
	}

	if !utils.IsOfflineResizeEnabled(owner, policy) {
		logger.Info("offline-resize recovery is not enabled for this persistentvolumeclaim, skipping")

		return reconcile.Result{}, nil
	}

	if utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimControllerResizeError) {
		return reconcile.Result{}, r.evictPodsUsingPVC(ctx, logger, pvc)
	}

	return reconcile.Result{}, nil
}

// evictPodsUsingPVC evicts all Pods that use the given PVC so that the volume can
// be detached and resized offline. Eviction is used so PodDisruptionBudgets are
// respected. Pods that are already terminating, not running, or not owned by a
// workload controller are skipped.
func (r *Reconciler) evictPodsUsingPVC(ctx context.Context, logger logr.Logger, pvc *corev1.PersistentVolumeClaim) error {
	pods, err := r.podsUsingPVC(ctx, pvc)
	if err != nil {
		return err
	}

	var blocked bool
	for i := range pods {
		pod := &pods[i]

		if pod.DeletionTimestamp != nil {
			continue
		}

		if pod.Status.Phase != corev1.PodRunning {
			logger.Info("pod is not running, skipping eviction", "pod", client.ObjectKeyFromObject(pod), "phase", pod.Status.Phase)

			continue
		}

		logger.Info("evicting pod for offline resize", "pod", client.ObjectKeyFromObject(pod))
		if err := r.evict(ctx, pod); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}

			// A blocked eviction (e.g. PodDisruptionBudget) is not a hard error -
			// record it and requeue so it is retried later. The k8s API returns 429 (too many requests) when
			// eviction would violate the PodDisruptionBudget.
			if apierrors.IsTooManyRequests(err) {
				blocked = true
				r.eventRecorder.Eventf(pod, corev1.EventTypeWarning, ReasonEvictionBlocked,
					"eviction of pod %s for offline resize of PersistentVolumeClaim %s was blocked, will retry: %s",
					client.ObjectKeyFromObject(pod), pvc.Name, err.Error())
				logger.Info("eviction blocked, will retry", "pod", client.ObjectKeyFromObject(pod), "reason", err.Error())

				continue
			}

			return fmt.Errorf("failed to evict pod %s: %w", client.ObjectKeyFromObject(pod), err)
		}

		r.eventRecorder.Eventf(pod, corev1.EventTypeNormal, ReasonEvicting,
			"evicted pod %s so PersistentVolumeClaim %s can be resized offline",
			client.ObjectKeyFromObject(pod), pvc.Name)
	}

	if blocked {
		// Requeue via a returned error so at least one eviction is retried.
		return fmt.Errorf("one or more pod evictions for offline resize of PersistentVolumeClaim %s were blocked", pvc.Name)
	}

	return nil
}

// evict evicts the given Pod via the pods/eviction subresource so that
// PodDisruptionBudgets are respected. The API server returns a TooManyRequests
// error when the eviction is blocked by a PodDisruptionBudget, which the caller
// treats as retryable rather than fatal.
func (r *Reconciler) evict(ctx context.Context, pod *corev1.Pod) error {
	return r.client.SubResource("eviction").Create(ctx, pod, &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	})
}

// removeSchedulingGates removes the offline-resize scheduling gate from all Pods
// that use the given PVC, allowing them to be scheduled so that, once they mount
// the volume, the kubelet can complete the filesystem resize.
func (r *Reconciler) removeSchedulingGates(ctx context.Context, logger logr.Logger, pvc *corev1.PersistentVolumeClaim) error {
	pods, err := r.podsUsingPVC(ctx, pvc)
	if err != nil {
		return err
	}

	for i := range pods {
		pod := &pods[i]

		if !utils.HasSchedulingGate(pod, common.SchedulingGateOfflineResize) {
			continue
		}

		patch := client.MergeFrom(pod.DeepCopy())
		utils.RemoveSchedulingGate(pod, common.SchedulingGateOfflineResize)
		if err := r.client.Patch(ctx, pod, patch); err != nil {
			return fmt.Errorf("failed to remove scheduling gate from pod %s: %w", client.ObjectKeyFromObject(pod), err)
		}

		logger.Info("removed offline-resize scheduling gate from pod", "pod", client.ObjectKeyFromObject(pod))
		r.eventRecorder.Eventf(pod, corev1.EventTypeNormal, ReasonGateRemoved,
			"removed offline-resize scheduling gate from pod %s, PersistentVolumeClaim %s is ready for filesystem resize",
			client.ObjectKeyFromObject(pod), pvc.Name)
	}

	return nil
}

// podsUsingPVC returns all Pods in the PVC's namespace that reference it via a
// PersistentVolumeClaim volume.
func (r *Reconciler) podsUsingPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := r.client.List(ctx, podList, client.InNamespace(pvc.Namespace)); err != nil {
		return nil, fmt.Errorf("failed to list Pods: %w", err)
	}

	pods := make([]corev1.Pod, 0)
	for i := range podList.Items {
		for _, volume := range podList.Items[i].Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == pvc.Name {
				pods = append(pods, podList.Items[i])

				break
			}
		}
	}

	return pods, nil
}
