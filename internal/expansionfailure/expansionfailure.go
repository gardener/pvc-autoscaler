// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package expansionfailure

import (
	"context"
	"fmt"
	"math"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/metrics"
)

// ReasonResizeFailureRecovery indicates that the autoscaler reduced the requested storage on a PVC to recover from an infeasible volume expansion.
const ReasonResizeFailureRecovery = "ResizeFailureRecovery"

// IsResizeInfeasible is a predicate which returns whether the storage resize on
// the given PersistentVolumeClaim has been rejected by the CSI driver as infeasible.
func IsResizeInfeasible(obj *corev1.PersistentVolumeClaim) bool {
	status, ok := obj.Status.AllocatedResourceStatuses[corev1.ResourceStorage]

	return ok && status == corev1.PersistentVolumeClaimControllerResizeInfeasible || status == corev1.PersistentVolumeClaimNodeResizeInfeasible
}

// Recoverer reduces a [corev1.PersistentVolumeClaim]'s requested storage when a
// volume expansion has been reported as infeasible by the CSI driver.
type Recoverer struct {
	client        client.Client
	eventRecorder record.EventRecorder
}

// New creates a new [Recoverer] with the given client and event recorder.
func New(c client.Client, eventRecorder record.EventRecorder) *Recoverer {
	return &Recoverer{
		client:        c,
		eventRecorder: eventRecorder,
	}
}

// Recover reduces the [corev1.PersistentVolumeClaim]'s requested storage when the CSI driver has reported the
// expansion as infeasible. It retries at half the step (allocated + max(ScalingResolutionBytes, failedStep/2)) and
// returns the (possibly updated) volume recommendation or a nil condition when there is nothing to report.
func (r *Recoverer) Recover(ctx context.Context, logger logr.Logger, pvc *corev1.PersistentVolumeClaim, volumeRecommendation v1alpha1.VolumeRecommendation) (v1alpha1.VolumeRecommendation, *metav1.Condition, error) {
	metrics.ResizeFailureRecoveryTotal.WithLabelValues(pvc.Namespace, pvc.Name).Inc()

	currSpecSize := pvc.Spec.Resources.Requests.Storage()
	allocated := pvc.Status.AllocatedResources.Storage()
	if allocated.IsZero() || allocated.Cmp(*currSpecSize) >= 0 {
		return volumeRecommendation, nil, nil
	}

	failedStep := currSpecSize.Value() - allocated.Value()
	recoveryIncrement := int64(math.Max(float64(common.ScalingResolutionBytes), float64(failedStep)/2.0))
	newTargetBytes := int64(math.Ceil(float64(allocated.Value()+recoveryIncrement)/float64(common.ScalingResolutionBytes))) * common.ScalingResolutionBytes
	targetSize := resource.NewQuantity(newTargetBytes, resource.BinarySI)

	// If the half-step meets or exceeds the size that just failed, roll back to the last-known-good capacity instead.
	if targetSize.Cmp(*currSpecSize) >= 0 {
		rollback := allocated.DeepCopy()
		targetSize = &rollback
	}

	infeasibleStatus := pvc.Status.AllocatedResourceStatuses[corev1.ResourceStorage]

	logger.Info("recovering from failed pvc resize", "from", currSpecSize.String(), "to", targetSize.String(), "status", string(infeasibleStatus))
	r.eventRecorder.Eventf(
		pvc,
		corev1.EventTypeWarning,
		"ResizeFailureRecovery",
		"reducing requested storage from %s to %s because volume expansion was reported as infeasible: %s",
		currSpecSize.String(),
		targetSize.String(),
		string(infeasibleStatus),
	)

	pvcPatch := client.MergeFromWithOptions(pvc.DeepCopy(), client.MergeFromWithOptimisticLock{})
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = *targetSize
	if err := r.client.Patch(ctx, pvc, pvcPatch); err != nil {
		if apierrors.IsConflict(err) {
			logger.Info("skipping recovery, pvc changed concurrently", "from", currSpecSize.String(), "to", targetSize.String())

			return volumeRecommendation, nil, nil
		}

		return volumeRecommendation, &metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeResizing),
			Status:  metav1.ConditionFalse,
			Reason:  ReasonResizeFailureRecovery,
			Message: fmt.Sprintf("%s: failed to reduce requested storage from %s to %s after infeasible resize: %s", pvc.Name, currSpecSize.String(), targetSize.String(), err.Error()),
		}, err
	}
	volumeRecommendation.Target.Size = targetSize

	return volumeRecommendation, &metav1.Condition{
		Type:    string(v1alpha1.ConditionTypeResizing),
		Status:  metav1.ConditionFalse,
		Reason:  ReasonResizeFailureRecovery,
		Message: fmt.Sprintf("%s: reduced requested storage from %s to %s after infeasible resize (%s)", pvc.Name, currSpecSize.String(), targetSize.String(), string(infeasibleStatus)),
	}, nil
}
