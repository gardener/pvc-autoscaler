// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package expansionfailure

import (
	"context"
	"fmt"
	"math"
	"strings"

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
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

// ReasonResizeFailureRecovery indicates that the autoscaler reduced the requested storage on a PVC to recover from an infeasible volume expansion.
const ReasonResizeFailureRecovery = "ResizeFailureRecovery"

// resizeErrorConditionTypes are the PVC condition types a CSI driver uses to report a
// terminal resize error together with a provider-specific message.
var resizeErrorConditionTypes = []corev1.PersistentVolumeClaimConditionType{
	corev1.PersistentVolumeClaimControllerResizeError,
	corev1.PersistentVolumeClaimNodeResizeError,
}

// resizeLimitErrorFragments are lowercased, variable-free substrings of the
// ControllerResizeError/NodeResizeError messages that CSI drivers emit when a requested
// volume size exceeds a provider limit (volume-type maximum or quota) rather than reporting
// the resize as infeasible.
// TODO (RadaBDimitrova): externalize these fragments rather than carrying provider-specific knowledge in-tree.
var resizeLimitErrorFragments = []string{
	"is out of range. the value must be between", // Azure (disk.csi.azure.com), cloud code InvalidParameter
	"is too large for volume type",               // AWS (ebs.csi.aws.com), cloud code InvalidParameterValue
	"must be less than or equal to",              // GCP (pd.csi.storage.gke.io), cloud code InvalidArgument / googleapi: Error 400
	"volumesizeexceedslimit",                     // OpenStack (cinder.csi.openstack.org), HTTP 413
	"is larger than maximum allowed limit",       // OpenStack (cinder.csi.openstack.org), HTTP 413
}

// IsResizeInfeasible is a predicate which returns whether the storage resize on
// the given PersistentVolumeClaim has been rejected by the CSI driver as infeasible.
func IsResizeInfeasible(obj *corev1.PersistentVolumeClaim) bool {
	status, ok := obj.Status.AllocatedResourceStatuses[corev1.ResourceStorage]

	return ok && status == corev1.PersistentVolumeClaimControllerResizeInfeasible || status == corev1.PersistentVolumeClaimNodeResizeInfeasible
}

// HasResizeLimitError returns whether the given PersistentVolumeClaim has a
// ControllerResizeError or NodeResizeError condition whose message matches a known
// provider-specific resize-limit error (see [resizeLimitErrorFragments]). The condition
// must currently be true; a stale, resolved condition does not match.
func HasResizeLimitError(obj *corev1.PersistentVolumeClaim) bool {
	for _, conditionType := range resizeErrorConditionTypes {
		condition := utils.GetPersistentVolumeClaimCondition(obj, conditionType)
		if condition.Status != corev1.ConditionTrue {
			continue
		}

		message := strings.ToLower(condition.Message)
		for _, fragment := range resizeLimitErrorFragments {
			if strings.Contains(message, fragment) {
				return true
			}
		}
	}

	return false
}

// ShouldRecover returns whether a volume expansion on the given PersistentVolumeClaim has
// failed in a way the autoscaler can recover from by reducing the requested storage. This
// is the case when the CSI driver reported the controller/node resize as infeasible, or
// when the ControllerResizeError/NodeResizeError message matches a known provider-specific
// resize-limit error (volume-type maximum or quota).
func ShouldRecover(obj *corev1.PersistentVolumeClaim) bool {
	return IsResizeInfeasible(obj) || HasResizeLimitError(obj)
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
// expansion as infeasible. It retries at a half-step above the current capacity
// (capacity + max(ScalingResolutionBytes, failedStep/2)).
func (r *Recoverer) Recover(ctx context.Context, logger logr.Logger, pvc *corev1.PersistentVolumeClaim, volumeRecommendation v1alpha1.VolumeRecommendation) (v1alpha1.VolumeRecommendation, *metav1.Condition, error) {
	metrics.ResizeFailureRecoveryTotal.WithLabelValues(pvc.Namespace, pvc.Name).Inc()

	currSpecSize := pvc.Spec.Resources.Requests.Storage()
	statusCapacitySize := pvc.Status.Capacity.Storage()
	if statusCapacitySize.IsZero() {
		return volumeRecommendation, nil, nil
	}

	failedStep := currSpecSize.Value() - statusCapacitySize.Value()
	recoveryIncrement := int64(math.Max(float64(common.ScalingResolutionBytes), float64(failedStep)/2.0))
	newTargetBytes := int64(math.Ceil(float64(statusCapacitySize.Value()+recoveryIncrement)/float64(common.ScalingResolutionBytes))) * common.ScalingResolutionBytes
	targetSize := resource.NewQuantity(newTargetBytes, resource.BinarySI)

	if targetSize.Cmp(*currSpecSize) >= 0 {
		logger.Info("skipping recovery, target size is not smaller than current request", "from", currSpecSize.String(), "to", targetSize.String())

		return volumeRecommendation, nil, nil
	}

	resizeStatus := pvc.Status.AllocatedResourceStatuses[corev1.ResourceStorage]

	logger.Info("recovering from failed pvc resize", "from", currSpecSize.String(), "to", targetSize.String(), "status", string(resizeStatus))
	r.eventRecorder.Eventf(
		pvc,
		corev1.EventTypeWarning,
		"ResizeFailureRecovery",
		"reducing requested storage from %s to %s because volume expansion failed: %s",
		currSpecSize.String(),
		targetSize.String(),
		string(resizeStatus),
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
			Message: fmt.Sprintf("%s: failed to reduce requested storage from %s to %s after failed resize: %s", pvc.Name, currSpecSize.String(), targetSize.String(), err.Error()),
		}, err
	}
	volumeRecommendation.Target.Size = targetSize

	return volumeRecommendation, &metav1.Condition{
		Type:    string(v1alpha1.ConditionTypeResizing),
		Status:  metav1.ConditionTrue,
		Reason:  ReasonResizeFailureRecovery,
		Message: fmt.Sprintf("%s: reduced requested storage from %s to %s after failed resize (%s)", pvc.Name, currSpecSize.String(), targetSize.String(), string(resizeStatus)),
	}, nil
}
