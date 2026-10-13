// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package offlineresize

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

// PVCIsInResizeErrorOrFileSystemResizePending returns true for PersistentVolumeClaims that the
// pvc-autoscaler has resized (they carry the previous-size annotation) and which have
// either the ControllerResizeError or FileSystemResizePending condition.
func PVCIsInResizeErrorOrFileSystemResizePending(obj client.Object) bool {
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

// PVCARecommendationsOrPoliciesChangedPredicate is a predicate that returns true for update events if there were changes in the
// PVCA's .status.volumeRecommendations[].name fields or changes in the PVCA's `.spec.volumePolicies[]`.
func PVCARecommendationsOrPoliciesChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPVCA, ok := e.ObjectOld.(*v1alpha1.PersistentVolumeClaimAutoscaler)
			if !ok {
				return false
			}
			newPVCA, ok := e.ObjectNew.(*v1alpha1.PersistentVolumeClaimAutoscaler)
			if !ok {
				return false
			}

			if !slices.Equal(recommendationNames(oldPVCA), recommendationNames(newPVCA)) {
				return true
			}

			return !apiequality.Semantic.DeepEqual(oldPVCA.Spec.VolumePolicies, newPVCA.Spec.VolumePolicies)
		},
		CreateFunc:  func(event.CreateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// recommendationNames returns the ordered list of PVC names referenced by the given PVCA's
// volume recommendations.
func recommendationNames(pvca *v1alpha1.PersistentVolumeClaimAutoscaler) []string {
	names := make([]string, 0, len(pvca.Status.VolumeRecommendations))
	for _, volumeRecommendation := range pvca.Status.VolumeRecommendations {
		names = append(names, volumeRecommendation.Name)
	}

	slices.Sort(names)

	return names
}

// PodWithSchedulingGatePredicate limits the Pod watch to Pods that carry the offline-resize scheduling
// gate.
func PodWithSchedulingGatePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			pod, ok := e.Object.(*corev1.Pod)
			if !ok {
				return false
			}

			return utils.HasSchedulingGate(pod, common.SchedulingGateOfflineResize)
		},
		UpdateFunc:  func(e event.UpdateEvent) bool { return false },
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}
