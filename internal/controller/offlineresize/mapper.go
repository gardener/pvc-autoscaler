// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package offlineresize

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
)

// MapPVCAToPVCs maps a PersistentVolumeClaimAutoscaler to reconcile requests for the
// PersistentVolumeClaims referenced by its volume recommendations.
func MapPVCAToPVCs(_ context.Context, obj client.Object) []reconcile.Request {
	pvca, ok := obj.(*v1alpha1.PersistentVolumeClaimAutoscaler)
	if !ok {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(pvca.Status.VolumeRecommendations))
	for _, volumeRecommendation := range pvca.Status.VolumeRecommendations {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: pvca.Namespace, Name: volumeRecommendation.Name},
		})
	}

	return requests
}

// MapPodToPVCs maps a Pod to reconcile requests for the PersistentVolumeClaims it references.
func MapPodToPVCs(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}

	requests := make([]reconcile.Request, 0, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim == nil {
			continue
		}

		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: volume.PersistentVolumeClaim.ClaimName},
		})
	}

	return requests
}
