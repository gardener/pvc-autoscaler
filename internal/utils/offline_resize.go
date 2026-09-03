// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils

import "github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"

// IsOfflineResizeEnabled reports whether the given PersistentVolumeClaimAutoscaler
// and the VolumePolicy that applies to a PVC it manages opt into offline-resize
// recovery. This requires that both both the policy's scaleUp.resizeStrategy is PreferInPlace,
// and the PVCA targets a workload controller rather than a PersistentVolumeClaim directly.
func IsOfflineResizeEnabled(pvca *v1alpha1.PersistentVolumeClaimAutoscaler, policy *v1alpha1.VolumePolicy) bool {
	return pvca != nil &&
		pvca.Spec.TargetRef.Kind != "PersistentVolumeClaim" &&
		policy != nil &&
		policy.ScaleUp != nil &&
		policy.ScaleUp.ResizeStrategy == v1alpha1.PreferInPlaceVolumeResizeStrategy
}
