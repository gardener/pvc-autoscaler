// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
)

// offlineResizeRequiredErrorSubstrings holds lowercased fragments of ControllerResizeError
// messages that indicate a volume can only be resized offline (the volume must be detached
// from the node first). The fragments are variable-free parts of the full provider error so
// that embedded values (VM size, disk sizes) do not break matching.
// These are cloud-provider-specific.
//
// TODO(plkokanov): consider moving these checks out-of-tree or configuring them via options.
// The current strings are taken from errors returned by Azure's `disk.csi.azure.com` provisioner.`
var offlineResizeRequiredErrorSubstrings = []string{
	"change in disk property of vm of size",
	"while it is attached to a running vm",
	"please stop your vm or detach the disk",
}

// IsOfflineResizeRequired reports whether the given PersistentVolumeClaim's
// ControllerResizeError condition message indicates that the volume requires an offline resize.
// It matches case-insensitively against any of the known provider error fragments.
func IsOfflineResizeRequired(pvc *corev1.PersistentVolumeClaim) bool {
	condition := GetPersistentVolumeClaimCondition(pvc, corev1.PersistentVolumeClaimControllerResizeError)
	lowered := strings.ToLower(condition.Message)
	for _, substring := range offlineResizeRequiredErrorSubstrings {
		if strings.Contains(lowered, substring) {
			return true
		}
	}

	return false
}

// IsOfflineResizeEnabled reports whether the given PersistentVolumeClaimAutoscaler
// and the VolumePolicy that applies to a PVC it manages opt into offline-resize
// recovery. This requires that both the policy's scaleUp.resizeStrategy is PreferInPlace,
// and the PVCA targets a workload controller rather than a PersistentVolumeClaim directly.
func IsOfflineResizeEnabled(pvca *v1alpha1.PersistentVolumeClaimAutoscaler, policy *v1alpha1.VolumePolicy) bool {
	return pvca != nil &&
		pvca.Spec.TargetRef.Kind != "PersistentVolumeClaim" &&
		policy != nil &&
		policy.ScaleUp != nil &&
		policy.ScaleUp.ResizeStrategy == v1alpha1.PreferInPlaceVolumeResizeStrategy
}
