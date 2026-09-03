// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"path"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
)

// GetVolumePolicy returns the VolumePolicy for the given PersistentVolumeClaim
// name. Policies are evaluated in list order and the first policy whose match
// glob matches the name is returned. It returns nil if no policy matches.
func GetVolumePolicy(pvcName string, volumePolicies []v1alpha1.VolumePolicy) (*v1alpha1.VolumePolicy, error) {
	for i := range volumePolicies {
		matched, err := path.Match(volumePolicies[i].Match.Name, pvcName)
		if err != nil {
			return nil, fmt.Errorf("invalid volume policy name %q: %w", volumePolicies[i].Match.Name, err)
		}
		if matched {
			return &volumePolicies[i], nil
		}
	}

	return nil, nil
}
