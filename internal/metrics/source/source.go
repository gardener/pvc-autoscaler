// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/types"
)

const (
	// KubeletVolumeStatsAvailableBytes is a metric which returns
	// the available bytes in the volumes
	KubeletVolumeStatsAvailableBytes = "kubelet_volume_stats_available_bytes"

	// KubeletVolumeStatsCapacityBytes is a metric which returns the
	// capacity in bytes of the volumes
	KubeletVolumeStatsCapacityBytes = "kubelet_volume_stats_capacity_bytes"

	// KubeletVolumeStatusInodesFree is a metric which returns the number of
	// free inodes in the volumes
	KubeletVolumeStatsInodesFree = "kubelet_volume_stats_inodes_free"

	// KubeletVolumeStatsInodes is a metric which returns the maximum number
	// of inodes in the volumes
	KubeletVolumeStatsInodes = "kubelet_volume_stats_inodes"
)

// VolumeInfo provides stats about a persistent volume claim.
type VolumeInfo struct {
	// AvailableBytes represents the number of available bytes in the volume.
	AvailableBytes int

	// CapacityBytes represents the capacity in bytes of the volume.
	CapacityBytes int

	// AvailableInodes represents the number of free inodes in the volume.
	AvailableInodes int

	// CapacityInodes represents the max supported number of inodes in the volume.
	CapacityInodes int
}

// ErrCapacityIsZero is an error which is returned when the capacity of
// [VolumeInfo] is zero and trying to calculate the percentage of free/used
// space/inodes is not possible. This error is returned in order to avoid
// division by zero runtime errors.
var ErrCapacityIsZero = errors.New("capacity is zero")

// FreeSpacePercentage returns the free space as a percentage.
func (vi *VolumeInfo) FreeSpacePercentage() (float64, error) {
	if vi.CapacityBytes == 0 {
		return 0.0, ErrCapacityIsZero
	}

	val := float64(vi.AvailableBytes) / float64(vi.CapacityBytes) * 100.0
	return val, nil
}

// UsedSpacePercentage returns the used space as a percentage.
func (vi *VolumeInfo) UsedSpacePercentage() (float64, error) {
	free, err := vi.FreeSpacePercentage()
	if err != nil {
		return 0.0, err
	}
	val := 100.0 - free

	return val, nil
}

// FreeInodesPercentage returns the number of free inodes as a percentage.
func (vi *VolumeInfo) FreeInodesPercentage() (float64, error) {
	if vi.CapacityInodes == 0 {
		return 0.0, ErrCapacityIsZero
	}
	val := float64(vi.AvailableInodes) / float64(vi.CapacityInodes) * 100.0

	return val, nil
}

// UsedInodesPercentage returns the number of used inodes as a percentage.
func (vi *VolumeInfo) UsedInodesPercentage() (float64, error) {
	free, err := vi.FreeInodesPercentage()
	if err != nil {
		return 0.0, err
	}
	val := 100.0 - free

	return val, nil
}

// Metrics is a collection of metrics about persistent volume claims grouped by
// [types.NamespacedName].
type Metrics map[types.NamespacedName]*VolumeInfo

// Source represents a source for retrieving metrics about persistent volumes
// claims.
type Source interface {
	// Get retrieves and returns metrics about the persistent volume claims.
	Get(ctx context.Context) (Metrics, error)
}
