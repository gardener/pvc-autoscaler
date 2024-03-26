package source

import (
	"context"

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

// Metrics is a collection of metrics about persistent volume claims grouped by
// [types.NamespacedName].
type Metrics map[types.NamespacedName]*VolumeInfo

// Source represents a source for retrieving metrics about persistent volumes
// claims.
type Source interface {
	// Get retrieves and returns metrics about the persistent volume claims.
	Get(ctx context.Context) (Metrics, error)
}
