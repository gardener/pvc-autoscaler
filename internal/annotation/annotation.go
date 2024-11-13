// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package annotation

const (
	// Prefix is the prefix used by all annotations
	Prefix = "pvc.autoscaling.gardener.cloud/"

	// IsEnabled is the annotation used to specify that autoscaling is
	// enabled for the PVC
	IsEnabled = Prefix + "is-enabled"

	// IncreaseBy is an annotation, which specifies an increase by
	// percentage value (e.g. 10%, 20%, etc.) by which the Persistent Volume
	// Claim storage will be resized.
	IncreaseBy = Prefix + "increase-by"

	// Threshold is an annotation which specifies the threshold value in
	// percentage (e.g. 10%, 20%, etc.) for the PVC. Once the available
	// capacity (free space) for the PVC reaches or drops below the
	// specified threshold this will trigger a resize operation by the
	// controller.
	//
	// If Threshold and MinThreshold are both specified, a resize is
	// initiated when any of the thresholds is reached.
	Threshold = Prefix + "threshold"

	// MinThreshold is an annotation which is applied to a PVC, and
	// specifies the scaling trigger threshold for the PVC's free space in
	// absolute terms (e.g. 1Gi, 600Mi, etc.).
	// Once the available capacity (free space) for the PVC drops below the
	// level specified by the annotation, the controller will respond by
	// resizing the PVC.
	//
	// If both Threshold and MinThreshold are specified, a resize is
	// initiated if any of the thresholds is reached.
	//
	// When scaling is initiated based on MinThreshold, the increment
	// (see IncreaseBy) is calculated by a different formula:
	//
	// Increment = (IncreaseBy / Threshold) * MinThreshold
	//
	// With this formula, a scaling event based on MinThreshold actually
	// results in the same increment, as would be suggested by the pair of
	// relative Threshold and IncreaseBy parameters, at the capacity point
	// where the absolute value derived from Threshold would equal
	// MinThreshold.
	//
	// Note: In contrast to Threshold, MinThreshold only applies to free
	// space, not to inodes.
	MinThreshold = Prefix + "min-threshold"

	// MaxCapacity is an annotation which specifies the maximum capacity up
	// to which a PVC is allowed to be extended. The max capacity is
	// specified as a [k8s.io/apimachinery/pkg/api/resource.Quantity] value.
	MaxCapacity = Prefix + "max-capacity"

	// LastCheck is the annotation which specifies the time since Unix epoch
	// of the last periodic check.
	LastCheck = Prefix + "last-check"

	// NextCheck is the annotation which specifies the time since Unix epoch
	// at which the next check is scheduled for.
	NextCheck = Prefix + "next-check"

	// UsedSpacePercentage is the annotation which specifies the last
	// observed used space of the PVC as a percentage.
	UsedSpacePercentage = Prefix + "used-space"

	// FreeSpacePercentage is the annotation which specifies the last
	// observed free space of the PVC as a percentage.
	FreeSpacePercentage = Prefix + "free-space"

	// UsedInodesPercentage is the annotation which specifies the last
	// observed used inodes of the PVC as a percentage.
	UsedInodesPercentage = Prefix + "used-inodes"

	// FreeInodesPercentage is the annotation which specifies the last
	// observed free inodes of the PVC as a percentage.
	FreeInodesPercentage = Prefix + "free-inodes"

	// PrevSize is the annotation which is used to record the previous
	// .status.capacity.storage value before the PVC is being resized.
	PrevSize = Prefix + "prev-size"
)
