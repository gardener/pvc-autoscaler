// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// AutoscalerNameIndexKey is the field index key used to filter PVCAs by their autoscalerName.
	AutoscalerNameIndexKey = ".spec.autoscalerName"

	// VolumeRecommendationIndexKey is the field index key used to look up the PVCA that owns a
	// PersistentVolumeClaim, keyed by the recommended PVC name.
	VolumeRecommendationIndexKey = ".status.volumeRecommendations.name"
)

// AutoscalerNameIndexFunc extracts the spec.autoscalerName of a PVCA for the AutoscalerNameIndexKey index.
func AutoscalerNameIndexFunc(obj client.Object) []string {
	pvca, ok := obj.(*PersistentVolumeClaimAutoscaler)
	if !ok {
		return nil
	}

	return []string{pvca.Spec.AutoscalerName}
}

// VolumeRecommendationIndexFunc extracts the names of a PVCA's status.volumeRecommendations for the
// VolumeRecommendationIndexKey index.
func VolumeRecommendationIndexFunc(obj client.Object) []string {
	pvca, ok := obj.(*PersistentVolumeClaimAutoscaler)
	if !ok {
		return nil
	}
	volumeRecommendationNames := make([]string, 0, len(pvca.Status.VolumeRecommendations))
	for _, volumeRecommendation := range pvca.Status.VolumeRecommendations {
		volumeRecommendationNames = append(volumeRecommendationNames, volumeRecommendation.Name)
	}

	return volumeRecommendationNames
}

// AddAutoscalerNameFieldIndexer adds an index for AutoscalerName to the given indexer.
func AddAutoscalerNameFieldIndexer(ctx context.Context, indexer client.FieldIndexer) error {
	err := indexer.IndexField(ctx, &PersistentVolumeClaimAutoscaler{}, AutoscalerNameIndexKey, AutoscalerNameIndexFunc)
	if err != nil {
		return fmt.Errorf("failed to add indexer for %s to PersistentVolumeClaimAutoscaler Informer: %w", AutoscalerNameIndexKey, err)
	}

	return nil
}

// AddVolumeRecommendationFieldIndexer adds an index that maps a PersistentVolumeClaim to the
// PVCA that owns it (lists it in status.volumeRecommendations).
func AddVolumeRecommendationFieldIndexer(ctx context.Context, indexer client.FieldIndexer) error {
	err := indexer.IndexField(ctx, &PersistentVolumeClaimAutoscaler{}, VolumeRecommendationIndexKey, VolumeRecommendationIndexFunc)
	if err != nil {
		return fmt.Errorf("failed to add indexer for %s to PersistentVolumeClaimAutoscaler Informer: %w", VolumeRecommendationIndexKey, err)
	}

	return nil
}
