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

// AddAutoscalerNameFieldIndexer adds an index for AutoscalerName to the given indexer.
func AddAutoscalerNameFieldIndexer(ctx context.Context, indexer client.FieldIndexer) error {
	err := indexer.IndexField(ctx, &PersistentVolumeClaimAutoscaler{}, AutoscalerNameIndexKey, func(obj client.Object) []string {
		pvca, ok := obj.(*PersistentVolumeClaimAutoscaler)
		if !ok {
			return nil
		}

		return []string{pvca.Spec.AutoscalerName}
	})
	if err != nil {
		return fmt.Errorf("failed to add indexer for %s to PersistentVolumeClaimAutoscaler Informer: %w", AutoscalerNameIndexKey, err)
	}

	return nil
}

// AddVolumeRecommendationFieldIndexer adds an index that maps a PersistentVolumeClaim to the
// PVCA that owns it (lists it in status.volumeRecommendations).
func AddVolumeRecommendationFieldIndexer(ctx context.Context, indexer client.FieldIndexer) error {
	err := indexer.IndexField(ctx, &PersistentVolumeClaimAutoscaler{}, VolumeRecommendationIndexKey, func(obj client.Object) []string {
		pvca, ok := obj.(*PersistentVolumeClaimAutoscaler)
		if !ok {
			return nil
		}
		values := make([]string, 0, len(pvca.Status.VolumeRecommendations))
		for _, volumeRecommendation := range pvca.Status.VolumeRecommendations {
			values = append(values, volumeRecommendation.Name)
		}

		return values
	})
	if err != nil {
		return fmt.Errorf("failed to add indexer for %s to PersistentVolumeClaimAutoscaler Informer: %w", VolumeRecommendationIndexKey, err)
	}

	return nil
}
