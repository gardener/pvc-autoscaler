// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// AutoscalerNameIndexKey is the field index key used to filter PVCAs by their autoscalerName.
const AutoscalerNameIndexKey = ".spec.autoscalerName"

// AddAutoscalerNameFieldIndexer adds an index for AutoscalerName to the given indexer.
func AddAutoscalerNameFieldIndexer(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &PersistentVolumeClaimAutoscaler{}, AutoscalerNameIndexKey, func(obj client.Object) []string {
		pvca, ok := obj.(*PersistentVolumeClaimAutoscaler)
		if !ok {
			return nil
		}

		return []string{pvca.Spec.AutoscalerName}
	}); err != nil {
		return fmt.Errorf("failed to add indexer for %s to PersistentVolumeClaimAutoscaler Informer: %w", AutoscalerNameIndexKey, err)
	}

	return nil
}
