// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"github.com/gardener/pvc-autoscaler/internal/annotation"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Key is the index key we use to lookup PersistentVolumeClaim objects.
const Key = "pvc.autoscaling.gardener.cloud/idx"

// IndexerFunc is a [sigs.k8s.io/controller-runtime/pkg/client.IndexerFunc],
// which knows how to extract values for [Key] index.
func IndexerFunc(rawObj client.Object) []string {
	obj, ok := rawObj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return []string{}
	}

	value, ok := obj.Annotations[annotation.IsEnabled]
	if !ok {
		return []string{}
	}

	return []string{value}
}
