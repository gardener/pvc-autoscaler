// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Namespace is the namespace component of the fully qualified metric name
const Namespace = "pvc_autoscaler"

var (
	// ResizedTotal is a metric which increments each time a PVC is being
	// resized.
	ResizedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "resized_total",
			Help:      "Total number of times a PVC has been resized",
		},
		[]string{"namespace", "persistentvolumeclaim"},
	)

	// ThresholdReachedTotal is a metric which increments each time the free
	// capacity (space or inodes) for a PVC reaches the threshold.
	ThresholdReachedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "threshold_reached_total",
			Help:      "Total number of times the free capacity for a PVC has reached the threshold",
		},
		[]string{"namespace", "persistentvolumeclaim", "reason"},
	)
)

func init() {
	ctrlmetrics.Registry.MustRegister(ResizedTotal, ThresholdReachedTotal)
}
