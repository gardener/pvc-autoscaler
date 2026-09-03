// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
)

// HasSchedulingGate returns whether the given Pod already has a scheduling gate
// with the given name in its spec.
func HasSchedulingGate(pod *corev1.Pod, name string) bool {
	return slices.ContainsFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
		return gate.Name == name
	})
}

// AddSchedulingGate adds a scheduling gate with the given name to the Pod's spec,
// if it is not already present.
func AddSchedulingGate(pod *corev1.Pod, name string) {
	if HasSchedulingGate(pod, name) {
		return
	}

	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: name})
}

// RemoveSchedulingGate removes the scheduling gate with the given name from the
// Pod's spec if it is present.
func RemoveSchedulingGate(pod *corev1.Pod, name string) {
	pod.Spec.SchedulingGates = slices.DeleteFunc(pod.Spec.SchedulingGates, func(gate corev1.PodSchedulingGate) bool {
		return gate.Name == name
	})
}
