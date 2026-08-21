// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// ErrBadPercentageValue is an error which is returned when attempting to parse
// a bad percentage value.
var ErrBadPercentageValue = errors.New("bad percentage value")

// ParsePercentage parses a string value, which represents percentage, e.g. 10%.
func ParsePercentage(s string) (float64, error) {
	s = strings.TrimSpace(s)

	if !strings.HasSuffix(s, "%") {
		return 0.0, ErrBadPercentageValue
	}
	s = strings.TrimRight(s, "%")
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return val, ErrBadPercentageValue
	}

	if val < 0.0 || val > 100.0 {
		return val, ErrBadPercentageValue
	}

	return val, nil
}

// IsPersistentVolumeClaimConditionTrue is a predicate which tests whether the
// given PersistentVolumeClaim object's status condition is set to [corev1.ConditionTrue].
func IsPersistentVolumeClaimConditionTrue(obj *corev1.PersistentVolumeClaim, conditionType corev1.PersistentVolumeClaimConditionType) bool {
	return IsPersistentVolumeClaimConditionPresentAndEqual(obj, conditionType, corev1.ConditionTrue)
}

// IsPersistentVolumeClaimConditionPresentAndEqual is a predicate which returns
// whether the condition of the given type is equal to the given status.
func IsPersistentVolumeClaimConditionPresentAndEqual(obj *corev1.PersistentVolumeClaim, conditionType corev1.PersistentVolumeClaimConditionType, status corev1.ConditionStatus) bool {
	for _, condition := range obj.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}

	return false
}

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
