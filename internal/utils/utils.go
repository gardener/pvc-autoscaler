// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"errors"
	"strconv"
	"strings"

	"github.com/Masterminds/semver/v3"
	corev1 "k8s.io/api/core/v1"
)

// ErrBadPercentageValue is an error which is returned when attempting to parse
// a bad percentage value.
var ErrBadPercentageValue = errors.New("bad percentage value")

// constraintK8sGreaterEqual134 matches Kubernetes versions 1.34.0 and higher.
var constraintK8sGreaterEqual134 = mustNewConstraint(">= 1.34-0")

func mustNewConstraint(constraint string) *semver.Constraints {
	c, err := semver.NewConstraint(constraint)
	if err != nil {
		panic(err)
	}

	return c
}

// normalizeVersion returns the normalized version string by removing the
// leading 'v' and any suffixes like '-rc1', '+build', etc.
func normalizeVersion(version string) string {
	v := strings.ReplaceAll(version, "v", "")
	if idx := strings.IndexAny(v, "-+"); idx != -1 {
		v = v[:idx]
	}

	return v
}

// IsKubernetesVersionGreaterEqual134 reports whether the given Kubernetes
// version string is 1.34.0 or newer.
func IsKubernetesVersionGreaterEqual134(version string) bool {
	v, err := semver.NewVersion(normalizeVersion(version))
	if err != nil {
		return false
	}

	return constraintK8sGreaterEqual134.Check(v)
}

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

// IsPersistentVolumeClaimResizeInfeasible is a predicate which returns whether the storage resize on
// the given PersistentVolumeClaim has been rejected by the CSI driver as infeasible.
func IsPersistentVolumeClaimResizeInfeasible(obj *corev1.PersistentVolumeClaim) bool {
	status, ok := obj.Status.AllocatedResourceStatuses[corev1.ResourceStorage]
	if !ok {
		return false
	}

	return status == corev1.PersistentVolumeClaimControllerResizeInfeasible || status == corev1.PersistentVolumeClaimNodeResizeInfeasible
}
