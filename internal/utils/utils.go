// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	v1alpha1 "github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
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

// ValidatePersistentVolumeClaimAutoscaler sanity checks the spec in order to
// ensure it contains valid values. Returns nil if the spec is valid, and
// non-nil error otherwise.
func ValidatePersistentVolumeClaimAutoscaler(obj *v1alpha1.PersistentVolumeClaimAutoscaler) error {
	if obj.Spec.Threshold == "" {
		obj.Spec.Threshold = common.DefaultThresholdValue
	}
	threshold, err := ParsePercentage(obj.Spec.Threshold)
	if err != nil {
		return fmt.Errorf("cannot parse threshold: %w", err)
	}
	if threshold == 0.0 {
		return fmt.Errorf("invalid threshold: %w", common.ErrZeroPercentage)
	}

	if obj.Spec.MaxCapacity.IsZero() {
		return fmt.Errorf("invalid max capacity: %w", common.ErrNoMaxCapacity)
	}

	if obj.Spec.IncreaseBy == "" {
		obj.Spec.IncreaseBy = common.DefaultIncreaseByValue
	}
	increaseBy, err := ParsePercentage(obj.Spec.IncreaseBy)
	if err != nil {
		return fmt.Errorf("cannot parse increase-by value: %w", err)
	}
	if increaseBy == 0.0 {
		return fmt.Errorf("invalid increase-by: %w", common.ErrZeroPercentage)
	}

	return nil
}
