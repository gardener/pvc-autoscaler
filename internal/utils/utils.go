// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// GetAnnotation returns the annotation with the given name from the object, if
// it exists, otherwise it returns a default value.
func GetAnnotation(obj client.Object, name, defaultVal string) string {
	val, ok := obj.GetAnnotations()[name]
	if !ok {
		return defaultVal
	}

	return val
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

// ParseMinThreshold returns the value of the absolute scaling trigger threshold, specified in the PVC.
// If minimum threshold is not specified, or an error occurs, a nil value is returned.
func ParseMinThreshold(pvc *corev1.PersistentVolumeClaim) (asQuantity *resource.Quantity, err error) {
	minThresholdVal := GetAnnotation(pvc, annotation.MinThreshold, "")
	if minThresholdVal == "" {
		return nil, nil
	}

	q, err := resource.ParseQuantity(minThresholdVal)
	if err != nil {
		return nil, fmt.Errorf("cannot parse minimum threshold: %w", err)
	}

	if q.Sign() < 0 {
		return nil, fmt.Errorf("invalid minimum threshold `%s`: negative values are not accepted", minThresholdVal)
	}

	return &q, nil
}

// ValidatePersistentVolumeClaimAnnotations sanity checks the custom annotations
// in order to ensure they contain valid values. Returns nil if all
// user-specified annotations are valid, otherwise it returns a non-nil error.
func ValidatePersistentVolumeClaimAnnotations(obj *corev1.PersistentVolumeClaim) error {
	thresholdVal := GetAnnotation(obj, annotation.Threshold, common.DefaultThresholdValue)
	threshold, err := ParsePercentage(thresholdVal)
	if err != nil {
		return fmt.Errorf("cannot parse threshold: %w", err)
	}
	if threshold == 0.0 {
		return fmt.Errorf("invalid threshold: %w", common.ErrZeroPercentage)
	}

	if _, err := ParseMinThreshold(obj); err != nil {
		return err
	}

	maxCapacityVal := GetAnnotation(obj, annotation.MaxCapacity, "0Gi")
	maxCapacity, err := resource.ParseQuantity(maxCapacityVal)
	if err != nil {
		return fmt.Errorf("cannot parse max capacity: %w", err)
	}
	if maxCapacity.IsZero() {
		return fmt.Errorf("invalid max capacity: %w", common.ErrNoMaxCapacity)
	}

	currStatusSize := obj.Status.Capacity.Storage()
	if currStatusSize.IsZero() {
		return fmt.Errorf(".status.capacity.storage is invalid: %s", currStatusSize.String())
	}

	if maxCapacity.Value() < currStatusSize.Value() {
		return fmt.Errorf("max capacity (%s) cannot be less than current size (%s)", maxCapacity.String(), currStatusSize.String())
	}

	increaseByVal := GetAnnotation(obj, annotation.IncreaseBy, common.DefaultIncreaseByValue)
	increaseBy, err := ParsePercentage(increaseByVal)
	if err != nil {
		return fmt.Errorf("cannot parse increase-by value: %w", err)
	}
	if increaseBy == 0.0 {
		return fmt.Errorf("invalid increase-by: %w", common.ErrZeroPercentage)
	}

	return nil
}
