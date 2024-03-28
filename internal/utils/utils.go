package utils

import (
	"errors"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrBadPercentageValue is an error which is returned when attempting to parse
// a bad percentage value.
var ErrBadPercentageValue = errors.New("bad percentage value")

// ParsePercentage parses a string value, which represents percentage, e.g. 10%.
func ParsePercentage(s string) (float64, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimRight(s, "%")
	val, err := strconv.ParseFloat(s, 64)

	if err != nil {
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
