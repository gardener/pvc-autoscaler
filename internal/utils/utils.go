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

	maxCapacityVal := GetAnnotation(obj, annotation.MaxCapacity, "0Gi")
	maxCapacity, err := resource.ParseQuantity(maxCapacityVal)
	if err != nil {
		return fmt.Errorf("cannot parse max-capacity: %w", err)
	}
	if maxCapacity.IsZero() {
		return fmt.Errorf("invalid max-capacity: %w", common.ErrNoMaxCapacity)
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
