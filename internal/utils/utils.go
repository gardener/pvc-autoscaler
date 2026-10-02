// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
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

// FindOwningPVCAAndPolicy returns the PersistentVolumeClaimAutoscaler that manages the
// given PersistentVolumeClaim and whose spec.autoscalerName matches the given
// autoscalerName, together with the VolumePolicy that applies to the PVC, or
// (nil, nil) if the PVC is not managed by any such PVCA.
func FindOwningPVCAAndPolicy(ctx context.Context, c client.Client, autoscalerName string, pvc *corev1.PersistentVolumeClaim) (*v1alpha1.PersistentVolumeClaimAutoscaler, *v1alpha1.VolumePolicy, error) {
	pvcaList := &v1alpha1.PersistentVolumeClaimAutoscalerList{}
	if err := c.List(ctx, pvcaList, client.InNamespace(pvc.Namespace), client.MatchingFields{v1alpha1.AutoscalerNameIndexKey: autoscalerName}); err != nil {
		return nil, nil, fmt.Errorf("failed to list PersistentVolumeClaimAutoscalers: %w", err)
	}

	for _, pvca := range pvcaList.Items {
		if slices.ContainsFunc(pvca.Status.VolumeRecommendations, func(vr v1alpha1.VolumeRecommendation) bool {
			return vr.Name == pvc.Name
		}) {
			policy, err := GetVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
			if err != nil {
				return nil, nil, err
			}

			return &pvca, policy, nil
		}
	}

	return nil, nil, nil
}
