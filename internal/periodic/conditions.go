// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package periodic

import (
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
)

// recommendationsConditionAggregator is a condition aggregator for the Recommended condition of the PVCA.
type recommendationsConditionAggregator struct {
	conditions []metav1.Condition
}

// addCondition adds a condition to the aggregator. Only conditions with false status can be aggregated.
func (c *recommendationsConditionAggregator) addCondition(condition metav1.Condition) {
	if condition.Status == metav1.ConditionFalse {
		c.conditions = append(c.conditions, condition)
	}
}

// getAggregatedCondition aggregates all conditions into one. If there are no false conditions, it
// returns a true condition indicating that recommendations have been provided
func (c *recommendationsConditionAggregator) getAggregatedCondition() metav1.Condition {
	var (
		status         = metav1.ConditionTrue
		failureReasons = sets.New[string]()
		failures       = make([]string, 0, len(c.conditions))
	)

	for _, condition := range c.conditions {
		if condition.Status == metav1.ConditionFalse {
			status = metav1.ConditionFalse
			failureReasons.Insert(condition.Reason)
			failures = append(failures, condition.Message)
		}
	}

	if status == metav1.ConditionTrue {
		return metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
			Status:  metav1.ConditionTrue,
			Reason:  ReasonRecommendationsProvided,
			Message: "Recommendations have been provided",
		}
	}

	slices.Sort(failures)
	message := "Recommendations could not be provided for some PersistentVolumeClaims:"
	for _, failure := range failures {
		message = message + "\n- " + failure
	}

	reason := ReasonRecommendationsNotProvided
	if failureReasons.Len() == 1 {
		reason, _ = failureReasons.PopAny()
	}

	return metav1.Condition{
		Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
		Reason:  reason,
		Message: message,
		Status:  status,
	}
}

type resizingConditionAggregator struct {
	conditions []metav1.Condition
}

func (c *resizingConditionAggregator) addCondition(condition metav1.Condition) {
	c.conditions = append(c.conditions, condition)
}

func (c *resizingConditionAggregator) getAggregatedCondition() metav1.Condition {
	if len(c.conditions) == 0 {
		return metav1.Condition{Type: string(v1alpha1.ConditionTypeResizing)}
	}

	var (
		message           = "PersistentVolumeClaims cannot be resized:"
		status            = metav1.ConditionFalse
		conditionMessages = make([]string, 0, len(c.conditions))
	)

	for _, condition := range c.conditions {
		conditionMessages = append(conditionMessages, condition.Message)
		if condition.Status == metav1.ConditionTrue {
			message = "PersistentVolumeClaims are being resized:"
			status = metav1.ConditionTrue
		}
	}

	slices.Sort(conditionMessages)
	for _, conditionMessage := range conditionMessages {
		message = message + "\n- " + conditionMessage
	}

	return metav1.Condition{
		Type:    string(v1alpha1.ConditionTypeResizing),
		Message: message,
		Reason:  ReasonReconcile,
		Status:  status,
	}
}
