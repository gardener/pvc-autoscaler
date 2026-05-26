// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package periodic

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
)

var _ = Describe("recommendationsConditionAggregator", func() {
	var aggregator *recommendationsConditionAggregator

	BeforeEach(func() {
		aggregator = &recommendationsConditionAggregator{}
	})

	It("should return a true condition when no condition aggregated", func() {
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
			Status:  metav1.ConditionTrue,
			Reason:  ReasonRecommendationsProvided,
			Message: "Recommendations have been provided",
		}

		Expect(aggregator.getAggregatedCondition()).To(Equal(condition))
	})

	It("should aggregate to False and only list failed conditions when any condition is False", func() {
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionTrue,
			Reason:  ReasonMetricsFetched,
			Message: "pvc-a: ok",
		})
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionFalse,
			Reason:  ReasonMetricsFetchError,
			Message: "pvc-b: stale metrics",
		})

		got := aggregator.getAggregatedCondition()
		Expect(got.Status).To(Equal(metav1.ConditionFalse))
		Expect(got.Reason).To(Equal(ReasonMetricsFetchError))
		Expect(got.Message).To(Equal("Recommendations could not be provided for some PersistentVolumeClaims:\n- pvc-b: stale metrics"))
	})

	It("should use RecommendationsNotProvided when failed conditions disagree on reason", func() {
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionFalse,
			Reason:  ReasonMetricsFetchError,
			Message: "Recommendations could not be provided for some PersistentVolumeClaims:\n- pvc-a: stale metrics",
		})
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionFalse,
			Reason:  ReasonRecommendationError,
			Message: "Recommendations could not be provided for some PersistentVolumeClaims:\n- pvc-b: invalid",
		})

		got := aggregator.getAggregatedCondition()
		Expect(got.Status).To(Equal(metav1.ConditionFalse))
		Expect(got.Reason).To(Equal(ReasonRecommendationsNotProvided))
	})

	It("should sort failure messages deterministically regardless of insertion order", func() {
		aggregator.addCondition(metav1.Condition{
			Status: metav1.ConditionFalse, Reason: ReasonMetricsFetchError, Message: "pvc-c: failed",
		})
		aggregator.addCondition(metav1.Condition{
			Status: metav1.ConditionFalse, Reason: ReasonMetricsFetchError, Message: "pvc-a: failed",
		})
		aggregator.addCondition(metav1.Condition{
			Status: metav1.ConditionFalse, Reason: ReasonMetricsFetchError, Message: "pvc-b: failed",
		})

		got := aggregator.getAggregatedCondition()
		Expect(got.Message).To(Equal("Recommendations could not be provided for some PersistentVolumeClaims:\n- pvc-a: failed\n- pvc-b: failed\n- pvc-c: failed"))
	})
})

var _ = Describe("resizingConditionAggregator", func() {
	var aggregator *resizingConditionAggregator

	BeforeEach(func() {
		aggregator = &resizingConditionAggregator{}
	})

	It("should return a Condition with only Type set when no conditions were added", func() {
		Expect(aggregator.getAggregatedCondition()).To(Equal(metav1.Condition{
			Type: string(v1alpha1.ConditionTypeResizing),
		}))
	})

	It("should return the only condition one was added", func() {
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeResizing),
			Status:  metav1.ConditionTrue,
			Reason:  ReasonReconcile,
			Message: "pvc-a: resizing from 1Gi to 2Gi",
		}
		aggregator.addCondition(condition)

		expectedCondition := condition
		expectedCondition.Message = "PersistentVolumeClaims are being resized:\n- pvc-a: resizing from 1Gi to 2Gi"
		Expect(aggregator.getAggregatedCondition()).To(Equal(expectedCondition))
	})

	It("should aggregate to True when any condition is True", func() {
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionFalse,
			Reason:  ReasonReconcile,
			Message: "pvc-a: max capacity reached",
		})
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionTrue,
			Reason:  ReasonReconcile,
			Message: "pvc-b: resizing from 1Gi to 2Gi",
		})
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionUnknown,
			Reason:  ReasonReconcile,
			Message: "pvc-d: cannot resize due to unknown error",
		})

		got := aggregator.getAggregatedCondition()
		Expect(got.Type).To(Equal(string(v1alpha1.ConditionTypeResizing)))
		Expect(got.Reason).To(Equal(ReasonReconcile))
		Expect(got.Status).To(Equal(metav1.ConditionTrue))
		Expect(got.Message).To(Equal("PersistentVolumeClaims are being resized:\n- pvc-a: max capacity reached\n- pvc-b: resizing from 1Gi to 2Gi\n- pvc-d: cannot resize due to unknown error"))
	})

	It("should aggregate to False when there are no True conditions", func() {
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionFalse,
			Reason:  ReasonPVCResizeCooldown,
			Message: "pvc-a: cooldown duration has not elapsed yet",
		})
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionFalse,
			Reason:  ReasonReconcile,
			Message: "pvc-b: max capacity reached",
		})
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionUnknown,
			Reason:  ReasonReconcile,
			Message: "pvc-d: cannot resize due to unknown error",
		})

		got := aggregator.getAggregatedCondition()
		Expect(got.Type).To(Equal(string(v1alpha1.ConditionTypeResizing)))
		Expect(got.Reason).To(Equal(ReasonReconcile))
		Expect(got.Status).To(Equal(metav1.ConditionFalse))
		Expect(got.Message).To(Equal("PersistentVolumeClaims cannot be resized:\n- pvc-a: cooldown duration has not elapsed yet\n- pvc-b: max capacity reached\n- pvc-d: cannot resize due to unknown error"))
	})

	It("should aggregate to Unknown when there are only Unknpwn conditions", func() {
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionUnknown,
			Reason:  ReasonReconcile,
			Message: "pvc-a: cannot resize due to unknown error",
		})
		aggregator.addCondition(metav1.Condition{
			Status:  metav1.ConditionUnknown,
			Reason:  ReasonReconcile,
			Message: "pvc-b: cannot resize due to unknown error",
		})

		got := aggregator.getAggregatedCondition()
		Expect(got.Type).To(Equal(string(v1alpha1.ConditionTypeResizing)))
		Expect(got.Reason).To(Equal(ReasonReconcile))
		Expect(got.Status).To(Equal(metav1.ConditionUnknown))
		Expect(got.Message).To(Equal("PersistentVolumeClaims cannot be resized:\n- pvc-a: cannot resize due to unknown error\n- pvc-b: cannot resize due to unknown error"))
	})

	It("should sort messages deterministically regardless of insertion order", func() {
		aggregator.addCondition(metav1.Condition{
			Status: metav1.ConditionTrue, Message: "pvc-c: resizing",
		})
		aggregator.addCondition(metav1.Condition{
			Status: metav1.ConditionTrue, Message: "pvc-a: resizing",
		})
		aggregator.addCondition(metav1.Condition{
			Status: metav1.ConditionTrue, Message: "pvc-b: resizing",
		})

		got := aggregator.getAggregatedCondition()
		Expect(got.Message).To(Equal("PersistentVolumeClaims are being resized:\n- pvc-a: resizing\n- pvc-b: resizing\n- pvc-c: resizing"))
	})
})
