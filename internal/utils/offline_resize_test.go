// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

var _ = Describe("Offline Resize", func() {
	Describe("#IsOfflineResizeEnabled", func() {
		var (
			workloadPVCA *v1alpha1.PersistentVolumeClaimAutoscaler
			preferPolicy *v1alpha1.VolumePolicy
		)

		BeforeEach(func() {
			workloadPVCA = &v1alpha1.PersistentVolumeClaimAutoscaler{
				Spec: v1alpha1.PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{Kind: "StatefulSet"},
				},
			}
			preferPolicy = &v1alpha1.VolumePolicy{ScaleUp: &v1alpha1.ScalingRules{ResizeStrategy: v1alpha1.PreferInPlaceVolumeResizeStrategy}}
		})

		DescribeTable("should report whether offline-resize recovery is enabled",
			func(mutate func(*v1alpha1.PersistentVolumeClaimAutoscaler, *v1alpha1.VolumePolicy), want bool) {
				mutate(workloadPVCA, preferPolicy)
				Expect(utils.IsOfflineResizeEnabled(workloadPVCA, preferPolicy)).To(Equal(want))
			},
			Entry("when the PVCA targets a workload and the strategy is PreferInPlace",
				func(*v1alpha1.PersistentVolumeClaimAutoscaler, *v1alpha1.VolumePolicy) {}, true),
			Entry("when the PVCA is nil",
				func(_ *v1alpha1.PersistentVolumeClaimAutoscaler, _ *v1alpha1.VolumePolicy) { workloadPVCA = nil }, false),
			Entry("when the PVCA targets a PersistentVolumeClaim directly",
				func(p *v1alpha1.PersistentVolumeClaimAutoscaler, _ *v1alpha1.VolumePolicy) {
					p.Spec.TargetRef.Kind = "PersistentVolumeClaim"
				}, false),
			Entry("when the policy is nil",
				func(_ *v1alpha1.PersistentVolumeClaimAutoscaler, _ *v1alpha1.VolumePolicy) { preferPolicy = nil }, false),
			Entry("when scaleUp is nil",
				func(_ *v1alpha1.PersistentVolumeClaimAutoscaler, p *v1alpha1.VolumePolicy) { p.ScaleUp = nil }, false),
			Entry("when the strategy is unset",
				func(_ *v1alpha1.PersistentVolumeClaimAutoscaler, p *v1alpha1.VolumePolicy) {
					p.ScaleUp.ResizeStrategy = ""
				}, false),
			Entry("when the strategy is InPlace",
				func(_ *v1alpha1.PersistentVolumeClaimAutoscaler, p *v1alpha1.VolumePolicy) {
					p.ScaleUp.ResizeStrategy = v1alpha1.InPlaceVolumeResizeStrategy
				}, false),
			Entry("when the strategy is Off",
				func(_ *v1alpha1.PersistentVolumeClaimAutoscaler, p *v1alpha1.VolumePolicy) {
					p.ScaleUp.ResizeStrategy = v1alpha1.OffVolumeResizeStrategy
				}, false),
		)
	})
})
