// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"

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

	Describe("#IsOfflineResizeRequired", func() {
		DescribeTable("should report whether the resize error requires an offline resize",
			func(message string, want bool) {
				pvc := &corev1.PersistentVolumeClaim{
					Status: corev1.PersistentVolumeClaimStatus{
						Conditions: []corev1.PersistentVolumeClaimCondition{{
							Type:    corev1.PersistentVolumeClaimControllerResizeError,
							Status:  corev1.ConditionTrue,
							Message: message,
						}},
					},
				}
				Expect(utils.IsOfflineResizeRequired(pvc)).To(Equal(want))
			},
			Entry("Azure unsupported disk property change (full message, VM size embedded)",
				"Change in disk property of VM of size 'Standard_A4_v2' is not supported.", true),
			Entry("Azure resize-while-attached (full message, disk sizes embedded)",
				"Disk of size 1024 GB (<=4096 GB) cannot be resized to 10240 GB (>4096 GB) while it is attached to a running VM. Please stop your VM or detach the disk and retry the operation", true),
			Entry("matches case-insensitively",
				"CHANGE IN DISK PROPERTY OF VM OF SIZE 'Standard_D2' IS NOT SUPPORTED.", true),
			Entry("matches a fragment embedded in a longer wrapped message",
				"failed to resize disk(/subscriptions/x/disk) with error(while it is attached to a running VM)", true),
			Entry("does not match an unrelated resize error",
				"failed to resize disk: quota exceeded for the subscription", false),
			Entry("does not match an empty message", "", false),
		)
	})
})
