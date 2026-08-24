// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

var _ = Describe("Utils", func() {
	Describe("#ParsePercentage", func() {
		It("should succeed", func() {
			tests := []struct {
				val  string
				want float64
			}{
				{val: "20%", want: 20.0},
				{val: " 20%", want: 20.0},
				{val: "  20%  ", want: 20.0},
			}
			for _, test := range tests {
				Expect(utils.ParsePercentage(test.val)).To(Equal(test.want))
			}
		})
		It("should fail", func() {
			values := []string{"10", "20 %", " foobar", "", "1000%", "-100%"}
			for _, val := range values {
				_, err := utils.ParsePercentage(val)
				Expect(err).To(MatchError(utils.ErrBadPercentageValue))
			}
		})
	})

	Describe("#IsPersistentVolumeClaimConditionPresentAndEqual", func() {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-pvc",
				Namespace: "default",
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Conditions: []corev1.PersistentVolumeClaimCondition{
					{
						Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
						Status: corev1.ConditionTrue,
					},
					{
						Type:   corev1.PersistentVolumeClaimResizing,
						Status: corev1.ConditionFalse,
					},
					{
						Type:   corev1.PersistentVolumeClaimVolumeModifyingVolume,
						Status: corev1.ConditionUnknown,
					},
				},
			},
		}

		It("should be present and true", func() {
			Expect(utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimFileSystemResizePending)).To(BeTrue())
		})

		It("should be present and equal", func() {
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimFileSystemResizePending, corev1.ConditionTrue)).To(BeTrue())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimResizing, corev1.ConditionFalse)).To(BeTrue())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimVolumeModifyingVolume, corev1.ConditionUnknown)).To(BeTrue())
		})

		It("should be present and false", func() {
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimFileSystemResizePending, corev1.ConditionFalse)).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimResizing, corev1.ConditionTrue)).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimResizing, corev1.ConditionTrue)).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimVolumeModifyVolumeError, corev1.ConditionTrue)).To(BeFalse())
		})
	})

	Context("scheduling gates", func() {
		const gate = "pvc.autoscaling.gardener.cloud/offline-resize"

		var newPod = func(gates ...string) *corev1.Pod {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "sample-pod", Namespace: "default"},
			}
			for _, gate := range gates {
				pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: gate})
			}
			return pod
		}

		Describe("#HasSchedulingGate", func() {
			It("should return true when the gate is present", func() {
				Expect(utils.HasSchedulingGate(newPod("other", gate), gate)).To(BeTrue())
			})

			It("should return false when the gate is absent", func() {
				Expect(utils.HasSchedulingGate(newPod("other"), gate)).To(BeFalse())
			})
		})

		Describe("#AddSchedulingGate", func() {
			It("should add the gate and report modification", func() {
				pod := newPod("other")
				utils.AddSchedulingGate(pod, gate)

				Expect(pod.Spec.SchedulingGates).To(HaveLen(2))
				Expect(pod.Spec.SchedulingGates).To(ContainElement(HaveField("Name", gate)))
			})

			It("should be idempotent when the gate is already present", func() {
				pod := newPod(gate)
				utils.AddSchedulingGate(pod, gate)

				Expect(pod.Spec.SchedulingGates).To(HaveLen(1))
			})
		})

		Describe("#RemoveSchedulingGate", func() {
			It("should remove the gate and report modification", func() {
				pod := newPod("other", gate)
				utils.RemoveSchedulingGate(pod, gate)

				Expect(pod.Spec.SchedulingGates).To(HaveLen(1))
				Expect(pod.Spec.SchedulingGates).To(ConsistOf(HaveField("Name", "other")))
			})

			It("should be a no-op when the gate is absent", func() {
				pod := newPod("other")
				utils.RemoveSchedulingGate(pod, gate)

				Expect(pod.Spec.SchedulingGates).To(HaveLen(1))
				Expect(pod.Spec.SchedulingGates).To(ConsistOf(HaveField("Name", "other")))
			})
		})
	})

	Describe("#GetVolumePolicy", func() {
		It("should return error on invalid glob pattern", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "[",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid volume policy name \"[\""))
			Expect(policy).To(BeNil())
		})

		It("should return nil when no policy matches", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "other-pvc",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).To(BeNil())
		})

		It("should match exact name", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "data-pvc",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("data-pvc"))
		})

		It("should match glob pattern", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "*-logs",
					},
					MaxCapacity: resource.MustParse("15Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("app-logs", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*-logs"))
		})

		It("should match default policy", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*"))
		})

		It("should fall back to default policy when no other policy matches", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "other-pvc",
					},
					MaxCapacity: resource.MustParse("20Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*"))
			Expect(policy.MaxCapacity).To(Equal(resource.MustParse("5Gi")))
		})

		It("should return first seen matching policy", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "data-*",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "data-pvc",
					},
					MaxCapacity: resource.MustParse("20Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("data-*"))
			Expect(policy.MaxCapacity).To(Equal(resource.MustParse("10Gi")))
		})
	})

	Describe("#FindOwningPVCAAndPolicy", func() {
		const (
			namespace      = "default"
			autoscalerName = "instance-a"
		)

		var (
			ctx  context.Context
			pvc  *corev1.PersistentVolumeClaim
			pvca *v1alpha1.PersistentVolumeClaimAutoscaler

			testScheme *runtime.Scheme
		)

		BeforeEach(func() {
			ctx = context.Background()

			testScheme = runtime.NewScheme()
			Expect(corev1.AddToScheme(testScheme)).To(Succeed())
			Expect(v1alpha1.AddToScheme(testScheme)).To(Succeed())

			pvc = &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: namespace},
			}

			pvca = &v1alpha1.PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvca", Namespace: namespace},
				Spec: v1alpha1.PersistentVolumeClaimAutoscalerSpec{
					AutoscalerName: autoscalerName,
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						Kind:       "PersistentVolumeClaim",
						Name:       "test",
						APIVersion: "v1",
					},
					VolumePolicies: []v1alpha1.VolumePolicy{{Match: v1alpha1.Match{Name: "*"}, MaxCapacity: resource.MustParse("100Gi")}},
				},
				Status: v1alpha1.PersistentVolumeClaimAutoscalerStatus{
					VolumeRecommendations: []v1alpha1.VolumeRecommendation{{Name: "test"}},
				},
			}
		})

		DescribeTable("should resolve PVC ownership",
			func(mutate func(*v1alpha1.PersistentVolumeClaimAutoscaler), seedPVCA, wantOwner bool) {
				mutate(pvca)

				objs := []client.Object{pvc}
				if seedPVCA {
					objs = append(objs, pvca)
				}

				c := fake.NewClientBuilder().
					WithScheme(testScheme).
					WithObjects(objs...).
					WithIndex(&v1alpha1.PersistentVolumeClaimAutoscaler{}, v1alpha1.AutoscalerNameIndexKey, func(obj client.Object) []string {
						return []string{obj.(*v1alpha1.PersistentVolumeClaimAutoscaler).Spec.AutoscalerName}
					}).
					Build()

				owner, policy, err := utils.FindOwningPVCAAndPolicy(ctx, c, autoscalerName, pvc)
				Expect(err).NotTo(HaveOccurred())

				if wantOwner {
					Expect(owner).NotTo(BeNil())
					Expect(owner.Name).To(Equal(pvca.Name))
					// The seeded policy matches all PVCs, so an owned PVC always
					// resolves to a matching policy.
					Expect(policy).NotTo(BeNil())
				} else {
					Expect(owner).To(BeNil())
					Expect(policy).To(BeNil())
				}
			},
			Entry("when the PVCA status lists the PVC", func(*v1alpha1.PersistentVolumeClaimAutoscaler) {}, true, true),
			Entry("when no PVCA exists", func(*v1alpha1.PersistentVolumeClaimAutoscaler) {}, false, false),
			Entry("when the managing PVCA has a different autoscalerName", func(p *v1alpha1.PersistentVolumeClaimAutoscaler) { p.Spec.AutoscalerName = "other" }, true, false),
			Entry("when the PVCA status does not list the PVC", func(p *v1alpha1.PersistentVolumeClaimAutoscaler) {
				p.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{{Name: "other-pvc"}}
			}, true, false),
			Entry("when the PVCA is in a different namespace", func(p *v1alpha1.PersistentVolumeClaimAutoscaler) { p.Namespace = "other-namespace" }, true, false),
		)
	})

	Context("#IsOfflineResizeEnabled", func() {
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
