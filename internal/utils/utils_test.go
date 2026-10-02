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
})
