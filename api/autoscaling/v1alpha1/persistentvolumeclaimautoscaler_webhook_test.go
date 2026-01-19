// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"github.com/gardener/pvc-autoscaler/internal/common"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ = Describe("PersistentVolumeClaimAutoscaler Webhook", func() {
	Context("When creating PersistentVolumeClaimAutoscaler under Defaulting Webhook", func() {
		It("Should fill in default values if empty", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-1",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-1",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								MinStepAbsolute:  resource.MustParse("1Gi"),
								CooldownDuration: metav1.Duration{Duration: 3600},
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			pvca := &PersistentVolumeClaimAutoscaler{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), pvca)).To(Succeed())
			Expect(pvca.Spec.VolumePolicies).To(HaveLen(1))
			Expect(pvca.Spec.VolumePolicies[0].ScaleUp.StepPercent).To(Equal(ptr.To(common.DefaultIncreaseByValue)))
			Expect(pvca.Spec.VolumePolicies[0].ScaleUp.UtilizationThresholdPercent).To(Equal(ptr.To(common.DefaultThresholdValue)))
		})
	})

	Context("When creating PersistentVolumeClaimAutoscaler under Validating Webhook", func() {
		It("Should admit if all required fields are provided", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-2",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-2",
					},
					VolumePolicies: []VolumePolicy{
						{
							MinCapacity: resource.MustParse("1Gi"),
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								MinStepAbsolute:             resource.MustParse("1Gi"),
								CooldownDuration:            metav1.Duration{Duration: 3600},
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			Expect(k8sClient.Delete(ctx, obj)).To(Succeed())
		})

		It("Should deny if no volume policies are specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-3",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-3",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if more than one volume policy is specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-4",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-4",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
							},
						},
						{
							MaxCapacity: resource.MustParse("10Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if maxCapacity is not specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-5",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-5",
					},
					VolumePolicies: []VolumePolicy{
						{
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if minCapacity is greater than maxCapacity", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-6",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-6",
					},
					VolumePolicies: []VolumePolicy{
						{
							MinCapacity: resource.MustParse("10Gi"),
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if invalid stepPercent is specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-7",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-7",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(200),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if invalid utilizationThresholdPercent is specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-8",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-8",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(200),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if zero minStepAbsolute is specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-9",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-9",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								MinStepAbsolute:             resource.MustParse("0Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if invalid cooldownDuration is specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-10",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-10",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								MinStepAbsolute:             resource.MustParse("1Gi"),
								CooldownDuration:            metav1.Duration{Duration: 0},
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if targetRef kind is empty", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-11",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "",
						Name:       "pvc-11",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								CooldownDuration:            metav1.Duration{Duration: 3600},
								MinStepAbsolute:             resource.MustParse("1Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if targetRef kind contains invalid characters", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-12",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "invalid/kind",
						Name:       "pvc-12",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								CooldownDuration:            metav1.Duration{Duration: 3600},
								MinStepAbsolute:             resource.MustParse("1Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if targetRef kind is not PersistentVolumeClaim", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-13",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "StatefulSet",
						Name:       "pvc-13",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								CooldownDuration:            metav1.Duration{Duration: 3600},
								MinStepAbsolute:             resource.MustParse("1Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if targetRef name is empty", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-14",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								CooldownDuration:            metav1.Duration{Duration: 3600},
								MinStepAbsolute:             resource.MustParse("1Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if targetRef name contains invalid characters", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-15",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v1",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc/invalid/name",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								CooldownDuration:            metav1.Duration{Duration: 3600},
								MinStepAbsolute:             resource.MustParse("1Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if targetRef apiVersion is empty", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-16",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-16",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								CooldownDuration:            metav1.Duration{Duration: 3600},
								MinStepAbsolute:             resource.MustParse("1Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if targetRef apiVersion is not v1", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-17",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						APIVersion: "v2",
						Kind:       "PersistentVolumeClaim",
						Name:       "pvc-17",
					},
					VolumePolicies: []VolumePolicy{
						{
							MaxCapacity: resource.MustParse("5Gi"),
							ScaleUp: ScaleUpPolicy{
								UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
								StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
								CooldownDuration:            metav1.Duration{Duration: 3600},
								MinStepAbsolute:             resource.MustParse("1Gi"),
							},
						},
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

	})
})
