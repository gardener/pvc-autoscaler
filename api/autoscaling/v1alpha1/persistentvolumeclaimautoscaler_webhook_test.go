// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gardener/pvc-autoscaler/internal/common"
)

var _ = Describe("PersistentVolumeClaimAutoscaler Webhook", func() {
	Context("When creating PersistentVolumeClaimAutoscaler under Defaulting Webhook", func() {
		It("Should fill in default increaseBy and threshold if empty", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-1",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					// No increaseBy and threshold specified
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-1",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			pvca := &PersistentVolumeClaimAutoscaler{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), pvca)).To(Succeed())
			Expect(pvca.Spec.IncreaseBy).To(Equal(common.DefaultIncreaseByValue))
			Expect(pvca.Spec.Threshold).To(Equal(common.DefaultThresholdValue))
		})
	})

	Context("When creating PersistentVolumeClaimAutoscaler under Validating Webhook", func() {
		It("Should deny if max capacity is not specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-2",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					// MaxCapacity is not set
					IncreaseBy: common.DefaultIncreaseByValue,
					Threshold:  common.DefaultThresholdValue,
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-2",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if bad percentage values are specified", func() {
			// Bad increaseBy
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-3",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					IncreaseBy:  "bad-increase-by",
					Threshold:   common.DefaultThresholdValue,
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-3",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())

			// Bad threshold
			obj = &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-4",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					IncreaseBy:  common.DefaultIncreaseByValue,
					Threshold:   "bad-threshold",
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-4",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny if no target pvc is specified", func() {
			// No target PVC has been specified
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-5",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					IncreaseBy:  common.DefaultIncreaseByValue,
					Threshold:   common.DefaultThresholdValue,
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should admit if all required fields are provided", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-6",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					IncreaseBy:  common.DefaultIncreaseByValue,
					Threshold:   common.DefaultThresholdValue,
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-6",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).To(Succeed())
			Expect(k8sClient.Delete(ctx, obj)).To(Succeed())

		})

		It("Should deny if zero percentage values are specified", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-7",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					IncreaseBy:  "0%",
					Threshold:   common.DefaultThresholdValue,
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-7",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())

			obj = &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-8",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					IncreaseBy:  common.DefaultIncreaseByValue,
					Threshold:   "0%",
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-8",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).NotTo(Succeed())
		})

		It("Should deny on updating with invalid threshold", func() {
			obj := &PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvca-9",
					Namespace: "default",
				},
				Spec: PersistentVolumeClaimAutoscalerSpec{
					IncreaseBy:  common.DefaultIncreaseByValue,
					Threshold:   common.DefaultThresholdValue,
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleTargetRef: corev1.LocalObjectReference{
						Name: "pvc-9",
					},
				},
			}

			Expect(k8sClient.Create(ctx, obj)).To(Succeed())

			pvca := &PersistentVolumeClaimAutoscaler{}
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), pvca)).To(Succeed())

			pvca.Spec.Threshold = "invalid-threshold"
			Expect(k8sClient.Update(ctx, pvca)).NotTo(Succeed())
		})
	})
})
