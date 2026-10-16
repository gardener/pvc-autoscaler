// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package offlineresize_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	. "github.com/gardener/pvc-autoscaler/internal/controller/offlineresize"
)

var _ = Describe("Mappers", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	Describe("#MapPVCAToPVCs", func() {
		var pvca *v1alpha1.PersistentVolumeClaimAutoscaler

		BeforeEach(func() {
			pvca = &v1alpha1.PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvca", Namespace: "default"},
			}
		})

		When("the object is not a PersistentVolumeClaimAutoscaler", func() {
			It("should return nil", func() {
				Expect(MapPVCAToPVCs(ctx, &corev1.Pod{})).To(BeNil())
			})
		})

		When("the PVCA has no volume recommendations", func() {
			It("should return no requests", func() {
				Expect(MapPVCAToPVCs(ctx, pvca)).To(BeEmpty())
			})
		})

		When("the PVCA has volume recommendations", func() {
			BeforeEach(func() {
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{{Name: "pvc-a"}, {Name: "pvc-b"}}
			})

			It("should return a request per recommended PVC in the PVCA's namespace", func() {
				Expect(MapPVCAToPVCs(ctx, pvca)).To(ConsistOf(
					reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pvc-a"}},
					reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pvc-b"}},
				))
			})
		})
	})

	Describe("#MapPodToPVCs", func() {
		var pod *corev1.Pod

		BeforeEach(func() {
			pod = &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			}
		})

		When("the object is not a Pod", func() {
			It("should return nil", func() {
				Expect(MapPodToPVCs(ctx, &corev1.PersistentVolumeClaim{})).To(BeNil())
			})
		})

		When("the Pod references no PersistentVolumeClaims", func() {
			BeforeEach(func() {
				pod.Spec.Volumes = []corev1.Volume{{
					Name:         "ephemeral",
					VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				}}
			})

			It("should return no requests", func() {
				Expect(MapPodToPVCs(ctx, pod)).To(BeEmpty())
			})
		})

		When("the Pod references PersistentVolumeClaims", func() {
			BeforeEach(func() {
				pod.Spec.Volumes = []corev1.Volume{
					{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc-a"}}},
					{Name: "ephemeral", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					{Name: "logs", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "pvc-b"}}},
				}
			})

			It("should return a request per referenced PVC in the Pod's namespace", func() {
				Expect(MapPodToPVCs(ctx, pod)).To(ConsistOf(
					reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pvc-a"}},
					reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pvc-b"}},
				))
			})
		})
	})
})
