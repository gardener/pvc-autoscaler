// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package offlineresize_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	. "github.com/gardener/pvc-autoscaler/internal/controller/offlineresize"
)

var _ = Describe("Predicates", func() {
	withCondition := func(pvc *corev1.PersistentVolumeClaim, conditionType corev1.PersistentVolumeClaimConditionType) *corev1.PersistentVolumeClaim {
		pvc.Status.Conditions = append(pvc.Status.Conditions, corev1.PersistentVolumeClaimCondition{
			Type:   conditionType,
			Status: corev1.ConditionTrue,
		})

		return pvc
	}

	Describe("#PVCIsInResizeErrorOrFileSystemResizePending", func() {
		var pvc *corev1.PersistentVolumeClaim

		BeforeEach(func() {
			pvc = &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test",
					Namespace: "default",
					Annotations: map[string]string{
						common.AnnotationPreviousSize: "10Gi",
					},
				},
			}
		})

		When("the object is not a PersistentVolumeClaim", func() {
			It("should return false", func() {
				Expect(PVCIsInResizeErrorOrFileSystemResizePending(&corev1.Pod{})).To(BeFalse())
			})
		})

		When("the PVC does not carry the previous-size annotation", func() {
			BeforeEach(func() {
				delete(pvc.Annotations, common.AnnotationPreviousSize)
				withCondition(pvc, corev1.PersistentVolumeClaimControllerResizeError)
			})

			It("should return false", func() {
				Expect(PVCIsInResizeErrorOrFileSystemResizePending(pvc)).To(BeFalse())
			})
		})

		When("the PVC carries the previous-size annotation but has no relevant condition", func() {
			It("should return false", func() {
				Expect(PVCIsInResizeErrorOrFileSystemResizePending(pvc)).To(BeFalse())
			})
		})

		When("the PVC has the ControllerResizeError condition", func() {
			BeforeEach(func() {
				withCondition(pvc, corev1.PersistentVolumeClaimControllerResizeError)
			})

			It("should return true", func() {
				Expect(PVCIsInResizeErrorOrFileSystemResizePending(pvc)).To(BeTrue())
			})
		})

		When("the PVC has the FileSystemResizePending condition", func() {
			BeforeEach(func() {
				withCondition(pvc, corev1.PersistentVolumeClaimFileSystemResizePending)
			})

			It("should return true", func() {
				Expect(PVCIsInResizeErrorOrFileSystemResizePending(pvc)).To(BeTrue())
			})
		})
	})

	Describe("#PVCARecommendationsOrPoliciesChangedPredicate", func() {
		var (
			predicate        = PVCARecommendationsOrPoliciesChangedPredicate()
			oldPVCA, newPVCA *v1alpha1.PersistentVolumeClaimAutoscaler
		)

		BeforeEach(func() {
			oldPVCA = &v1alpha1.PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pvca", Namespace: "default"},
				Spec: v1alpha1.PersistentVolumeClaimAutoscalerSpec{
					VolumePolicies: []v1alpha1.VolumePolicy{{
						Match:       v1alpha1.Match{Name: "*"},
						MaxCapacity: resource.MustParse("100Gi"),
						ScaleUp:     &v1alpha1.ScalingRules{ResizeStrategy: v1alpha1.PreferInPlaceVolumeResizeStrategy},
					}},
				},
				Status: v1alpha1.PersistentVolumeClaimAutoscalerStatus{
					VolumeRecommendations: []v1alpha1.VolumeRecommendation{{Name: "pvc-a"}},
				},
			}
			newPVCA = oldPVCA.DeepCopy()
		})

		It("should return false on create events", func() {
			Expect(predicate.Create(event.CreateEvent{Object: newPVCA})).To(BeFalse())
		})

		It("should return false on delete events", func() {
			Expect(predicate.Delete(event.DeleteEvent{Object: newPVCA})).To(BeFalse())
		})

		It("should return false on generic events", func() {
			Expect(predicate.Generic(event.GenericEvent{Object: newPVCA})).To(BeFalse())
		})

		When("neither the recommendations nor the policies changed", func() {
			It("should return false", func() {
				Expect(predicate.Update(event.UpdateEvent{ObjectOld: oldPVCA, ObjectNew: newPVCA})).To(BeFalse())
			})
		})

		When("only the order of the recommendations changed", func() {
			BeforeEach(func() {
				oldPVCA.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{{Name: "pvc-a"}, {Name: "pvc-b"}}
				newPVCA.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{{Name: "pvc-b"}, {Name: "pvc-a"}}
			})

			It("should return false", func() {
				Expect(predicate.Update(event.UpdateEvent{ObjectOld: oldPVCA, ObjectNew: newPVCA})).To(BeFalse())
			})
		})

		When("the set of recommended PVC names changed", func() {
			BeforeEach(func() {
				newPVCA.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{{Name: "pvc-b"}}
			})

			It("should return true", func() {
				Expect(predicate.Update(event.UpdateEvent{ObjectOld: oldPVCA, ObjectNew: newPVCA})).To(BeTrue())
			})
		})

		When("the volume policies changed", func() {
			BeforeEach(func() {
				newPVCA.Spec.VolumePolicies[0].ScaleUp.ResizeStrategy = v1alpha1.InPlaceVolumeResizeStrategy
			})

			It("should return true", func() {
				Expect(predicate.Update(event.UpdateEvent{ObjectOld: oldPVCA, ObjectNew: newPVCA})).To(BeTrue())
			})
		})

		When("one of the objects is not a PVCA", func() {
			It("should return false", func() {
				Expect(predicate.Update(event.UpdateEvent{ObjectOld: &corev1.Pod{}, ObjectNew: newPVCA})).To(BeFalse())
				Expect(predicate.Update(event.UpdateEvent{ObjectOld: oldPVCA, ObjectNew: &corev1.Pod{}})).To(BeFalse())
			})
		})
	})

	Describe("#PodWithSchedulingGatePredicate", func() {
		var (
			predicate = PodWithSchedulingGatePredicate()
			gatedPod  *corev1.Pod
			plainPod  *corev1.Pod
		)

		BeforeEach(func() {
			gatedPod = &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "gated", Namespace: "default"},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{{Name: common.SchedulingGateOfflineResize}},
				},
			}
			plainPod = &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: "default"},
			}
		})

		It("should return true on create of a gated pod", func() {
			Expect(predicate.Create(event.CreateEvent{Object: gatedPod})).To(BeTrue())
		})

		It("should return false on create of a pod without the gate", func() {
			Expect(predicate.Create(event.CreateEvent{Object: plainPod})).To(BeFalse())
		})

		It("should return false on update events", func() {
			Expect(predicate.Update(event.UpdateEvent{ObjectOld: plainPod, ObjectNew: gatedPod})).To(BeFalse())
		})

		It("should return false on delete events", func() {
			Expect(predicate.Delete(event.DeleteEvent{Object: gatedPod})).To(BeFalse())
		})

		It("should return false on generic events", func() {
			Expect(predicate.Generic(event.GenericEvent{Object: gatedPod})).To(BeFalse())
		})

		It("should return false when the object is not a pod", func() {
			Expect(predicate.Create(event.CreateEvent{Object: &corev1.PersistentVolumeClaim{}})).To(BeFalse())
		})
	})
})
