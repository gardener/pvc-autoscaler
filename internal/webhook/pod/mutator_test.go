// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package pod_test

import (
	"context"
	"encoding/json"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	admissionv1 "k8s.io/api/admission/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	podwebhook "github.com/gardener/pvc-autoscaler/internal/webhook/pod"
)

func TestPodWebhook(t *testing.T) {
	RegisterFailHandler(Fail)
	Expect(v1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	RunSpecs(t, "Pod Webhook Suite")
}

var _ = Describe("Pod Mutator", func() {
	const (
		namespace      = "default"
		autoscalerName = ""
	)

	var (
		ctx     context.Context
		decoder admission.Decoder

		pod  *corev1.Pod
		pvcs []*corev1.PersistentVolumeClaim
		pvca *v1alpha1.PersistentVolumeClaimAutoscaler

		mutated *corev1.Pod

		offlineResizeGate = corev1.PodSchedulingGate{Name: common.SchedulingGateOfflineResize}
	)

	BeforeEach(func() {
		ctx = context.Background()
		decoder = admission.NewDecoder(scheme.Scheme)

		pvcs = []*corev1.PersistentVolumeClaim{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: namespace,
			},
			Status: corev1.PersistentVolumeClaimStatus{
				Conditions: []corev1.PersistentVolumeClaimCondition{{
					Type:   corev1.PersistentVolumeClaimControllerResizeError,
					Status: corev1.ConditionTrue,
				}},
			},
		}}

		pvca = &v1alpha1.PersistentVolumeClaimAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pvca", Namespace: namespace},
			Spec: v1alpha1.PersistentVolumeClaimAutoscalerSpec{
				AutoscalerName: autoscalerName,
				TargetRef: autoscalingv1.CrossVersionObjectReference{
					Kind:       "StatefulSet",
					Name:       "test-sts",
					APIVersion: "apps/v1",
				},
				VolumePolicies: []v1alpha1.VolumePolicy{{
					Match:       v1alpha1.Match{Name: "*"},
					MaxCapacity: resource.MustParse("100Gi"),
					ScaleUp:     &v1alpha1.ScalingRules{ResizeStrategy: v1alpha1.PreferInPlaceVolumeResizeStrategy},
				}},
			},
			Status: v1alpha1.PersistentVolumeClaimAutoscalerStatus{
				VolumeRecommendations: []v1alpha1.VolumeRecommendation{{Name: "test"}},
			},
		}

		pod = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "test-", Namespace: namespace},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "c", Image: "nginx"}},
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "test"},
					},
				}},
			},
		}
	})

	JustBeforeEach(func() {
		objs := make([]client.Object, 0, len(pvcs)+1)
		for _, p := range pvcs {
			objs = append(objs, p)
		}
		if pvca != nil {
			objs = append(objs, pvca)
		}

		c := fake.NewClientBuilder().
			WithScheme(scheme.Scheme).
			WithObjects(objs...).
			WithIndex(&v1alpha1.PersistentVolumeClaimAutoscaler{}, v1alpha1.AutoscalerNameIndexKey, func(obj client.Object) []string {
				return []string{obj.(*v1alpha1.PersistentVolumeClaimAutoscaler).Spec.AutoscalerName}
			}).
			Build()

		raw, err := json.Marshal(pod)
		Expect(err).NotTo(HaveOccurred())

		mutator := podwebhook.NewMutator(c, decoder, autoscalerName)
		response := mutator.Handle(ctx, admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace: namespace,
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		Expect(response.Allowed).To(BeTrue())

		mutated = pod.DeepCopy()
		if len(response.Patches) > 0 {
			patchJSON, err := json.Marshal(response.Patches)
			Expect(err).NotTo(HaveOccurred())
			patch, err := jsonpatch.DecodePatch(patchJSON)
			Expect(err).NotTo(HaveOccurred())
			modified, err := patch.Apply(raw)
			Expect(err).NotTo(HaveOccurred())
			mutated = &corev1.Pod{}
			Expect(json.Unmarshal(modified, mutated)).To(Succeed())
		}
	})

	When("a referenced PVC is managed and has ControllerResizeError", func() {
		It("should add the offline-resize scheduling gate", func() {
			Expect(mutated.Spec.SchedulingGates).To(ConsistOf(offlineResizeGate))
		})

		When("the pod already carries the gate", func() {
			BeforeEach(func() {
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: common.SchedulingGateOfflineResize}}
			})

			It("should not duplicate the gate", func() {
				Expect(mutated.Spec.SchedulingGates).To(ConsistOf(offlineResizeGate))
			})
		})

		When("another referenced PVC is healthy", func() {
			BeforeEach(func() {
				pvcs = append(pvcs, &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "healthy",
						Namespace: namespace,
					},
				})
				pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{
					Name: "data-healthy",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "healthy"},
					},
				})
			})

			It("should still add the gate because one PVC is stuck", func() {
				Expect(mutated.Spec.SchedulingGates).To(ConsistOf(offlineResizeGate))
			})
		})

		When("the managing PVCA uses the InPlace resize strategy", func() {
			BeforeEach(func() {
				pvca.Spec.VolumePolicies[0].ScaleUp.ResizeStrategy = v1alpha1.InPlaceVolumeResizeStrategy
			})

			It("should not add the gate", func() {
				Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
			})
		})

		When("the managing PVCA uses the Off resize strategy", func() {
			BeforeEach(func() {
				pvca.Spec.VolumePolicies[0].ScaleUp.ResizeStrategy = v1alpha1.OffVolumeResizeStrategy
			})

			It("should not add the gate", func() {
				Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
			})
		})

		When("the managing PVCA has no scaleUp rules", func() {
			BeforeEach(func() {
				pvca.Spec.VolumePolicies[0].ScaleUp = nil
			})

			It("should not add the gate", func() {
				Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
			})
		})
	})

	When("the referenced PVC is not managed by any PVCA of this instance", func() {
		BeforeEach(func() {
			pvca = nil
		})

		It("should not add the gate", func() {
			Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
		})
	})

	When("the referenced PVC is managed by a PVCA of another instance", func() {
		BeforeEach(func() {
			pvca.Spec.AutoscalerName = "other"
		})

		It("should not add the gate", func() {
			Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
		})
	})

	When("the referenced PVC already has FileSystemResizePending", func() {
		BeforeEach(func() {
			pvcs[0].Status.Conditions = append(pvcs[0].Status.Conditions, corev1.PersistentVolumeClaimCondition{
				Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
				Status: corev1.ConditionTrue,
			})
		})

		It("should not add the gate", func() {
			Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
		})
	})

	When("the referenced PVC has no relevant condition", func() {
		BeforeEach(func() {
			pvcs[0].Status.Conditions = nil
		})

		It("should not add the gate", func() {
			Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
		})
	})

	When("the referenced PVC does not exist", func() {
		BeforeEach(func() {
			pvcs = nil
		})

		It("should not add the gate", func() {
			Expect(mutated.Spec.SchedulingGates).NotTo(ContainElement(offlineResizeGate))
		})
	})
})
