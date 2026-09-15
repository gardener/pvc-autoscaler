// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gardener/pvc-autoscaler/internal/utils"
)

var _ = Describe("Scheduling Gates", func() {
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
