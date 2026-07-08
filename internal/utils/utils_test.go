// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
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

var _ = Describe("Utils", func() {
	Context("# ParsePercentage", func() {
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

	Context("# IsPersistentVolumeClaimConditionPresentAndEqual", func() {
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

		It("is present and true", func() {
			Expect(utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimFileSystemResizePending)).To(BeTrue())
		})

		It("is present and equal", func() {
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimFileSystemResizePending, corev1.ConditionTrue)).To(BeTrue())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimResizing, corev1.ConditionFalse)).To(BeTrue())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimVolumeModifyingVolume, corev1.ConditionUnknown)).To(BeTrue())
		})

		It("is present and false", func() {
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimFileSystemResizePending, corev1.ConditionFalse)).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimResizing, corev1.ConditionTrue)).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimResizing, corev1.ConditionTrue)).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimConditionPresentAndEqual(pvc, corev1.PersistentVolumeClaimVolumeModifyVolumeError, corev1.ConditionTrue)).To(BeFalse())
		})
	})

	Context("# IsPersistentVolumeClaimResizeInfeasible", func() {
		newPVC := func(status corev1.ClaimResourceStatus) *corev1.PersistentVolumeClaim {
			return &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default"},
				Status: corev1.PersistentVolumeClaimStatus{
					AllocatedResourceStatuses: map[corev1.ResourceName]corev1.ClaimResourceStatus{
						corev1.ResourceStorage: status,
					},
				},
			}
		}

		It("returns false when the allocated resource statuses map is empty", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default"},
			}
			Expect(utils.IsPersistentVolumeClaimResizeInfeasible(pvc)).To(BeFalse())
		})

		It("returns false when the storage status indicates progress", func() {
			Expect(utils.IsPersistentVolumeClaimResizeInfeasible(newPVC(corev1.PersistentVolumeClaimControllerResizeInProgress))).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimResizeInfeasible(newPVC(corev1.PersistentVolumeClaimNodeResizeInProgress))).To(BeFalse())
			Expect(utils.IsPersistentVolumeClaimResizeInfeasible(newPVC(corev1.PersistentVolumeClaimNodeResizePending))).To(BeFalse())
		})

		It("returns true for ControllerResizeInfeasible", func() {
			Expect(utils.IsPersistentVolumeClaimResizeInfeasible(newPVC(corev1.PersistentVolumeClaimControllerResizeInfeasible))).To(BeTrue())
		})

		It("returns true for NodeResizeInfeasible", func() {
			Expect(utils.IsPersistentVolumeClaimResizeInfeasible(newPVC(corev1.PersistentVolumeClaimNodeResizeInfeasible))).To(BeTrue())
		})
	})
})
