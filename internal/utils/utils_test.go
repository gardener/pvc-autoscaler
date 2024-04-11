package utils_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/utils"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

	Context("# GetAnnotation", func() {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "sample-pod",
				Annotations: map[string]string{
					"foo": "bar",
					"baz": "qux",
				},
			},
		}

		It("should return annotation from object", func() {
			Expect(utils.GetAnnotation(pod, "foo", "")).To(Equal("bar"))
			Expect(utils.GetAnnotation(pod, "baz", "")).To(Equal("qux"))
		})
		It("should return default values", func() {
			Expect(utils.GetAnnotation(pod, "unknown", "foobar")).To(Equal("foobar"))
			Expect(utils.GetAnnotation(pod, "missing", "default")).To(Equal("default"))
		})
	})

	Context("# IsPersistentVolumeClaimConditionPresentAndEqual", func() {
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: "sample-pvc",
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
})
