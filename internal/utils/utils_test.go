package utils_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/utils"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	Context("# ValidatePersistentVolumeClaimAnnotations", func() {
		It("shoud succeed with good annotations", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:            "true",
						annotation.IncreaseBy:           "25%",
						annotation.Threshold:            "25%",
						annotation.MaxCapacity:          "100Gi",
						annotation.LastCheck:            "1712826720",
						annotation.NextCheck:            "1712826721",
						annotation.UsedSpacePercentage:  "10.00%",
						annotation.FreeSpacePercentage:  "90.00%",
						annotation.UsedInodesPercentage: "5.00%",
						annotation.FreeInodesPercentage: "95.00%",
						annotation.PrevSize:             "90Gi",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc)).To(Succeed())
		})

		It("shoud fail with zero capacity", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "100Gi",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("0Gi"),
					},
				},
			}
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc)).ShouldNot(Succeed())
		})

		It("should fail with invalid max-capacity annotation", func() {
			pvc1 := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						// No max capacity specified
						annotation.IsEnabled: "true",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			pvc2 := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "foobar", // Bad resource value
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}

			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc1)).To(MatchError(common.ErrNoMaxCapacity))
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc2)).ShouldNot(Succeed())
		})

		It("should fail with bad increase-by annotation", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "100Gi",
						annotation.IncreaseBy:  "bad-increase-by",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc)).ShouldNot(Succeed())
		})

		It("should fail with zero increase-by annotation", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.IncreaseBy:  "0%",
						annotation.MaxCapacity: "100Gi",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc)).To(MatchError(common.ErrZeroPercentage))
		})

		It("should fail with invalid threshold annotation", func() {
			pvc1 := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.Threshold:   "0%", // Zero threshold is invalid
						annotation.MaxCapacity: "100Gi",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			pvc2 := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.Threshold:   "foobar", // Bad threshold
						annotation.MaxCapacity: "100Gi",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc1)).To(MatchError(common.ErrZeroPercentage))
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc2)).ShouldNot(Succeed())

		})

		It("should fail with max capacity less than current capacity", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "sample-pvc",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "50Gi",
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Gi"),
					},
				},
			}
			Expect(utils.ValidatePersistentVolumeClaimAnnotations(pvc)).ShouldNot(Succeed())
		})

	})
})
