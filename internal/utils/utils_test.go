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
})
