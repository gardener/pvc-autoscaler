package utils_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

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
})
