// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package version_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/version"
)

var _ = Describe("Version", func() {
	Context("# IsKubernetesVersionGreaterEqual134", func() {
		DescribeTable("should report the correct result",
			func(v string, expected bool) {
				Expect(version.IsKubernetesVersionGreaterEqual134(v)).To(Equal(expected))
			},
			Entry("1.34.0", "1.34.0", true),
			Entry("v1.34.0", "v1.34.0", true),
			Entry("1.28.0", "1.28.0", false),
			Entry("v1.34.2+build", "v1.34.2+build", true),
			Entry("empty string", "", false),
			Entry("non-version", "not-a-version", false),
		)
	})
})
