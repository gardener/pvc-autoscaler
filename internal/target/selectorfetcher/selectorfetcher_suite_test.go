// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package selectorfetcher_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSelectorFetcher(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SelectorFetcher Suite")
}
