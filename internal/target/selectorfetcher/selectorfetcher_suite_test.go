// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
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
