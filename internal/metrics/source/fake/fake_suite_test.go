package fake_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFake(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Fake Suite")
}
