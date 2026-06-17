// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package healthcheck_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/gardener/pvc-autoscaler/internal/healthcheck"
)

var _ = Describe("Heartbeat", func() {
	var (
		fakeClock *clocktesting.FakeClock
	)

	BeforeEach(func() {
		fakeClock = clocktesting.NewFakeClock(time.Now())
	})

	It("returns nil before monitoring starts", func() {
		heartbeat := healthcheck.NewHeartbeat(1*time.Nanosecond, fakeClock)
		fakeClock.Step(1 * time.Millisecond)

		Expect(heartbeat.Check(nil)).To(Succeed())
	})

	It("returns an error after timeout", func() {
		heartbeat := healthcheck.NewHeartbeat(5*time.Millisecond, fakeClock)
		heartbeat.StartMonitoring()
		fakeClock.Step(15 * time.Millisecond)

		Expect(heartbeat.Check(nil)).To(HaveOccurred())
	})

	It("does not timeout after activity update", func() {
		heartbeat := healthcheck.NewHeartbeat(20*time.Millisecond, fakeClock)
		heartbeat.StartMonitoring()
		fakeClock.Step(5 * time.Millisecond)
		heartbeat.UpdateLastActivity()
		fakeClock.Step(5 * time.Millisecond)

		Expect(heartbeat.Check(nil)).To(Succeed())
	})
})
