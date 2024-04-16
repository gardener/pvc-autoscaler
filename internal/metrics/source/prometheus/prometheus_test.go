// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
)

// p, err := prometheus.New(
// 	prometheus.WithAddress("http://localhost:9090/"),
// 	prometheus.WithAvailableBytesQuery("some-avail-bytes-query"),
// 	prometheus.WithCapacityBytesQuery("some-capacity-bytes-query"),
// 	prometheus.WithAvailableInodesQuery("some-available-inodes-query"),
// 	prometheus.WithCapacityInodesQuery("some-capacity-inodes-query"),
// 	prometheus.WithHTTPClient(http.DefaultClient),
// 	prometheus.WithRoundTripper(nil),
// )

var _ = Describe("Prometheus", func() {
	Context("Create new Prometheus source", func() {
		It("should fail because of missing address", func() {
			p, err := New()
			Expect(err).To(MatchError(ErrNoPrometheusAddress))
			Expect(p).To(BeNil())
		})

		It("should use default queries", func() {
			p, err := New(
				WithAddress("http://localhost:9090/"),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(p).NotTo(BeNil())

			// No custom queries provided, so we should use the default ones
			Expect(p.availableBytesQuery).To(Equal(metricssource.KubeletVolumeStatsAvailableBytes))
			Expect(p.availableInodesQuery).To(Equal(metricssource.KubeletVolumeStatsInodesFree))
			Expect(p.capacityBytesQuery).To(Equal(metricssource.KubeletVolumeStatsCapacityBytes))
			Expect(p.capacityInodesQuery).To(Equal(metricssource.KubeletVolumeStatsInodes))
		})

		It("should use custom queries", func() {
			p, err := New(
				WithAddress("http://localhost:9090/"),
				WithAvailableBytesQuery("my-available-bytes-query"),
				WithAvailableInodesQuery("my-available-inodes-query"),
				WithCapacityBytesQuery("my-capacity-bytes-query"),
				WithCapacityInodesQuery("my-capacity-inodes-query"),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(p).NotTo(BeNil())

			Expect(p.availableBytesQuery).To(Equal("my-available-bytes-query"))
			Expect(p.availableInodesQuery).To(Equal("my-available-inodes-query"))
			Expect(p.capacityBytesQuery).To(Equal("my-capacity-bytes-query"))
			Expect(p.capacityInodesQuery).To(Equal("my-capacity-inodes-query"))
		})

		It("should use custom http.Client", func() {
			c := &http.Client{Timeout: 1 * time.Second}

			p, err := New(
				WithAddress("http://localhost:9090/"),
				WithHTTPClient(c),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(p).NotTo(BeNil())
			Expect(p.httpClient).To(BeEquivalentTo(c))
		})

		It("should use custom http.RoundTripper", func() {
			t := &http.Transport{TLSHandshakeTimeout: 1 * time.Second}

			p, err := New(
				WithAddress("http://localhost:9090/"),
				WithRoundTripper(t),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(p).NotTo(BeNil())
			Expect(p.roundTripper).To(BeEquivalentTo(t))
		})

		It("should fail - http.Client and http.RoundTripper are mutually exclusive", func() {
			c := &http.Client{Timeout: 1 * time.Second}
			t := &http.Transport{TLSHandshakeTimeout: 1 * time.Second}

			p, err := New(
				WithAddress("http://localhost:9090/"),
				WithHTTPClient(c),
				WithRoundTripper(t),
			)
			Expect(err).To(HaveOccurred())
			Expect(p).To(BeNil())
		})
	})
})
