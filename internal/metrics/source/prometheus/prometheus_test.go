// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/apimachinery/pkg/types"

	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
)

// sample is a single Prometheus vector element used to build fake query
// responses in the tests below.
type sample struct {
	namespace string
	pvc       string
	value     int
}

// vector builds a [model.Vector] from the given samples, labelling each with
// the namespace and persistentvolumeclaim labels that [Prometheus.Get] keys on.
func vector(samples ...sample) model.Vector {
	v := make(model.Vector, 0, len(samples))
	for _, s := range samples {
		v = append(v, &model.Sample{
			Metric: model.Metric{
				"namespace":             model.LabelValue(s.namespace),
				"persistentvolumeclaim": model.LabelValue(s.pvc),
			},
			Value: model.SampleValue(s.value),
		})
	}

	return v
}

// fakeAPI satisfies [promv1.API] by embedding it and overriding only Query,
// which returns a vector keyed by the query string.
type fakeAPI struct {
	promv1.API

	responses map[string]model.Vector
}

func (f fakeAPI) Query(_ context.Context, query string, _ time.Time, _ ...promv1.Option) (model.Value, promv1.Warnings, error) {
	return f.responses[query], nil, nil
}

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

	Context("Get all metrics", func() {
		const (
			availBytesQuery  = "avail-bytes-query"
			capBytesQuery    = "cap-bytes-query"
			availInodesQuery = "avail-inodes-query"
			capInodesQuery   = "cap-inodes-query"
		)

		// newSource creates a Prometheus source whose API returns the given
		// per-query responses, bypassing the HTTP transport entirely.
		newSource := func(responses map[string]model.Vector) *Prometheus {
			p, err := New(
				WithAddress("http://localhost:9090/"),
				WithAvailableBytesQuery(availBytesQuery),
				WithCapacityBytesQuery(capBytesQuery),
				WithAvailableInodesQuery(availInodesQuery),
				WithCapacityInodesQuery(capInodesQuery),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(p).NotTo(BeNil())
			p.api = fakeAPI{responses: responses}

			return p
		}

		It("should retain a PVC when all four series are present", func() {
			key := types.NamespacedName{Namespace: "foo", Name: "pvc-a"}
			p := newSource(map[string]model.Vector{
				// AvailableBytes is a legitimate 0 (full volume) and must be kept.
				availBytesQuery:  vector(sample{namespace: "foo", pvc: "pvc-a", value: 0}),
				capBytesQuery:    vector(sample{namespace: "foo", pvc: "pvc-a", value: 100}),
				availInodesQuery: vector(sample{namespace: "foo", pvc: "pvc-a", value: 10}),
				capInodesQuery:   vector(sample{namespace: "foo", pvc: "pvc-a", value: 20}),
			})

			metrics, err := p.Get(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(metrics).To(HaveKey(key))
			Expect(metrics[key]).To(Equal(&metricssource.VolumeInfo{
				AvailableBytes:  0,
				CapacityBytes:   100,
				AvailableInodes: 10,
				CapacityInodes:  20,
			}))
		})

		It("should drop a PVC when a series is missing", func() {
			key := types.NamespacedName{Namespace: "foo", Name: "pvc-a"}
			p := newSource(map[string]model.Vector{
				capBytesQuery:    vector(sample{namespace: "foo", pvc: "pvc-a", value: 100}),
				availInodesQuery: vector(sample{namespace: "foo", pvc: "pvc-a", value: 10}),
				capInodesQuery:   vector(sample{namespace: "foo", pvc: "pvc-a", value: 20}),
			})

			metrics, err := p.Get(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(metrics).NotTo(HaveKey(key))
		})

		It("should keep complete PVCs and drop incomplete ones", func() {
			complete := types.NamespacedName{Namespace: "foo", Name: "pvc-complete"}
			incomplete := types.NamespacedName{Namespace: "foo", Name: "pvc-incomplete"}
			p := newSource(map[string]model.Vector{
				availBytesQuery: vector(
					sample{namespace: "foo", pvc: "pvc-complete", value: 50},
				),
				capBytesQuery: vector(
					sample{namespace: "foo", pvc: "pvc-complete", value: 100},
					sample{namespace: "foo", pvc: "pvc-incomplete", value: 100},
				),
				availInodesQuery: vector(
					sample{namespace: "foo", pvc: "pvc-complete", value: 10},
					sample{namespace: "foo", pvc: "pvc-incomplete", value: 10},
				),
				capInodesQuery: vector(
					sample{namespace: "foo", pvc: "pvc-complete", value: 20},
					sample{namespace: "foo", pvc: "pvc-incomplete", value: 20},
				),
			})

			metrics, err := p.Get(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(metrics).To(HaveKey(complete))
			Expect(metrics).NotTo(HaveKey(incomplete))
			Expect(metrics[complete]).To(Equal(&metricssource.VolumeInfo{
				AvailableBytes:  50,
				CapacityBytes:   100,
				AvailableInodes: 10,
				CapacityInodes:  20,
			}))
		})
	})
})
