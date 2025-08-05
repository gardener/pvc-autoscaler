// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package prometheus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
)

// ErrNoPrometheusAddress is an error, which is returned when no Prometheus
// endpoint address was configured.
var ErrNoPrometheusAddress = errors.New("no address specified")

// Prometheus is an implementation of [metricssource.Source], which collects metrics
// about persistent volume claims from a Prometheus instance.
type Prometheus struct {
	address              string
	api                  promv1.API
	httpClient           *http.Client
	roundTripper         http.RoundTripper
	availableBytesQuery  string
	capacityBytesQuery   string
	availableInodesQuery string
	capacityInodesQuery  string
}

var _ metricssource.Source = &Prometheus{}

// Option is a function which can configure a [Prometheus] instance.
type Option func(p *Prometheus)

// WithAddress configures [Prometheus] to use the given address of the
// Prometheus instance.
func WithAddress(addr string) Option {
	opt := func(p *Prometheus) {
		p.address = addr
	}

	return opt
}

// WithHTTPClient configures [Prometheus] to use the given [http.Client].
func WithHTTPClient(client *http.Client) Option {
	opt := func(p *Prometheus) {
		p.httpClient = client
	}

	return opt
}

// WithRoundTripper configures [Prometheus] to use the given
// [http.RoundTripper].
func WithRoundTripper(rt http.RoundTripper) Option {
	opt := func(p *Prometheus) {
		p.roundTripper = rt
	}

	return opt
}

// WithAvailableBytesQuery configures [Prometheus] to use the given query for
// fetching metrics about available bytes.
func WithAvailableBytesQuery(query string) Option {
	opt := func(p *Prometheus) {
		p.availableBytesQuery = query
	}

	return opt
}

// WithCapacityBytesQuery configures [Prometheus] to use the given query for
// fetching metrics about volume capacity in bytes.
func WithCapacityBytesQuery(query string) Option {
	opt := func(p *Prometheus) {
		p.capacityBytesQuery = query
	}

	return opt
}

// WithAvailableInodesQuery configures [Prometheus] to use the given query for
// fetching metrics about available inodes.
func WithAvailableInodesQuery(query string) Option {
	opt := func(p *Prometheus) {
		p.availableInodesQuery = query
	}

	return opt
}

// WithCapacityInodesQuery configures [Prometheus] to use the given query for
// fetching metrics about the capacity of inodes for volumes.
func WithCapacityInodesQuery(query string) Option {
	opt := func(p *Prometheus) {
		p.capacityInodesQuery = query
	}

	return opt
}

// New creates a new [Prometheus] metrics source and configures it with the
// given options.
func New(opts ...Option) (*Prometheus, error) {
	p := &Prometheus{}
	for _, opt := range opts {
		opt(p)
	}

	if p.address == "" {
		return nil, ErrNoPrometheusAddress
	}

	// Configure the Prometheus API client
	cfg := api.Config{
		Address:      p.address,
		Client:       p.httpClient,
		RoundTripper: p.roundTripper,
	}

	client, err := api.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	p.api = promv1.NewAPI(client)

	// Set some sane defaults here.
	//
	// See https://kubernetes.io/docs/reference/instrumentation/metrics/ for
	// more details.
	if p.availableBytesQuery == "" {
		p.availableBytesQuery = metricssource.KubeletVolumeStatsAvailableBytes
	}
	if p.capacityBytesQuery == "" {
		p.capacityBytesQuery = metricssource.KubeletVolumeStatsCapacityBytes
	}
	if p.availableInodesQuery == "" {
		p.availableInodesQuery = metricssource.KubeletVolumeStatsInodesFree
	}
	if p.capacityInodesQuery == "" {
		p.capacityInodesQuery = metricssource.KubeletVolumeStatsInodes
	}

	return p, nil
}

// valueMapperFunc is a function which knows how to map a given metric value to
// a field in [metricssource.VolumeInfo].
type valueMapperFunc func(val int, info *metricssource.VolumeInfo)

// Get implements the [metricssource.Source] interface
func (p *Prometheus) Get(ctx context.Context) (metricssource.Metrics, error) {
	result := make(metricssource.Metrics)

	// Maps queries to mappers for setting the values to the respective
	// metricssource.VolumeInfo fields.
	queryToMapper := map[string]valueMapperFunc{
		p.availableBytesQuery: func(val int, info *metricssource.VolumeInfo) {
			info.AvailableBytes = val
		},
		p.capacityBytesQuery: func(val int, info *metricssource.VolumeInfo) {
			info.CapacityBytes = val
		},
		p.availableInodesQuery: func(val int, info *metricssource.VolumeInfo) {
			info.AvailableInodes = val
		},
		p.capacityInodesQuery: func(val int, info *metricssource.VolumeInfo) {
			info.CapacityInodes = val
		},
	}

	for query, mapper := range queryToMapper {
		if err := p.getMetric(ctx, query, result, mapper); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// getMetric retrieves the given metric specified by `query' and maps the values
// to `metrics' using a provided valueMapperFunc.
func (p *Prometheus) getMetric(ctx context.Context, query string, metrics metricssource.Metrics, mapValue valueMapperFunc) error {
	result, warnings, err := p.api.Query(ctx, query, time.Now())
	if err != nil {
		return err
	}

	// Warnings are non critical, but we still want them to be logged
	logger := log.FromContext(ctx)
	for _, warning := range warnings {
		logger.Info(warning, "query", query)
	}

	vector, ok := result.(model.Vector)
	if !ok {
		return fmt.Errorf("expected model.Vector result, got %s", result.Type())
	}

	for _, val := range vector {
		namespaceVal, ok := val.Metric["namespace"]
		if !ok {
			return fmt.Errorf("metric does not provide namespace label: %v", val)
		}
		nameVal, ok := val.Metric["persistentvolumeclaim"]
		if !ok {
			return fmt.Errorf("metric does not provide persistentvolumeclaim label: %v", val)
		}

		key := types.NamespacedName{
			Namespace: string(namespaceVal),
			Name:      string(nameVal),
		}

		volInfo, exists := metrics[key]
		if !exists {
			volInfo = &metricssource.VolumeInfo{}
			metrics[key] = volInfo
		}
		metricValue := int(val.Value)
		mapValue(metricValue, volInfo)
	}

	return nil
}
