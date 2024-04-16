// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package fake

import (
	"context"
	"sync"
	"time"

	"github.com/gardener/pvc-autoscaler/internal/common"
	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"

	"k8s.io/apimachinery/pkg/types"
)

// AlwaysFailing is a [metricssource.Source] implementation which always fails to get metrics.
type AlwaysFailing struct{}

// Get implements the [metricssource.Source] interface
func (s *AlwaysFailing) Get(ctx context.Context) (metricssource.Metrics, error) {
	return nil, common.ErrNoMetrics
}

// Item represents a fake item from the [Fake] registry.
type Item struct {
	// NamespacedName identifying a test object (PVC)
	NamespacedName types.NamespacedName

	// CapacityBytes represents the total number of bytes for the fake item.
	CapacityBytes int

	// AvailableBytes represents the available free bytes for the fake item.
	AvailableBytes int

	// CapacityInodes represents the total number of inodes for the fake
	// item.
	CapacityInodes int

	// AvailableInodes represents the free number of inodes for the fake
	// item.
	AvailableInodes int

	// ConsumeBytesIncrement represents an increment of bytes to be "consumed"
	ConsumeBytesIncrement int

	// ConsumeInodesIncrement represents an increment of inodes to be "consumed"
	ConsumeInodesIncrement int
}

// Consume will "consume" the space and inodes of the fake item.
func (i *Item) Consume() {
	i.AvailableBytes -= i.ConsumeBytesIncrement
	if i.AvailableBytes < 0 {
		i.AvailableBytes = 0
	}

	i.AvailableInodes -= i.ConsumeInodesIncrement
	if i.AvailableInodes < 0 {
		i.AvailableInodes = 0
	}
}

// Fake implements the [metricssource.Source] interface by providing a fake
// source of metrics, which can be used in unit tests.
type Fake struct {
	sync.Mutex

	// The "registry" of fake items
	items map[types.NamespacedName]*Item

	// interval specifies a periodic interval at which available bytes and
	// inodes will be "consumed".
	interval time.Duration
}

var _ metricssource.Source = &Fake{}

// Option is a function which configures the fake metrics source.
type Option func(f *Fake)

// New creates a new fake metrics source
func New(opts ...Option) *Fake {
	f := &Fake{
		items: make(map[types.NamespacedName]*Item),
	}

	for _, opt := range opts {
		opt(f)
	}

	return f
}

// WithInterval configures the [Fake] metrics source to "consume" space and
// inodes on every interval.
func WithInterval(i time.Duration) Option {
	opt := func(f *Fake) {
		f.interval = i
	}

	return opt
}

// Register registers the given items with the [Fake] metrics source.
func (f *Fake) Register(items ...*Item) {
	f.Lock()
	defer f.Unlock()

	for _, item := range items {
		f.items[item.NamespacedName] = item
	}
}

// Start starts the fake source of metrics and blocks until the context is
// cancelled.
func (f *Fake) Start(ctx context.Context) {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.consumeItems()
		}
	}
}

// consumeItems will "consume" space and inodes from the registered items
func (f *Fake) consumeItems() {
	f.Lock()
	defer f.Unlock()
	for _, v := range f.items {
		v.Consume()
	}
}

// Get implements the [metricssource.Source] interface
func (f *Fake) Get(ctx context.Context) (metricssource.Metrics, error) {
	f.Lock()
	defer f.Unlock()

	result := make(metricssource.Metrics)
	for _, item := range f.items {
		volInfo := &metricssource.VolumeInfo{
			AvailableBytes:  item.AvailableBytes,
			CapacityBytes:   item.CapacityBytes,
			AvailableInodes: item.AvailableInodes,
			CapacityInodes:  item.CapacityInodes,
		}
		result[item.NamespacedName] = volInfo
	}

	return result, nil
}
