package periodic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/index"
	"github.com/gardener/pvc-autoscaler/internal/metrics/source"
	"github.com/gardener/pvc-autoscaler/internal/utils"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// ErrNoMetrics is an error which is returned when metrics about a PVC are
// missing.
var ErrNoMetrics = errors.New("no metrics found")

// ErrNoMaxCapacity is an error which is returned when a PVC does not specify
// the max capacity.
var ErrNoMaxCapacity = errors.New("no max capacity specified")

// ErrBadPercentageValue is an error which is returned when attempting to parse
// a bad percentage value.
var ErrBadPercentageValue = errors.New("bad percentage value")

// ErrVolumeModeIsNotFilesystem is an error which is returned if a target PVC
// for resizing is not using the Filesystem VolumeMode.
var ErrVolumeModeIsNotFilesystem = errors.New("volume mode is not filesystem")

// ErrStorageClassNotFound is an error which is returned when the storage class
// for a PVC is not found.
var ErrStorageClassNotFound = errors.New("no storage class found")

// ErrStorageClassDoesNotSupportExpansion is an error which is returned when an
// annotated PVC uses a storage class that does not support volume expansion.
var ErrStorageClassDoesNotSupportExpansion = errors.New("storage class does not support expansion")

// Runner is a [sigs.k8s.io/controller-runtime/pkg/manager.Runnable], which
// enqueues PersistentVolumeClaims for reconciling on regular basis.
type Runner struct {
	client        client.Client
	interval      time.Duration
	eventCh       chan event.GenericEvent
	metricsSource source.Source
}

var _ manager.Runnable = &Runner{}

// Option is a function which configures the [Runner].
type Option func(c *Runner)

// New creates a new [Runner] with the given options.
func New(opts ...Option) *Runner {
	r := &Runner{}
	for _, opt := range opts {
		opt(r)
	}

	return r
}

// WithClient configures the [Runner] with the given client.
func WithClient(c client.Client) Option {
	opt := func(r *Runner) {
		r.client = c
	}

	return opt
}

// WithInterval configures the [Runner] with the given interval.
func WithInterval(interval time.Duration) Option {
	opt := func(r *Runner) {
		r.interval = interval
	}

	return opt
}

// WithEventChannel configures the [Runner] to use the given channel for
// enqueuing.
func WithEventChannel(ch chan event.GenericEvent) Option {
	opt := func(r *Runner) {
		r.eventCh = ch
	}

	return opt
}

// WithMetricsSource configures the [Runner] to use the given source of metrics.
func WithMetricsSource(src source.Source) Option {
	opt := func(r *Runner) {
		r.metricsSource = src
	}

	return opt
}

// Start implements the
// [sigs.k8s.io/controller-runtime/pkg/manager.Runnable] interface.
func (r *Runner) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	logger := log.FromContext(ctx)
	defer ticker.Stop()
	defer close(r.eventCh)

	for {
		select {
		case <-ticker.C:
			if err := r.enqueueObjects(ctx); err != nil {
				logger.Error(err, "failed to enqueue persistentvolumeclaims")
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// enqueueObjects enqueues the PVCs which are properly annotated
func (r *Runner) enqueueObjects(ctx context.Context) error {
	var items corev1.PersistentVolumeClaimList
	opts := client.MatchingFields{index.Key: "true"}
	if err := r.client.List(ctx, &items, opts); err != nil {
		return err
	}

	for _, item := range items.Items {
		event := event.GenericEvent{
			Object: &item,
		}
		r.eventCh <- event
	}

	return nil
}
