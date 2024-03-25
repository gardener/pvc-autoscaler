package periodic

import (
	"context"
	"time"

	"github.com/gardener/pvc-autoscaler/internal/index"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Runner is a [sigs.k8s.io/controller-runtime/pkg/manager.Runnable], which
// enqueues PersistentVolumeClaims for reconciling on regular basis.
type Runner struct {
	client   client.Client
	interval time.Duration
	eventCh  chan event.GenericEvent
}

// Option is a function which configures the [Runner].
type Option func(c *Runner) error

// New creates a new [Runner] with the given options.
func New(opts ...Option) (*Runner, error) {
	r := &Runner{}
	for _, opt := range opts {
		if err := opt(r); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// WithClient configures the [Runner] with the given client.
func WithClient(c client.Client) Option {
	opt := func(r *Runner) error {
		r.client = c
		return nil
	}

	return opt
}

// WithInterval configures the [Runner] with the given interval.
func WithInterval(interval time.Duration) Option {
	opt := func(r *Runner) error {
		r.interval = interval
		return nil
	}

	return opt
}

// WithEventChannel configures the [Runner] to use the given channel for
// enqueuing.
func WithEventChannel(ch chan event.GenericEvent) Option {
	opt := func(r *Runner) error {
		r.eventCh = ch
		return nil
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
