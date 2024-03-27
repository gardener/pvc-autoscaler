package periodic

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/common"
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

// ErrNoMetricsSource is returned when the [Runner] is configured without a
// metrics source.
var ErrNoMetricsSource = errors.New("no metrics source provided")

// ErrNoMaxCapacity is an error which is returned when a PVC does not specify
// the max capacity.
var ErrNoMaxCapacity = errors.New("no max capacity specified")

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
func New(opts ...Option) (*Runner, error) {
	r := &Runner{}
	for _, opt := range opts {
		opt(r)
	}

	if r.metricsSource == nil {
		return nil, ErrNoMetricsSource
	}

	return r, nil
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
	logger := log.FromContext(ctx, "controller", common.ControllerName)
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

	metrics, err := r.metricsSource.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to get metrics: %w", err)
	}

	toReconcile := make([]corev1.PersistentVolumeClaim, 0)
	for _, item := range items.Items {
		volInfo := metrics[client.ObjectKeyFromObject(&item)]
		logger := log.FromContext(ctx, "controller", common.ControllerName, "namespace", item.Namespace, "name", item.Name)

		ok, err := r.shouldReconcilePVC(ctx, &item, volInfo)
		if err != nil {
			logger.Info("skipping persistentvolumeclaim", "reason", err.Error())
			continue
		}

		if ok {
			toReconcile = append(toReconcile, item)
		}
	}

	for _, item := range toReconcile {
		event := event.GenericEvent{
			Object: &item,
		}
		r.eventCh <- event
	}

	return nil
}

// stampPVC stamps the given persistent volume claim by updating the list of the
// managed annotations.
func (r *Runner) stampPVC(ctx context.Context, obj *corev1.PersistentVolumeClaim, volInfo *source.VolumeInfo) error {
	patch := client.MergeFrom(obj.DeepCopy())
	now := time.Now()
	nextCheck := now.Add(r.interval)

	freeSpaceStr := "unknown"
	usedSpaceStr := "unknown"

	if volInfo != nil {
		if freeSpace, err := volInfo.FreeSpacePercentage(); err == nil {
			freeSpaceStr = fmt.Sprintf("%.2f%%", freeSpace)
		}

		if usedSpace, err := volInfo.UsedSpacePercentage(); err == nil {
			usedSpaceStr = fmt.Sprintf("%.2f%%", usedSpace)
		}
	}

	obj.Annotations[annotation.LastCheck] = strconv.FormatInt(now.Unix(), 10)
	obj.Annotations[annotation.NextCheck] = strconv.FormatInt(nextCheck.Unix(), 10)
	obj.Annotations[annotation.UsedSpacePercentage] = usedSpaceStr
	obj.Annotations[annotation.FreeSpacePercentage] = freeSpaceStr

	return r.client.Patch(ctx, obj, patch)
}

// shouldReconcilePVC is a predicate which checks whether the given
// PersistentVolumeClaim object should be considered for reconciliation.
func (r *Runner) shouldReconcilePVC(ctx context.Context, obj *corev1.PersistentVolumeClaim, volInfo *source.VolumeInfo) (bool, error) {
	if err := r.stampPVC(ctx, obj, volInfo); err != nil {
		return false, err
	}

	// No metrics found, nothing to do for now
	if volInfo == nil {
		return false, ErrNoMetrics
	}

	// We need a StorageClass with expansion support
	scName := ptr.Deref(obj.Spec.StorageClassName, "")
	if scName == "" {
		return false, ErrStorageClassNotFound
	}

	var sc storagev1.StorageClass
	scKey := types.NamespacedName{Name: scName}
	if err := r.client.Get(ctx, scKey, &sc); err != nil {
		return false, err
	}

	if !ptr.Deref(sc.AllowVolumeExpansion, false) {
		return false, ErrStorageClassDoesNotSupportExpansion
	}

	// TODO(dnaeon): Add support for inodes as well
	freeSpace, err := volInfo.FreeSpacePercentage()
	if err != nil {
		// Getting an error from FreeSpacePercentage() means that the
		// capacity for the volume is zero, which in turn means that we
		// didn't get any metrics for it.
		return false, ErrNoMetrics
	}

	thresholdVal := utils.GetAnnotation(obj, annotation.Threshold, common.DefaultThresholdValue)
	threshold, err := utils.ParsePercentage(thresholdVal)
	if err != nil {
		return false, fmt.Errorf("cannot parse threshold: %w", err)
	}

	// Having a max capacity is required.
	maxCapacityVal := utils.GetAnnotation(obj, annotation.MaxCapacity, "")
	if maxCapacityVal == "" {
		return false, ErrNoMaxCapacity
	}
	maxCapacity, err := resource.ParseQuantity(maxCapacityVal)
	if err != nil {
		return false, err
	}

	if maxCapacity.IsZero() {
		return false, ErrNoMaxCapacity
	}

	// VolumeMode should be Filesystem
	if obj.Spec.VolumeMode == nil {
		return false, nil
	}
	if *obj.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return false, ErrVolumeModeIsNotFilesystem
	}

	// The PVC should be bound
	if obj.Status.Phase != corev1.ClaimBound {
		return false, nil
	}

	return freeSpace <= threshold, nil
}
