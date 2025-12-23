// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package periodic

import (
	"context"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/metrics"
	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

// UnknownUtilizationValue is the value which will be used when the free
// space/inodes utilization is unknown.
const UnknownUtilizationValue = "unknown"

// ErrNoMetricsSource is returned when the [Runner] is configured without a
// metrics source.
var ErrNoMetricsSource = errors.New("no metrics source provided")

// ErrVolumeModeIsNotFilesystem is an error which is returned if a target PVC
// for resizing is not using the Filesystem VolumeMode.
var ErrVolumeModeIsNotFilesystem = errors.New("volume mode is not filesystem")

// ErrStorageClassNotFound is an error which is returned when the storage class
// for a PVC is not found.
var ErrStorageClassNotFound = errors.New("no storage class found")

// ErrStorageClassDoesNotSupportExpansion is an error which is returned when an
// annotated PVC uses a storage class that does not support volume expansion.
var ErrStorageClassDoesNotSupportExpansion = errors.New("storage class does not support expansion")

// ErrNoClient is an error which is returned when the periodic [Runner] was
// configured configured without a Kubernetes API client.
var ErrNoClient = errors.New("no client provided")

// Runner is a [sigs.k8s.io/controller-runtime/pkg/manager.Runnable], which
// enqueues [v1alpha1.PersistentVolumeClaimAutoscaler] items for reconciling on
// regular basis.
type Runner struct {
	client        client.Client
	interval      time.Duration
	eventCh       chan event.GenericEvent
	metricsSource metricssource.Source
	eventRecorder record.EventRecorder
}

var _ manager.Runnable = &Runner{}

// Option is a function which configures the [Runner].
type Option func(r *Runner)

// New creates a new [Runner] with the given options.
func New(opts ...Option) (*Runner, error) {
	r := &Runner{}
	for _, opt := range opts {
		opt(r)
	}

	if r.metricsSource == nil {
		return nil, ErrNoMetricsSource
	}

	if r.eventRecorder == nil {
		return nil, common.ErrNoEventRecorder
	}

	if r.eventCh == nil {
		return nil, common.ErrNoEventChannel
	}

	if r.client == nil {
		return nil, ErrNoClient
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
func WithMetricsSource(src metricssource.Source) Option {
	opt := func(r *Runner) {
		r.metricsSource = src
	}

	return opt
}

// WithEventRecorder configures the [Runner] to use the given event recorder.
func WithEventRecorder(recorder record.EventRecorder) Option {
	opt := func(r *Runner) {
		r.eventRecorder = recorder
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
				logger.Error(err, "failed to enqueue persistentvolumeclaimautoscalers")
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// enqueueObjects enqueues the [v1alpha1.PersitentVolumeClaimAutoscaler]
// resources for reconciliation.
func (r *Runner) enqueueObjects(ctx context.Context) error {
	var items v1alpha1.PersistentVolumeClaimAutoscalerList
	if err := r.client.List(ctx, &items); err != nil {
		return err
	}

	// Nothing to do for now
	if len(items.Items) == 0 {
		return nil
	}

	metricsData, err := r.metricsSource.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to get metrics: %w", err)
	}

	toReconcile := make([]v1alpha1.PersistentVolumeClaimAutoscaler, 0)
	for _, item := range items.Items {
		pvcObjKey := client.ObjectKey{Namespace: item.Namespace, Name: item.Spec.TargetRef.Name}
		volInfo := metricsData[pvcObjKey]
		logger := log.FromContext(
			ctx,
			"controller", common.ControllerName,
			"namespace", item.Namespace,
			"name", item.Name,
			"pvc", item.Spec.TargetRef.Name,
		)

		ok, err := r.shouldReconcilePVC(ctx, &item, volInfo)
		if err != nil {
			logger.Info("skipping persistentvolumeclaim", "reason", err.Error())
			metrics.SkippedTotal.WithLabelValues(item.Namespace, item.Name, err.Error()).Inc()
			condition := metav1.Condition{
				Type:    utils.ConditionTypeHealthy,
				Status:  metav1.ConditionUnknown,
				Reason:  "Reconciling",
				Message: err.Error(),
			}
			if err := item.SetCondition(ctx, r.client, condition); err != nil {
				logger.Info("failed to update status condition", "reason", err.Error())
			}

			continue
		}

		if ok {
			toReconcile = append(toReconcile, item)
		} else {
			condition := metav1.Condition{
				Type:    utils.ConditionTypeHealthy,
				Status:  metav1.ConditionTrue,
				Reason:  "Reconciling",
				Message: "Successfully reconciled",
			}
			if err := item.SetCondition(ctx, r.client, condition); err != nil {
				logger.Info("failed to update status condition", "reason", err.Error())
			}
		}
	}

	for _, item := range toReconcile {
		e := event.GenericEvent{
			Object: &item,
		}
		r.eventCh <- e
	}

	return nil
}

// updatePVCAStatus updates the status of the
// [v1alpha1.PersistentVolumeClaimAutoscaler] with the latest observed
// information about the target [corev1.PersistentVolumeClaim].
func (r *Runner) updatePVCAStatus(ctx context.Context, obj *v1alpha1.PersistentVolumeClaimAutoscaler, volInfo *metricssource.VolumeInfo) error {
	patch := client.MergeFrom(obj.DeepCopy())
	now := time.Now()
	nextCheck := now.Add(r.interval)

	freeSpaceStr := UnknownUtilizationValue
	usedSpaceStr := UnknownUtilizationValue
	freeInodesStr := UnknownUtilizationValue
	usedInodesStr := UnknownUtilizationValue

	if volInfo != nil {
		if freeSpace, err := volInfo.FreeSpacePercentage(); err == nil {
			freeSpaceStr = fmt.Sprintf("%.2f%%", freeSpace)
		}

		if usedSpace, err := volInfo.UsedSpacePercentage(); err == nil {
			usedSpaceStr = fmt.Sprintf("%.2f%%", usedSpace)
		}

		if freeInodes, err := volInfo.FreeInodesPercentage(); err == nil {
			freeInodesStr = fmt.Sprintf("%.2f%%", freeInodes)
		}

		if usedInodes, err := volInfo.UsedInodesPercentage(); err == nil {
			usedInodesStr = fmt.Sprintf("%.2f%%", usedInodes)
		}
	}

	obj.Status.LastCheck = metav1.NewTime(now)
	obj.Status.NextCheck = metav1.NewTime(nextCheck)
	obj.Status.UsedSpacePercentage = usedSpaceStr
	obj.Status.FreeSpacePercentage = freeSpaceStr
	obj.Status.UsedInodesPercentage = usedInodesStr
	obj.Status.FreeInodesPercentage = freeInodesStr

	return r.client.Status().Patch(ctx, obj, patch)
}

// shouldReconcilePVC is a predicate which checks whether the
// [corev1.PersistentVolumeClaim] object targeted by
// [v1alpha1.PersistentVolumeClaimAutoscaler] should be considered for
// reconciliation.
func (r *Runner) shouldReconcilePVC(ctx context.Context, pvca *v1alpha1.PersistentVolumeClaimAutoscaler, volInfo *metricssource.VolumeInfo) (bool, error) {
	pvcObjKey := client.ObjectKey{Namespace: pvca.Namespace, Name: pvca.Spec.TargetRef.Name}
	pvcObj := &corev1.PersistentVolumeClaim{}
	if err := r.client.Get(ctx, pvcObjKey, pvcObj); err != nil {
		return false, err
	}

	if err := r.updatePVCAStatus(ctx, pvca, volInfo); err != nil {
		return false, err
	}

	// No metrics found, nothing to do for now
	if volInfo == nil {
		return false, common.ErrNoMetrics
	}

	// Validate the spec
	if err := r.validatePVCA(pvca); err != nil {
		return false, err
	}

	// Validate the PVC itself against the spec
	currStatusSize := pvcObj.Status.Capacity.Storage()
	if currStatusSize.IsZero() {
		return false, fmt.Errorf(".status.capacity.storage is invalid: %s", currStatusSize.String())
	}

	if pvca.Spec.MaxCapacity.Value() < currStatusSize.Value() {
		return false, fmt.Errorf("max capacity (%s) cannot be less than current size (%s)", pvca.Spec.MaxCapacity.String(), currStatusSize.String())
	}

	// We need a StorageClass with expansion support
	scName := ptr.Deref(pvcObj.Spec.StorageClassName, "")
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

	// Detect whether the metrics source is reporting stale data.  Stale
	// metrics data would be when the volume info metrics reported by the
	// metrics source are deviate from the current PVC size indicated by
	// `.status.capacity.storage'
	if statusSize, ok := currStatusSize.AsInt64(); ok {
		delta := statusSize - int64(volInfo.CapacityBytes)
		if delta < 0 {
			delta = -delta
		}
		if delta > common.ScalingResolutionBytes/2 {
			return false, common.ErrStaleMetrics
		}
	}

	// Getting an error from FreeSpacePercentage() means that the
	// capacity for the volume is zero, which in turn means that we
	// didn't get any metrics for it.
	freeSpace, err := volInfo.FreeSpacePercentage()
	if err != nil {
		return false, common.ErrNoMetrics
	}

	// Even, if we don't have inode metrics we still want to proceed here.
	freeInodes, err := volInfo.FreeInodesPercentage()
	if err != nil {
		return false, common.ErrNoMetrics
	}

	threshold, err := utils.ParsePercentage(pvca.Spec.Threshold)
	if err != nil {
		return false, fmt.Errorf("cannot parse threshold: %w", err)
	}

	// VolumeMode should be Filesystem
	if pvcObj.Spec.VolumeMode == nil {
		return false, nil
	}
	if *pvcObj.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return false, ErrVolumeModeIsNotFilesystem
	}

	// The PVC should be bound
	if pvcObj.Status.Phase != corev1.ClaimBound {
		return false, nil
	}

	switch {
	// Free space reached threshold
	case freeSpace < threshold:
		r.eventRecorder.Eventf(
			pvcObj,
			corev1.EventTypeWarning,
			"FreeSpaceThresholdReached",
			"free space (%.2f%%) is less than the configured threshold (%.2f%%)",
			freeSpace,
			threshold,
		)
		metrics.ThresholdReachedTotal.WithLabelValues(pvcObj.Namespace, pvcObj.Name, "space").Inc()

		return true, nil

	// Free inodes reached threshold
	case volInfo.CapacityInodes > 0.0 && (freeInodes < threshold):
		r.eventRecorder.Eventf(
			pvcObj,
			corev1.EventTypeWarning,
			"FreeInodesThresholdReached",
			"free inodes (%.2f%%) are less than the configured threshold (%.2f%%)",
			freeInodes,
			threshold,
		)
		metrics.ThresholdReachedTotal.WithLabelValues(pvcObj.Namespace, pvcObj.Name, "inodes").Inc()

		return true, nil

	// No need to reconcile the PVC for now
	default:
		return false, nil
	}
}

// validatePVCA sanity checks the spec in order to ensure it contains valid
// values. Returns nil if the spec is valid, and non-nil error otherwise.
func (*Runner) validatePVCA(obj *v1alpha1.PersistentVolumeClaimAutoscaler) error {
	threshold, err := utils.ParsePercentage(obj.Spec.Threshold)
	if err != nil {
		return fmt.Errorf("cannot parse threshold: %w", err)
	}
	if threshold == 0.0 {
		return fmt.Errorf("invalid threshold: %w", common.ErrZeroPercentage)
	}

	if obj.Spec.MaxCapacity.IsZero() {
		return fmt.Errorf("invalid max capacity: %w", common.ErrNoMaxCapacity)
	}

	increaseBy, err := utils.ParsePercentage(obj.Spec.IncreaseBy)
	if err != nil {
		return fmt.Errorf("cannot parse increase-by value: %w", err)
	}
	if increaseBy == 0.0 {
		return fmt.Errorf("invalid increase-by: %w", common.ErrZeroPercentage)
	}

	return nil
}
