// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package periodic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/metrics"
	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
	"github.com/gardener/pvc-autoscaler/internal/target/pvcfetcher"
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
// configured without a Kubernetes API client.
var ErrNoClient = errors.New("no client provided")

// ErrNoPVCFetcher is an error which is returned when the periodic [Runner] was
// configured without a [pvcfetcher.Fetcher].
var ErrNoPVCFetcher = errors.New("no PersistentVolumeClaim fetcher provided")

// ErrPVCNotBound is returned when the PVC is not in the Bound phase.
var ErrPVCNotBound = errors.New("PersistentVolumeClaim is not bound")

// Condition reasons for the RecommendationAvailable condition
const (
	// ReasonMetricsFetched indicates that metrics were successfully fetched and computed.
	ReasonMetricsFetched = "MetricsFetched"
	// ReasonMetricsFetchError indicates an error occurred while fetching metrics.
	ReasonMetricsFetchError = "MetricsFetchError"
	// ReasonPVCFetchError indicates an error occurred during fetching of PVCs.
	ReasonPVCFetchError = "PersistentVolumeClaimFetchError"
	// ReasonAmbiguousPVCA indicates that a PVC is autoscaled by multiple PVCAs.
	ReasonAmbiguousPVCA = "AmbiguousPersistentVolumeClaimAutoscaler"
	// ReasonRecommendationError indicates an error occurred during recommendation computation.
	ReasonRecommendationError = "RecommendationError"
	// ReasonReconcile condition reason for the Resizing condition.
	ReasonReconcile = "Reconcile"
	// ReasonPVCResizeCooldown indicates that the PVC resize is in cooldown period.
	ReasonPVCResizeCooldown = "PersistentVolumeClaimResizeCooldown"
)

// Runner is a [sigs.k8s.io/controller-runtime/pkg/manager.Runnable], which
// processes [v1alpha1.PersistentVolumeClaimAutoscaler] items on a regular basis
// and performs PVC resizing when thresholds are reached.
type Runner struct {
	client        client.Client
	interval      time.Duration
	metricsSource metricssource.Source
	eventRecorder record.EventRecorder
	pvcFetcher    pvcfetcher.Fetcher
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

	if r.client == nil {
		return nil, ErrNoClient
	}

	if r.pvcFetcher == nil {
		return nil, ErrNoPVCFetcher
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

// WithPVCFetcher configures the [Runner] to use the given [pvcfetcher.Fetcher].
func WithPVCFetcher(pvcFetcher pvcfetcher.Fetcher) Option {
	opt := func(r *Runner) {
		r.pvcFetcher = pvcFetcher
	}

	return opt
}

// Start implements the
// [sigs.k8s.io/controller-runtime/pkg/manager.Runnable] interface.
func (r *Runner) Start(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	logger := log.FromContext(ctx, "controller", common.ControllerName)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := r.reconcileAll(ctx); err != nil {
				logger.Error(err, "failed to reconcile persistentvolumeclaimautoscalers")
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// reconcileAll processes all [v1alpha1.PersistentVolumeClaimAutoscaler]
// resources and resizes PVCs when thresholds are reached.
func (r *Runner) reconcileAll(ctx context.Context) error {
	var (
		logger   = log.FromContext(ctx, "controller", common.ControllerName)
		pvcaList v1alpha1.PersistentVolumeClaimAutoscalerList
	)

	if err := r.client.List(ctx, &pvcaList); err != nil {
		return err
	}

	// Nothing to do for now
	if len(pvcaList.Items) == 0 {
		return nil
	}

	metricsData, err := r.metricsSource.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to get metrics: %w", err)
	}

	pvcaToPVCsMap, pvcToOwnersMap := r.fetchPVCsForPVCAs(ctx, logger, pvcaList.Items)

	for pvca, pvcs := range pvcaToPVCsMap {
		r.reconcilePVCA(ctx, logger, pvca, pvcs, pvcToOwnersMap, metricsData)
	}

	return nil
}

func (r *Runner) fetchPVCsForPVCAs(ctx context.Context, logger logr.Logger, persistentVolumeClaimAutoscalers []v1alpha1.PersistentVolumeClaimAutoscaler) (
	map[*v1alpha1.PersistentVolumeClaimAutoscaler][]*corev1.PersistentVolumeClaim,
	map[string][]string,
) {
	var (
		pvcaToPVCsMap  = make(map[*v1alpha1.PersistentVolumeClaimAutoscaler][]*corev1.PersistentVolumeClaim, len(persistentVolumeClaimAutoscalers))
		pvcToOwnersMap = map[string][]string{}
	)

	for _, pvca := range persistentVolumeClaimAutoscalers {
		persistentVolumeClaims, err := r.pvcFetcher.Fetch(ctx, &pvca)
		if err != nil {
			logger.Error(err, "failed to fetch persistentvolumeclaims for persistentvolumeclaimautoscaler", "pvca", client.ObjectKeyFromObject(&pvca))
			condition := metav1.Condition{
				Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
				Status:  metav1.ConditionFalse,
				Reason:  ReasonPVCFetchError,
				Message: fmt.Sprintf("failed to fetch PersistentVolumeClaims for PersistentVolumeClaimAutoscaler: %s", err.Error()),
			}
			if err := pvca.SetCondition(ctx, r.client, condition); err != nil {
				logger.Info("failed to update status condition", "pvca", client.ObjectKeyFromObject(&pvca), "reason", err.Error())
			}

			continue
		}

		pvcaToPVCsMap[&pvca] = persistentVolumeClaims

		for _, pvc := range persistentVolumeClaims {
			key := client.ObjectKeyFromObject(pvc).String()
			pvcToOwnersMap[key] = append(pvcToOwnersMap[key], client.ObjectKeyFromObject(&pvca).String())
		}
	}

	return pvcaToPVCsMap, pvcToOwnersMap
}

func (r *Runner) reconcilePVCA(
	ctx context.Context,
	logger logr.Logger,
	pvca *v1alpha1.PersistentVolumeClaimAutoscaler,
	pvcs []*corev1.PersistentVolumeClaim,
	pvcToOwnersMap map[string][]string,
	metricsData metricssource.Metrics,
) {
	logger = logger.WithValues("pvca", client.ObjectKeyFromObject(pvca))

	// TODO(plkokanov): When multi-PVC support is added, this must be updated to iterate over all PVCs.
	// Currently, the PVCA can only target one PVC, so we only get the first element.
	// Because of that, we can also skip reconciling the PVCA resource if
	// other PVCAs also target the same PVC.
	pvc := pvcs[0]
	pvcObjKey := client.ObjectKeyFromObject(pvc)
	logger = logger.WithValues("pvc", pvcObjKey)

	if owners, ok := pvcToOwnersMap[pvcObjKey.String()]; ok && len(owners) > 1 {
		logger.Info("skipping persistentvolumeclaim because it is scaled by multiple persistentvolumeclaimautoscalers", "pvcas", strings.Join(owners, ", "))
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
			Status:  metav1.ConditionFalse,
			Reason:  ReasonAmbiguousPVCA,
			Message: fmt.Sprintf("PersistentVolumeClaim %s is scaled by multiple PersistentVolumeClaimAutoscalers: %s", pvcObjKey, strings.Join(owners, ", ")),
		}
		if err := pvca.SetCondition(ctx, r.client, condition); err != nil {
			logger.Info("failed to update status condition", "reason", err.Error())
		}

		return
	}

	volInfo := metricsData[pvcObjKey]
	// Get a fresh copy of the pvc object.
	if err := r.client.Get(ctx, pvcObjKey, pvc); err != nil {
		logger.Info("failed to get persistentvolumeclaim", "reason", err.Error())
		return
	}

	policy := volumePolicyForPVC(pvca, pvc)

	if err := r.validatePVC(ctx, pvc, policy); err != nil {
		logger.Info("skipping persistentvolumeclaim", "reason", err.Error())
		metrics.SkippedTotal.WithLabelValues(pvca.Namespace, pvca.Name, err.Error()).Inc()
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
			Status:  metav1.ConditionFalse,
			Reason:  ReasonRecommendationError,
			Message: fmt.Sprintf(" - %s: %s", pvcObjKey.Name, err.Error()),
		}
		if err := pvca.SetCondition(ctx, r.client, condition); err != nil {
			logger.Info("failed to update status condition", "reason", err.Error())
		}

		return
	}

	ok, scalingReason, err := r.shouldReconcilePVC(ctx, pvca, pvc, policy, volInfo)
	if err != nil {
		logger.Info("skipping persistentvolumeclaim", "reason", err.Error())
		metrics.SkippedTotal.WithLabelValues(pvca.Namespace, pvca.Name, err.Error()).Inc()
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
			Status:  metav1.ConditionFalse,
			Reason:  ReasonMetricsFetchError,
			Message: fmt.Sprintf(" - %s: %s", pvcObjKey.Name, err.Error()),
		}
		if err := pvca.SetCondition(ctx, r.client, condition); err != nil {
			logger.Info("failed to update status condition", "reason", err.Error())
		}

		return
	}

	inProgress, err := r.isResizeInProgress(ctx, pvca, pvc, scalingReason)
	if err != nil {
		logger.Info("failed to determine whether persistentvolumeclaim is being resized", "reason", err.Error())
		return
	}

	if ok && !inProgress {
		if err := r.resizePVC(ctx, pvca, pvc, policy, scalingReason); err != nil {
			logger.Error(err, "failed to resize pvc")
		}
	} else if err := pvca.RemoveCondition(ctx, r.client, string(v1alpha1.ConditionTypeResizing)); err != nil {
		logger.Info("failed to remove status condition", "reason", err.Error())
	}

	condition := metav1.Condition{
		Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
		Status:  metav1.ConditionTrue,
		Reason:  ReasonMetricsFetched,
		Message: " - All metrics fetched successfully",
	}
	if err := pvca.SetCondition(ctx, r.client, condition); err != nil {
		logger.Info("failed to update status condition", "reason", err.Error())
	}
}

// updatePVCAStatus updates the status of the
// [v1alpha1.PersistentVolumeClaimAutoscaler] with the latest observed
// information about the target [corev1.PersistentVolumeClaim].
func (r *Runner) updatePVCAStatus(ctx context.Context, obj *v1alpha1.PersistentVolumeClaimAutoscaler, volInfo *metricssource.VolumeInfo) error {
	patch := client.MergeFrom(obj.DeepCopy())
	now := time.Now()
	obj.Status.LastCheck = metav1.NewTime(now)
	obj.Status.NextCheck = metav1.NewTime(now.Add(r.interval))

	// Start with existing recommendation to preserve fields set during resize,
	// or create a new one if none exists
	var volumeRecommendation v1alpha1.VolumeRecommendation
	if len(obj.Status.VolumeRecommendations) > 0 {
		volumeRecommendation = obj.Status.VolumeRecommendations[0]
	}
	volumeRecommendation.Name = obj.Spec.TargetRef.Name

	if volInfo != nil {
		usedSpace, err := volInfo.UsedSpacePercentage()
		if err != nil {
			return fmt.Errorf("failed to get used space percentage: %w", err)
		}
		volumeRecommendation.Current.UsedSpacePercent = &usedSpace

		usedInodes, err := volInfo.UsedInodesPercentage()
		if err != nil {
			return fmt.Errorf("failed to get used inodes percentage: %w", err)
		}
		volumeRecommendation.Current.UsedInodesPercent = &usedInodes
	}

	obj.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{volumeRecommendation}

	return r.client.Status().Patch(ctx, obj, patch)
}

// volumePolicyForPVC returns the volume policy from the given
// [v1alpha1.PersistentVolumeClaimAutoscaler] that applies to the specified PVC.
// Currently only one volume policy is supported, so the first policy is always
// returned. When multi-PVC support is added, this function will match the
// policy to the specific PVC.
func volumePolicyForPVC(pvca *v1alpha1.PersistentVolumeClaimAutoscaler, _ *corev1.PersistentVolumeClaim) v1alpha1.VolumePolicy {
	return pvca.Spec.VolumePolicies[0]
}

// validatePVC checks whether the [corev1.PersistentVolumeClaim] is valid for
// reconciliation based on its current state and the associated volume policy.
func (r *Runner) validatePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim, policy v1alpha1.VolumePolicy) error {
	currStatusSize := pvc.Status.Capacity.Storage()
	if currStatusSize.IsZero() {
		return fmt.Errorf(".status.capacity.storage is invalid: %s", currStatusSize.String())
	}

	if policy.MaxCapacity.Value() < currStatusSize.Value() {
		return fmt.Errorf("max capacity (%s) cannot be less than current size (%s)", policy.MaxCapacity.String(), currStatusSize.String())
	}

	// We need a StorageClass with expansion support
	scName := ptr.Deref(pvc.Spec.StorageClassName, "")
	if scName == "" {
		return ErrStorageClassNotFound
	}

	var sc storagev1.StorageClass
	scKey := types.NamespacedName{Name: scName}
	if err := r.client.Get(ctx, scKey, &sc); err != nil {
		return err
	}

	if !ptr.Deref(sc.AllowVolumeExpansion, false) {
		return ErrStorageClassDoesNotSupportExpansion
	}

	// VolumeMode should be Filesystem
	if pvc.Spec.VolumeMode != nil && *pvc.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return ErrVolumeModeIsNotFilesystem
	}

	// The PVC should be bound
	if pvc.Status.Phase != corev1.ClaimBound {
		return ErrPVCNotBound
	}

	return nil
}

// shouldReconcilePVC is a predicate which checks whether the
// [corev1.PersistentVolumeClaim] object targeted by
// [v1alpha1.PersistentVolumeClaimAutoscaler] should be considered for
// reconciliation. When it returns true, it also returns the scaling reason.
func (r *Runner) shouldReconcilePVC(ctx context.Context, pvca *v1alpha1.PersistentVolumeClaimAutoscaler, pvc *corev1.PersistentVolumeClaim, policy v1alpha1.VolumePolicy, volInfo *metricssource.VolumeInfo) (bool, string, error) {
	if err := r.updatePVCAStatus(ctx, pvca, volInfo); err != nil {
		return false, "", err
	}

	// No metrics found, nothing to do for now
	if volInfo == nil {
		return false, "", common.ErrNoMetrics
	}

	currStatusSize := pvc.Status.Capacity.Storage()

	// Detect whether the metrics source is reporting stale data. Stale
	// metrics data would be when the volume info metrics reported by the
	// metrics source deviate from the current PVC size indicated by
	// `.status.capacity.storage'
	if statusSize, ok := currStatusSize.AsInt64(); ok {
		delta := math.Abs(float64(statusSize) - float64(volInfo.CapacityBytes))
		tolerance := math.Max(common.MaxCapacityDeviationRatio*float64(statusSize), float64(common.ScalingResolutionBytes)/2)
		if delta > tolerance {
			return false, "", fmt.Errorf("stale metrics data detected: pvc size=%d bytes, metrics size=%d bytes: %w", statusSize, volInfo.CapacityBytes, common.ErrStaleMetrics)
		}
	}

	// Getting an error from UsedSpacePercentage() means that the
	// capacity for the volume is zero, which in turn means that we
	// didn't get any metrics for it.
	usedSpace, err := volInfo.UsedSpacePercentage()
	if err != nil {
		return false, "", common.ErrNoMetrics
	}

	// Even, if we don't have inode metrics we still want to proceed here.
	usedInodes, err := volInfo.UsedInodesPercentage()
	if err != nil {
		return false, "", common.ErrNoMetrics
	}

	// Get threshold from volume policy
	// Currently only one policy is supported and is enforced by the CRD schema
	threshold := *policy.ScaleUp.UtilizationThresholdPercent

	switch {
	// Used space reached threshold
	case usedSpace > threshold:
		r.eventRecorder.Eventf(
			pvc,
			corev1.EventTypeWarning,
			"UsedSpaceThresholdReached",
			"used space (%d%%) exceeds the configured threshold (%d%%)",
			usedSpace,
			threshold,
		)
		metrics.ThresholdReachedTotal.WithLabelValues(pvc.Namespace, pvc.Name, "space").Inc()

		return true, "passing storage threshold", nil

	// Used inodes reached threshold
	case volInfo.CapacityInodes > 0.0 && (usedInodes > threshold):
		r.eventRecorder.Eventf(
			pvc,
			corev1.EventTypeWarning,
			"UsedInodesThresholdReached",
			"used inodes (%d%%) exceeds the configured threshold (%d%%)",
			usedInodes,
			threshold,
		)
		metrics.ThresholdReachedTotal.WithLabelValues(pvc.Namespace, pvc.Name, "inodes").Inc()

		return true, "passing inodes threshold", nil

	// No need to reconcile the PVC for now
	default:
		return false, "", nil
	}
}

// isResizeInProgress checks whether the PVC is currently being resized.
// Returns true if a resize operation is in progress.
func (r *Runner) isResizeInProgress(ctx context.Context, pvca *v1alpha1.PersistentVolumeClaimAutoscaler, pvc *corev1.PersistentVolumeClaim, scalingReason string) (bool, error) {
	logger := log.FromContext(ctx).WithValues("pvc", pvc.Name)
	currStatusSize := pvc.Status.Capacity.Storage()

	if utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimResizing) {
		logger.Info("resize has been started")
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeResizing),
			Status:  metav1.ConditionTrue,
			Reason:  ReasonReconcile,
			Message: fmt.Sprintf(" - %s: is being scaled due to %s, resize has been started", pvc.Name, scalingReason),
		}

		return true, pvca.SetCondition(ctx, r.client, condition)
	}

	if utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimFileSystemResizePending) {
		logger.Info("filesystem resize is pending")
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeResizing),
			Status:  metav1.ConditionTrue,
			Reason:  ReasonReconcile,
			Message: fmt.Sprintf(" - %s: is being scaled due to %s, file system resize is pending", pvc.Name, scalingReason),
		}

		return true, pvca.SetCondition(ctx, r.client, condition)
	}

	if utils.IsPersistentVolumeClaimConditionTrue(pvc, corev1.PersistentVolumeClaimVolumeModifyingVolume) {
		logger.Info("volume is being modified")
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeResizing),
			Status:  metav1.ConditionTrue,
			Reason:  ReasonReconcile,
			Message: fmt.Sprintf(" - %s: is being scaled due to %s, volume is being modified", pvc.Name, scalingReason),
		}

		return true, pvca.SetCondition(ctx, r.client, condition)
	}

	// If previously recorded size is equal to the current status it means
	// we are still waiting for the resize to complete
	if pvca.Status.VolumeRecommendations[0].Current.Size != nil &&
		pvca.Status.VolumeRecommendations[0].Current.Size.Equal(*currStatusSize) {
		logger.Info("persistent volume claim is still being resized")
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeResizing),
			Status:  metav1.ConditionTrue,
			Reason:  ReasonReconcile,
			Message: fmt.Sprintf(" - %s: is being scaled due to %s, persistent volume claim is still being resized", pvc.Name, scalingReason),
		}

		return true, pvca.SetCondition(ctx, r.client, condition)
	}

	return false, nil
}

// resizePVC performs the actual resize of the PVC targeted by the given
// [v1alpha1.PersistentVolumeClaimAutoscaler].
func (r *Runner) resizePVC(ctx context.Context, pvca *v1alpha1.PersistentVolumeClaimAutoscaler, pvc *corev1.PersistentVolumeClaim, policy v1alpha1.VolumePolicy, scalingReason string) error {
	logger := log.FromContext(ctx).WithValues("pvc", pvc.Name)
	currSpecSize := pvc.Spec.Resources.Requests.Storage()

	// Calculate the new size
	stepPercent := float64(*policy.ScaleUp.StepPercent)
	increment := math.Max(float64(currSpecSize.Value())*(stepPercent/100.0), float64(policy.ScaleUp.MinStepAbsolute.Value()))
	targetSizeBytes := int64(math.Ceil((float64(currSpecSize.Value())+increment)/1073741824)) * 1073741824
	targetSize := resource.NewQuantity(targetSizeBytes, resource.BinarySI)

	// Check that we've got a valid new size
	cmp := targetSize.Cmp(*currSpecSize)
	switch cmp {
	case 0:
		logger.Info("new and current size are the same")

		return nil
	case -1:
		logger.Info("new size is less than current")

		return nil
	}

	// We don't want to exceed the max capacity
	if targetSize.Value() > policy.MaxCapacity.Value() {
		r.eventRecorder.Eventf(
			pvc,
			corev1.EventTypeWarning,
			"MaxCapacityReached",
			"max capacity (%s) has been reached, will not resize",
			policy.MaxCapacity.String(),
		)
		logger.Info("max capacity reached")
		metrics.MaxCapacityReachedTotal.WithLabelValues(pvc.Namespace, pvc.Name).Inc()
		condition := metav1.Condition{
			Type:    string(v1alpha1.ConditionTypeResizing),
			Status:  metav1.ConditionFalse,
			Reason:  ReasonReconcile,
			Message: fmt.Sprintf(" - %s: max capacity reached", pvc.Name),
		}

		return pvca.SetCondition(ctx, r.client, condition)
	}

	if policy.ScaleUp.CooldownDuration != nil {
		lastResizeTime := pvca.Status.VolumeRecommendations[0].LastResizeTime
		if lastResizeTime != nil {
			elapsed := time.Since(lastResizeTime.Time)
			cooldown := policy.ScaleUp.CooldownDuration.Duration
			if elapsed < cooldown {
				remaining := cooldown - elapsed
				logger.Info("cooldown period not elapsed", "remaining", remaining.String())
				condition := metav1.Condition{
					Type:    string(v1alpha1.ConditionTypeResizing),
					Status:  metav1.ConditionFalse,
					Reason:  ReasonPVCResizeCooldown,
					Message: fmt.Sprintf("- %s: cooldown duration has not elapsed yet", pvc.Name),
				}

				return pvca.SetCondition(ctx, r.client, condition)
			}
		}
	}

	// And finally we should be good to resize now
	logger.Info("resizing persistent volume claim", "from", currSpecSize.String(), "to", targetSize.String())
	metrics.ResizedTotal.WithLabelValues(pvc.Namespace, pvc.Name).Inc()
	r.eventRecorder.Eventf(
		pvc,
		corev1.EventTypeNormal,
		"ResizingStorage",
		"resizing storage from %s to %s",
		currSpecSize.String(),
		targetSize.String(),
	)

	// Update PVC and PVCA resources
	pvcPatch := client.MergeFrom(pvc.DeepCopy())
	pvc.Spec.Resources.Requests[corev1.ResourceStorage] = *targetSize
	if err := r.client.Patch(ctx, pvc, pvcPatch); err != nil {
		return err
	}

	pvcaPatch := client.MergeFrom(pvca.DeepCopy())
	pvca.Status.VolumeRecommendations[0].Current.Size = pvc.Status.Capacity.Storage()
	pvca.Status.VolumeRecommendations[0].Target.Size = targetSize
	pvca.Status.VolumeRecommendations[0].LastResizeTime = ptr.To(metav1.Now())
	if err := r.client.Status().Patch(ctx, pvca, pvcaPatch); err != nil {
		return err
	}

	condition := metav1.Condition{
		Type:    string(v1alpha1.ConditionTypeResizing),
		Status:  metav1.ConditionTrue,
		Reason:  ReasonReconcile,
		Message: fmt.Sprintf("- %s: resizing from %s to %s due to %s", pvc.Name, currSpecSize.String(), targetSize.String(), scalingReason),
	}

	return pvca.SetCondition(ctx, r.client, condition)
}
