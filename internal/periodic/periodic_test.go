// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package periodic

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
	"github.com/gardener/pvc-autoscaler/internal/metrics/source/fake"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"
)

// creates a new test periodic runner
func newRunner() (*Runner, error) {
	metricsSource := fake.New(
		fake.WithInterval(time.Second),
	)

	runner, err := New(
		WithClient(k8sClient),
		WithEventChannel(eventCh),
		WithEventRecorder(eventRecorder),
		WithInterval(time.Second),
		WithMetricsSource(metricsSource),
	)

	return runner, err
}

var _ = Describe("Periodic Runner", func() {
	Context("Create Runner instance", func() {
		It("should fail without any options", func() {
			runner, err := New()
			Expect(err).To(HaveOccurred())
			Expect(runner).To(BeNil())
		})

		It("should fail without metrics source", func() {
			runner, err := New(
				WithClient(k8sClient),
				WithEventChannel(eventCh),
				WithEventRecorder(eventRecorder),
				WithInterval(time.Second),
				WithMetricsSource(nil), // should not be nil
			)
			Expect(err).To(MatchError(ErrNoMetricsSource))
			Expect(runner).To(BeNil())
		})

		It("should fail without event channel", func() {
			runner, err := New(
				WithClient(k8sClient),
				WithEventChannel(nil), // should not be nil
				WithEventRecorder(eventRecorder),
				WithInterval(time.Second),
				WithMetricsSource(fake.New()),
			)
			Expect(err).To(MatchError(common.ErrNoEventChannel))
			Expect(runner).To(BeNil())
		})

		It("should fail without client", func() {
			runner, err := New(
				WithClient(nil), // should not be nil
				WithEventChannel(eventCh),
				WithEventRecorder(eventRecorder),
				WithInterval(time.Second),
				WithMetricsSource(fake.New()),
			)
			Expect(err).To(MatchError(ErrNoClient))
			Expect(runner).To(BeNil())
		})

		It("should fail without event recorder", func() {
			runner, err := New(
				WithClient(k8sClient),
				WithEventChannel(eventCh),
				WithEventRecorder(nil), // should not be nil
				WithInterval(time.Second),
				WithMetricsSource(fake.New()),
			)
			Expect(err).To(MatchError(common.ErrNoEventRecorder))
			Expect(runner).To(BeNil())
		})

		It("should create instance successfully", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
		})
	})

	Context("update PersistentVolumeClaimAutoscaler resource", func() {
		It("should update the pvca with unknown values", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-1",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			obj, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-1",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj).NotTo(BeNil())

			// No volume info provided, we should see default "unknown" values
			Expect(runner.updatePVCAStatus(parentCtx, obj, nil)).To(Succeed())
			Expect(obj.Status.LastCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.NextCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.UsedSpacePercentage).To(Equal(UnknownUtilizationValue))
			Expect(obj.Status.FreeSpacePercentage).To(Equal(UnknownUtilizationValue))
			Expect(obj.Status.UsedInodesPercentage).To(Equal(UnknownUtilizationValue))
			Expect(obj.Status.FreeInodesPercentage).To(Equal(UnknownUtilizationValue))
		})

		It("should update the pvca with valid percentage values", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-2",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			obj, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-2",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj).NotTo(BeNil())

			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1234,
				CapacityBytes:   123450,
				CapacityInodes:  2000,
				AvailableInodes: 12345,
			}
			freeSpace, _ := volInfo.FreeSpacePercentage()
			usedSpace, _ := volInfo.UsedSpacePercentage()
			freeInodes, _ := volInfo.FreeInodesPercentage()
			usedInodes, _ := volInfo.UsedInodesPercentage()

			// We should see the computed free and used space percentages
			Expect(runner.updatePVCAStatus(parentCtx, obj, volInfo)).To(Succeed())
			Expect(obj.Status.LastCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.NextCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.UsedSpacePercentage).To(Equal(fmt.Sprintf("%.2f%%", usedSpace)))
			Expect(obj.Status.FreeSpacePercentage).To(Equal(fmt.Sprintf("%.2f%%", freeSpace)))
			Expect(obj.Status.UsedInodesPercentage).To(Equal(fmt.Sprintf("%.2f%%", usedInodes)))
			Expect(obj.Status.FreeInodesPercentage).To(Equal(fmt.Sprintf("%.2f%%", freeInodes)))
		})
	})

	Context("shouldReconcilePVC predicate", func() {
		It("should return error - PVC is not found", func() {
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-missing-pvc",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			obj, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-missing-pvc",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(obj).NotTo(BeNil())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// No metrics at all
			ok, err := runner.shouldReconcilePVC(parentCtx, obj, nil)
			Expect(ok).To(BeFalse())
			Expect(err).To(HaveOccurred())
		})

		It("should return common.ErrNoMetrics", func() {
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-without-volinfo", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-without-volinfo",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-without-volinfo",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// No metrics at all
			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, nil)
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(common.ErrNoMetrics))
		})

		It("should return ErrStorageClassNotFound", func() {
			// This PVC does not define a storageclass
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-without-storageclass",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					// StorageClassName is not specified here
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}
			Expect(k8sClient.Create(parentCtx, pvc)).To(Succeed())

			// Update status of the pvc
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-without-storageclass",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-without-storageclass",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, &metricssource.VolumeInfo{})
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrStorageClassNotFound))
		})

		It("should return ErrStorageClassDoesNotSupportExpansion", func() {
			// This storage class does not support volume expansion
			scName := "storageclass-without-expasion"
			sc := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: scName,
				},
				Provisioner:          "no-provisioner",
				VolumeBindingMode:    ptr.To(storagev1.VolumeBindingImmediate),
				AllowVolumeExpansion: ptr.To(false),
				ReclaimPolicy:        ptr.To(corev1.PersistentVolumeReclaimDelete),
			}
			Expect(k8sClient.Create(parentCtx, sc)).To(Succeed())

			// Create a test PVC using the storageclass we've created above
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-sc-no-expansion",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: ptr.To(scName),
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}
			Expect(k8sClient.Create(parentCtx, pvc)).To(Succeed())

			// Update status of the pvc
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-sc-no-expansion",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-sc-no-expansion",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, &metricssource.VolumeInfo{})
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrStorageClassDoesNotSupportExpansion))
		})

		It("should return ErrVolumeModeIsNotFilesystem", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-block-mode",
					Namespace: "default",
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: ptr.To(testutils.StorageClassName),
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
					VolumeMode: ptr.To(corev1.PersistentVolumeBlock),
				},
			}
			Expect(k8sClient.Create(parentCtx, pvc)).To(Succeed())

			// Update status of the pvc to make it a bit more "real"
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-block-mode",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-block-mode",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Sample volume info metrics
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1000,
				CapacityBytes:   1024 * 1024 * 1024,
				AvailableInodes: 1000,
				CapacityInodes:  1000,
			}

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrVolumeModeIsNotFilesystem))
		})

		It("should not reconcile - lost pvc claim", func() {
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-lost-claim", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimLost,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-lost-claim",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-lost-claim",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Sample volume info metrics
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1000,
				CapacityBytes:   1024 * 1024 * 1024,
				AvailableInodes: 1000,
				CapacityInodes:  1000,
			}

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
			Expect(ok).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())
		})

		It("should reconcile - free space threshold reached", func() {
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-free-space-threshold-reached", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-free-space-threshold-reached",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-free-space-threshold-reached",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Sample volume info metrics with free space less < 10%
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  90 * 1024 * 1024,
				CapacityBytes:   1000 * 1024 * 1024,
				AvailableInodes: 1000,
				CapacityInodes:  1000,
			}

			// Use a new event recorder so that we capture only the
			// relevant events
			eventRecorder := record.NewFakeRecorder(128)
			withEventRecorderOpt := WithEventRecorder(eventRecorder)
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			withEventRecorderOpt(runner)

			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
			Expect(ok).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())

			event := <-eventRecorder.Events
			wantEvent := `Warning FreeSpaceThresholdReached free space (9.00%) is less than the configured threshold (10.00%)`
			Expect(event).To(Equal(wantEvent))
		})

		It("should return ErrStaleMetrics", func() {
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-stale-metrics", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-stale-metrics",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-stale-metrics",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Sample volume info metrics with free space less < 10%
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  9 * 1024 * 1024,
				CapacityBytes:   200 * 1024 * 1024,
				AvailableInodes: 1000,
				CapacityInodes:  1000,
			}

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(common.ErrStaleMetrics))
		})

		It("should reconcile - free inodes threshold reached", func() {
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-free-inodes-threshold-reached", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-free-inodes-threshold-reached",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-free-inodes-threshold-reached",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Sample volume info metrics with free inodes less < 10%
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1024 * 1024 * 1024,
				CapacityBytes:   1024 * 1024 * 1024,
				AvailableInodes: 90,
				CapacityInodes:  1000,
			}

			// Use a new event recorder so that we capture only the
			// relevant events
			eventRecorder := record.NewFakeRecorder(128)
			withEventRecorderOpt := WithEventRecorder(eventRecorder)
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			withEventRecorderOpt(runner)

			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
			Expect(ok).To(BeTrue())
			Expect(err).ToNot(HaveOccurred())

			event := <-eventRecorder.Events
			wantEvent := `Warning FreeInodesThresholdReached free inodes (9.00%) are less than the configured threshold (10.00%)`
			Expect(event).To(Equal(wantEvent))
		})

		It("should not reconcile - free space and inodes threshold was not reached", func() {
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-plenty-of-space-and-inodes", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// The PVC Autoscaler targeting our test PVC
			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-plenty-of-space-and-inodes",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-plenty-of-space-and-inodes",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Sample volume info metrics with free inodes less < 10%
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1024 * 1024 * 1024,
				CapacityBytes:   1024 * 1024 * 1024,
				AvailableInodes: 10000,
				CapacityInodes:  10000,
			}

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
			Expect(ok).To(BeFalse())
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("enqueueObjects", func() {
		It("should not enqueue -- no autoscaler for PVCs", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// The test pvc
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "sample-pvc-1", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-no-pvc-for-it",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-no-pvc-for-it",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// A fast space and inodes "consumer"
			metricsSource := fake.New(fake.WithInterval(10 * time.Millisecond))
			fakeItem := &fake.Item{
				NamespacedName:         client.ObjectKeyFromObject(pvc),
				CapacityBytes:          10000,
				AvailableBytes:         10000,
				CapacityInodes:         10000,
				AvailableInodes:        10000,
				ConsumeBytesIncrement:  1000,
				ConsumeInodesIncrement: 1000,
			}
			metricsSource.Register(fakeItem)

			newCtx, cancelFunc := context.WithCancel(parentCtx)
			go func() {
				ch := time.After(500 * time.Millisecond)
				<-ch
				cancelFunc()
			}()
			metricsSource.Start(newCtx)

			// Reconfigure the periodic runner, so that we always
			// start with a clean state of events. Also, reconfigure
			// the metrics source.
			eventCh := make(chan event.GenericEvent, 128)
			withEventChOpt := WithEventChannel(eventCh)
			withMetricsSourceOpt := WithMetricsSource(metricsSource)
			withEventChOpt(runner)
			withMetricsSourceOpt(runner)

			// We should not see any events for this PVC, even if it
			// is already full, since we haven't annotated it
			Expect(runner.enqueueObjects(parentCtx)).To(Succeed())
			waitCh := time.After(500 * time.Millisecond)
			select {
			case obj := <-eventCh:
				Expect(obj).To(BeNil())
			case <-waitCh:
				break // nolint:revive
			}
		})

		It("should not enqueue -- failed to get metrics", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// The test pvc
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "sample-pvc-2", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "sample-pvc-2",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"sample-pvca-2",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Reconfigure the periodic runner to use an always failing metrics source
			metricsSource := &fake.AlwaysFailing{}
			withMetricsSourceOpt := WithMetricsSource(metricsSource)
			withMetricsSourceOpt(runner)

			// We should not see any events for this PVC, since the
			// metrics source is returning errors
			Expect(runner.enqueueObjects(parentCtx)).NotTo(Succeed())
		})

		It("should enqueue -- threshold has been reached", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// The test pvc
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "sample-pvc-3", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "sample-pvc-3",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"sample-pvca-3",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// A fast space and inodes "consumer"
			metricsSource := fake.New(fake.WithInterval(10 * time.Millisecond))
			fakeItem := &fake.Item{
				NamespacedName:         client.ObjectKeyFromObject(pvc),
				CapacityBytes:          10000,
				AvailableBytes:         10000,
				CapacityInodes:         10000,
				AvailableInodes:        10000,
				ConsumeBytesIncrement:  1000,
				ConsumeInodesIncrement: 1000,
			}
			metricsSource.Register(fakeItem)

			newCtx, cancelFunc := context.WithCancel(parentCtx)
			go func() {
				ch := time.After(500 * time.Millisecond)
				<-ch
				cancelFunc()
			}()
			metricsSource.Start(newCtx)

			// Reconfigure the periodic runner, so that we always
			// start with a clean state of events. Also, reconfigure
			// the metrics source.
			eventCh := make(chan event.GenericEvent, 128)
			withEventChOpt := WithEventChannel(eventCh)
			withMetricsSourceOpt := WithMetricsSource(metricsSource)
			withEventChOpt(runner)
			withMetricsSourceOpt(runner)

			// We should see an event that our test PVC needs to be reconciled
			Expect(runner.enqueueObjects(parentCtx)).To(Succeed())
			waitCh := time.After(500 * time.Millisecond)
			select {
			case obj := <-eventCh:
				Expect(obj).NotTo(BeNil())
			case <-waitCh:
				break // nolint:revive
			}
		})
	})

	Context("Start periodic runner", func() {
		It("should fail to enqueue because of metrics source", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// The test pvc
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "sample-pvc-4", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "sample-pvc-4",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: resource.MustParse("1Gi"),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"sample-pvca-4",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Reconfigure the periodic runner to use our always
			// failing metrics source. Also, change the schedule to
			// run more frequently.
			withMetricsSourceOpt := WithMetricsSource(&fake.AlwaysFailing{})
			withMetricsSourceOpt(runner)
			withIntervalOpt := WithInterval(100 * time.Millisecond)
			withIntervalOpt(runner)

			// Inspect the log messages, that it did actually failed
			// to enqueue objects
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))

			ctx1, cancelFunc := context.WithTimeout(parentCtx, time.Second)
			defer cancelFunc()
			ctx2 := log.IntoContext(ctx1, logger)
			Expect(runner.Start(ctx2)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("failed to enqueue persistentvolumeclaimautoscalers"))
		})
	})
})
