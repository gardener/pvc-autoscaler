// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package periodic

import (
	"context"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
		WithEventRecorder(eventRecorder),
		WithInterval(time.Second),
		WithMetricsSource(metricsSource),
	)

	return runner, err
}

// getResizingCondition gets and returns the [utils.ConditionTypeResizing] status
// condition for the given PVC Autoscaler resource.
func getResizingCondition(ctx context.Context, c client.Client, key client.ObjectKey) (*metav1.Condition, error) {
	obj := &v1alpha1.PersistentVolumeClaimAutoscaler{}
	if err := c.Get(ctx, key, obj); err != nil {
		return nil, err
	}

	return meta.FindStatusCondition(obj.Status.Conditions, string(v1alpha1.ConditionTypeResizing)), nil
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
				WithEventRecorder(eventRecorder),
				WithInterval(time.Second),
				WithMetricsSource(nil), // should not be nil
			)
			Expect(err).To(MatchError(ErrNoMetricsSource))
			Expect(runner).To(BeNil())
		})

		It("should fail without client", func() {
			runner, err := New(
				WithClient(nil), // should not be nil
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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

			// No volume info provided, we should see nil values for percentages
			Expect(runner.updatePVCAStatus(parentCtx, obj, nil)).To(Succeed())
			Expect(obj.Status.LastCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.NextCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.VolumeRecommendations).To(HaveLen(1))
			Expect(obj.Status.VolumeRecommendations[0].Current.UsedSpacePercent).To(BeNil())
			Expect(obj.Status.VolumeRecommendations[0].Current.UsedInodesPercent).To(BeNil())
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
			usedSpace, _ := volInfo.UsedSpacePercentage()
			usedInodes, _ := volInfo.UsedInodesPercentage()

			// We should see the computed free and used space percentages
			Expect(runner.updatePVCAStatus(parentCtx, obj, volInfo)).To(Succeed())
			Expect(obj.Status.LastCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.NextCheck).NotTo(Equal(metav1.Time{}))
			Expect(obj.Status.VolumeRecommendations).To(HaveLen(1))
			Expect(*obj.Status.VolumeRecommendations[0].Current.UsedSpacePercent).To(Equal(usedSpace))
			Expect(*obj.Status.VolumeRecommendations[0].Current.UsedInodesPercent).To(Equal(usedInodes))
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
			volInfo := &metricssource.VolumeInfo{
				CapacityBytes:   1073741824, // 1Gi
				AvailableBytes:  536870912,  // 512Mi
				CapacityInodes:  1000,
				AvailableInodes: 500,
			}
			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
			volInfo := &metricssource.VolumeInfo{
				CapacityBytes:   1073741824, // 1Gi
				AvailableBytes:  536870912,  // 512Mi
				CapacityInodes:  1000,
				AvailableInodes: 500,
			}
			ok, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
			wantEvent := `Warning FreeSpaceThresholdReached free space (9%) is less than the configured threshold (20%)`
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
			wantEvent := `Warning FreeInodesThresholdReached free inodes (9%) are less than the configured threshold (20%)`
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

					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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

	Context("reconcileAll", func() {
		It("should not reconcile -- no autoscaler for PVCs", func() {
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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

			// Reconfigure the periodic runner with the metrics source
			withMetricsSourceOpt := WithMetricsSource(metricsSource)
			withMetricsSourceOpt(runner)

			// We should not see the PVC being resized, even if it
			// is already full, since the PVCA targets a different PVC
			Expect(runner.reconcileAll(parentCtx)).To(Succeed())

			// Verify the RecommendationAvailable condition
			updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
			Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
			var foundCondition *metav1.Condition
			for i := range updatedPVCA.Status.Conditions {
				if updatedPVCA.Status.Conditions[i].Type == string(v1alpha1.ConditionTypeRecommendationAvailable) {
					foundCondition = &updatedPVCA.Status.Conditions[i]

					break
				}
			}
			Expect(foundCondition).NotTo(BeNil())
			Expect(foundCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(foundCondition.Reason).To(Equal(ReasonRecommendationError))
		})

		It("should not reconcile -- failed to get metrics", func() {
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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

			// We should see an error since the metrics source is returning errors
			Expect(runner.reconcileAll(parentCtx)).NotTo(Succeed())
		})

		It("should set MetricsFetchError condition when no metrics for PVC", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// The test pvc
			pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-no-metrics-condition", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-no-metrics-condition",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"pvca-no-metrics-condition",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Reconfigure the periodic runner with empty metrics source (no metrics registered)
			metricsSource := fake.New(fake.WithInterval(time.Second))
			withMetricsSourceOpt := WithMetricsSource(metricsSource)
			withMetricsSourceOpt(runner)

			Expect(runner.reconcileAll(parentCtx)).To(Succeed())

			// Verify the RecommendationAvailable condition
			updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
			Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
			var foundCondition *metav1.Condition
			for i := range updatedPVCA.Status.Conditions {
				if updatedPVCA.Status.Conditions[i].Type == string(v1alpha1.ConditionTypeRecommendationAvailable) {
					foundCondition = &updatedPVCA.Status.Conditions[i]

					break
				}
			}
			Expect(foundCondition).NotTo(BeNil())
			Expect(foundCondition.Status).To(Equal(metav1.ConditionFalse))
			Expect(foundCondition.Reason).To(Equal(ReasonMetricsFetchError))
		})

		It("should reconcile -- threshold has been reached", func() {
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
				CapacityBytes:          1073741824,
				AvailableBytes:         1073741824,
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

			// Reconfigure the periodic runner with the metrics source
			withMetricsSourceOpt := WithMetricsSource(metricsSource)
			withMetricsSourceOpt(runner)

			// The PVC should be reconciled when threshold is reached
			Expect(runner.reconcileAll(parentCtx)).To(Succeed())

			// Verify the RecommendationAvailable condition
			updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
			Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
			var foundCondition *metav1.Condition
			for i := range updatedPVCA.Status.Conditions {
				if updatedPVCA.Status.Conditions[i].Type == string(v1alpha1.ConditionTypeRecommendationAvailable) {
					foundCondition = &updatedPVCA.Status.Conditions[i]

					break
				}
			}
			Expect(foundCondition).NotTo(BeNil())
			Expect(foundCondition.Status).To(Equal(metav1.ConditionTrue))
			Expect(foundCondition.Reason).To(Equal(ReasonMetricsFetched))
		})
	})

	Context("Start periodic runner", func() {
		It("should fail to reconcile because of metrics source", func() {
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
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
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
			// to reconcile objects
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))

			ctx1, cancelFunc := context.WithTimeout(parentCtx, time.Second)
			defer cancelFunc()
			ctx2 := log.IntoContext(ctx1, logger)
			Expect(runner.Start(ctx2)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("failed to reconcile persistentvolumeclaimautoscalers"))
		})
	})

	Context("resizePVC", func() {
		It("should skip resize if pvc resize has been started", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-is-resizing", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimResizing,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-is-resizing",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-is-resizing",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Set UsedSpacePercent above threshold to trigger storage threshold reason
			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
				{
					Name: "pvc-is-resizing",
					Current: v1alpha1.CurrentVolumeStatus{
						Size:             ptr.To(resource.MustParse("1Gi")),
						UsedSpacePercent: ptr.To(95),
					},
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("resize has been started"))

			// Check status condition
			condition, err := getResizingCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonReconcile))
			Expect(condition.Message).To(ContainSubstring("resize has been started"))
			Expect(condition.Message).To(ContainSubstring("storage threshold"))
		})

		It("should skip resize if filesystem resize is pending", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-fs-resize-is-pending", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-fs-resize-is-pending",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-fs-resize-is-pending",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Set UsedInodesPercent above threshold to trigger inodes threshold reason
			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
				{
					Name: "pvc-fs-resize-is-pending",
					Current: v1alpha1.CurrentVolumeStatus{
						Size:              ptr.To(resource.MustParse("1Gi")),
						UsedInodesPercent: ptr.To(95),
					},
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("filesystem resize is pending"))

			// Check status condition
			condition, err := getResizingCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonReconcile))
			Expect(condition.Message).To(ContainSubstring("file system resize is pending"))
			Expect(condition.Message).To(ContainSubstring("inodes threshold"))
		})

		It("should skip resize if volume is being modified", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-vol-is-being-modified", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimVolumeModifyingVolume,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-vol-is-being-modified",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-vol-is-being-modified",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Set UsedSpacePercent above threshold to trigger storage threshold reason
			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
				{
					Name: "pvc-vol-is-being-modified",
					Current: v1alpha1.CurrentVolumeStatus{
						Size:             ptr.To(resource.MustParse("1Gi")),
						UsedSpacePercent: ptr.To(95),
					},
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("volume is being modified"))

			// Check status condition
			condition, err := getResizingCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonReconcile))
			Expect(condition.Message).To(ContainSubstring("volume is being modified"))
			Expect(condition.Message).To(ContainSubstring("storage threshold"))
		})

		It("should skip resize if pvc is still being resized", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-vol-is-still-being-resized", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-vol-is-still-being-resized",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-vol-is-still-being-resized",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Set UsedInodesPercent above threshold and Current.Size equal to pvc.status.capacity
			// to simulate resize in progress
			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
				{
					Name: "pvc-vol-is-still-being-resized",
					Current: v1alpha1.CurrentVolumeStatus{
						Size:              ptr.To(resource.MustParse("1Gi")),
						UsedInodesPercent: ptr.To(95),
					},
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("persistent volume claim is still being resized"))

			// Check status condition
			condition, err := getResizingCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonReconcile))
			Expect(condition.Message).To(ContainSubstring("persistent volume claim is still being resized"))
			Expect(condition.Message).To(ContainSubstring("inodes threshold"))
		})

		It("should successfully resize the pvc based on storage threshold", func() {
			ctx := context.Background()
			initialCapacity := "1Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-should-resize", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-should-resize",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-should-resize",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
				{
					Name: "pvc-should-resize",
					Current: v1alpha1.CurrentVolumeStatus{
						Size:             ptr.To(resource.MustParse("2Gi")),
						UsedSpacePercent: ptr.To(95),
					},
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("resizing persistent volume claim"))

			var resizedPvc corev1.PersistentVolumeClaim
			increasedCapacity := resource.MustParse("2Gi") // New capacity should be 2Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(increasedCapacity))

			// Check status condition
			condition, err := getResizingCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonReconcile))
			Expect(condition.Message).To(ContainSubstring("resizing from 1Gi to 2Gi"))
			Expect(condition.Message).To(ContainSubstring("passing storage threshold"))
		})

		It("should successfully resize the pvc based on inodes threshold", func() {
			ctx := context.Background()
			initialCapacity := "1Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-resize-inodes-threshold", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-resize-inodes-threshold",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-resize-inodes-threshold",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// Set just UsedInodesPercent above threshold
			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
				{
					Name: "pvc-resize-inodes-threshold",
					Current: v1alpha1.CurrentVolumeStatus{
						Size:              ptr.To(resource.MustParse("2Gi")),
						UsedInodesPercent: ptr.To(95),
					},
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("resizing persistent volume claim"))

			var resizedPvc corev1.PersistentVolumeClaim
			increasedCapacity := resource.MustParse("2Gi") // New capacity should be 2Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(increasedCapacity))

			// Check status condition
			condition, err := getResizingCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionTrue))
			Expect(condition.Reason).To(Equal(ReasonReconcile))
			Expect(condition.Message).To(ContainSubstring("resizing from 1Gi to 2Gi"))
			Expect(condition.Message).To(ContainSubstring("passing inodes threshold"))
		})

		It("should not resize if max capacity has been reached", func() {
			ctx := context.Background()
			initialCapacity := "1Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-max-capacity-reached", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-max-capacity-reached",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MaxCapacity: resource.MustParse("3Gi"),
					ScaleUp: ptr.To(v1alpha1.ScalingRules{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdPercent),
						StepPercent:                 ptr.To(common.DefaultStepPercent),
						MinStepAbsolute:             ptr.To(resource.MustParse("1Gi")),
						CooldownDuration:            ptr.To(metav1.Duration{Duration: 3600}),
					}),
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-max-capacity-reached",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
				{
					Name: "pvc-max-capacity-reached",
					Current: v1alpha1.CurrentVolumeStatus{
						Size:             ptr.To(resource.MustParse("3Gi")),
						UsedSpacePercent: ptr.To(95),
					},
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			// First resize
			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())

			wantLog := `"resizing persistent volume claim","pvc":"pvc-max-capacity-reached","from":"1Gi","to":"2Gi"}`
			Expect(buf.String()).To(ContainSubstring(wantLog))

			// PVC should be resized
			var resizedPvc corev1.PersistentVolumeClaim
			firstIncreaseCap := resource.MustParse("2Gi") // New capacity should be 2Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(firstIncreaseCap))

			// Update status of the PVC, so that it seems like it
			// actually resized
			patch := client.MergeFrom(resizedPvc.DeepCopy())
			resizedPvc.Status.Capacity[corev1.ResourceStorage] = firstIncreaseCap
			Expect(k8sClient.Status().Patch(ctx, &resizedPvc, patch)).To(Succeed())

			// Re-fetch the pvca to get the updated status
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())

			// Resize for the second time
			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())

			wantLog = `"resizing persistent volume claim","pvc":"pvc-max-capacity-reached","from":"2Gi","to":"3Gi"}`
			Expect(buf.String()).To(ContainSubstring(wantLog))

			secondIncreaseCap := resource.MustParse("3Gi") // New capacity should be 3Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(secondIncreaseCap))

			// Update status of the PVC again, so that it seems like it
			// actually resized
			patch = client.MergeFrom(resizedPvc.DeepCopy())
			resizedPvc.Status.Capacity[corev1.ResourceStorage] = secondIncreaseCap
			Expect(k8sClient.Status().Patch(ctx, &resizedPvc, patch)).To(Succeed())

			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())
			Expect(runner.resizePVC(newCtx, pvca)).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("max capacity reached"))

			// Check status condition
			condition, err := getResizingCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal(ReasonReconcile))
			Expect(condition.Message).To(ContainSubstring("max capacity reached"))
		})
	})
})
