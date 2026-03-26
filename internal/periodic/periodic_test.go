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

	Context("With runner instance", func() {
		var runner *Runner
		var pvc *corev1.PersistentVolumeClaim
		var pvca *v1alpha1.PersistentVolumeClaimAutoscaler
		var defaultVolumePolicies []v1alpha1.VolumePolicy

		BeforeEach(func() {
			var err error
			runner, err = newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			defaultVolumePolicies = []v1alpha1.VolumePolicy{
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

			// Create shared PVC and PVCA for tests that can use them
			pvc, err = testutils.CreatePVC(parentCtx, k8sClient, "test-pvc", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "test-pvc",
			}
			pvca, err = testutils.CreatePersistentVolumeClaimAutoscaler(
				parentCtx,
				k8sClient,
				"test-pvca",
				targetRef,
				defaultVolumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			DeferCleanup(func() {
				// Remove finalizers and delete to ensure complete cleanup
				// This allows reusing the same names in subsequent tests

				// Clean up PVCA
				if err := k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca); err == nil {
					if len(pvca.Finalizers) > 0 {
						patch := client.MergeFrom(pvca.DeepCopy())
						pvca.Finalizers = nil
						Expect(k8sClient.Patch(parentCtx, pvca, patch)).To(Succeed())
					}
					Expect(client.IgnoreNotFound(k8sClient.Delete(parentCtx, pvca))).To(Succeed())
				}

				// Clean up PVC
				if err := k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), pvc); err == nil {
					if len(pvc.Finalizers) > 0 {
						patch := client.MergeFrom(pvc.DeepCopy())
						pvc.Finalizers = nil
						Expect(k8sClient.Patch(parentCtx, pvc, patch)).To(Succeed())
					}
					Expect(client.IgnoreNotFound(k8sClient.Delete(parentCtx, pvc))).To(Succeed())
				}

				// Wait for complete removal
				Eventually(func() bool {
					err := k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), &v1alpha1.PersistentVolumeClaimAutoscaler{})
					return err != nil && client.IgnoreNotFound(err) == nil
				}).Should(BeTrue())

				Eventually(func() bool {
					err := k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{})
					return err != nil && client.IgnoreNotFound(err) == nil
				}).Should(BeTrue())
			})
		})

		Context("Updating PVCA status with volume metrics (updatePVCAStatus)", func() {
			It("should update the pvca with unknown values", func() {
				Expect(runner.updatePVCAStatus(parentCtx, pvca, nil)).To(Succeed())
				Expect(pvca.Status.LastCheck).NotTo(Equal(metav1.Time{}))
				Expect(pvca.Status.NextCheck).NotTo(Equal(metav1.Time{}))
				Expect(pvca.Status.VolumeRecommendations).To(HaveLen(1))
				Expect(pvca.Status.VolumeRecommendations[0].Current.UsedSpacePercent).To(BeNil())
				Expect(pvca.Status.VolumeRecommendations[0].Current.UsedInodesPercent).To(BeNil())
			})

			It("should update the pvca with valid percentage values", func() {
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  1234,
					CapacityBytes:   123450,
					CapacityInodes:  2000,
					AvailableInodes: 12345,
				}
				usedSpace, _ := volInfo.UsedSpacePercentage()
				usedInodes, _ := volInfo.UsedInodesPercentage()

				Expect(runner.updatePVCAStatus(parentCtx, pvca, volInfo)).To(Succeed())
				Expect(pvca.Status.LastCheck).NotTo(Equal(metav1.Time{}))
				Expect(pvca.Status.NextCheck).NotTo(Equal(metav1.Time{}))
				Expect(pvca.Status.VolumeRecommendations).To(HaveLen(1))
				Expect(*pvca.Status.VolumeRecommendations[0].Current.UsedSpacePercent).To(Equal(usedSpace))
				Expect(*pvca.Status.VolumeRecommendations[0].Current.UsedInodesPercent).To(Equal(usedInodes))
			})
		})

		Context("Determining if PVC should be reconciled (shouldReconcilePVC)", func() {
			It("should return error - PVC is not found", func() {
				// Patch shared pvca to target a non-existent PVC
				patch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef.Name = "non-existent-pvc"
				Expect(k8sClient.Patch(parentCtx, pvca, patch)).To(Succeed())

				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, nil)
				Expect(ok).To(BeFalse())
				Expect(err).To(HaveOccurred())
			})

			It("should return common.ErrNoMetrics", func() {
				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, nil)
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

				pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
					parentCtx,
					k8sClient,
					"pvca-without-storageclass",
					targetRef,
					defaultVolumePolicies,
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(pvca).NotTo(BeNil())

				volInfo := &metricssource.VolumeInfo{
					CapacityBytes:   1073741824, // 1Gi
					AvailableBytes:  536870912,  // 512Mi
					CapacityInodes:  1000,
					AvailableInodes: 500,
				}
				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
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

				pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
					parentCtx,
					k8sClient,
					"pvca-sc-no-expansion",
					targetRef,
					defaultVolumePolicies,
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(pvca).NotTo(BeNil())

				volInfo := &metricssource.VolumeInfo{
					CapacityBytes:   1073741824, // 1Gi
					AvailableBytes:  536870912,  // 512Mi
					CapacityInodes:  1000,
					AvailableInodes: 500,
				}
				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
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

				pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
					parentCtx,
					k8sClient,
					"pvca-block-mode",
					targetRef,
					defaultVolumePolicies,
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

				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
				Expect(ok).To(BeFalse())
				Expect(err).To(MatchError(ErrVolumeModeIsNotFilesystem))
			})

			It("should return ErrStaleMetrics", func() {
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  9 * 1024 * 1024,
					CapacityBytes:   200 * 1024 * 1024,
					AvailableInodes: 1000,
					CapacityInodes:  1000,
				}

				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
				Expect(ok).To(BeFalse())
				Expect(err).To(MatchError(common.ErrStaleMetrics))
			})

			It("should not reconcile - lost pvc claim", func() {
				// Patch shared pvc to simulate lost claim
				patch := client.MergeFrom(pvc.DeepCopy())
				pvc.Status.Phase = corev1.ClaimLost
				Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  1000,
					CapacityBytes:   1024 * 1024 * 1024,
					AvailableInodes: 1000,
					CapacityInodes:  1000,
				}

				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
				Expect(ok).To(BeFalse())
				Expect(err).ToNot(HaveOccurred())
			})

			It("should not reconcile - free space and inodes threshold was not reached", func() {
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  1024 * 1024 * 1024,
					CapacityBytes:   1024 * 1024 * 1024,
					AvailableInodes: 10000,
					CapacityInodes:  10000,
				}

				ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
				Expect(ok).To(BeFalse())
				Expect(err).ToNot(HaveOccurred())
			})

			It("should reconcile - free space threshold reached", func() {
				// Sample volume info metrics with free space less < 10%
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  90 * 1024 * 1024,
					CapacityBytes:   1024 * 1024 * 1024,
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

				ok, reason, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
				Expect(ok).To(BeTrue())
				Expect(reason).To(Equal("passing storage threshold"))
				Expect(err).ToNot(HaveOccurred())

				event := <-eventRecorder.Events
				wantEvent := `Warning FreeSpaceThresholdReached free space (8%) is less than the configured threshold (20%)`
				Expect(event).To(Equal(wantEvent))
			})

			It("should reconcile - free inodes threshold reached", func() {
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  1024 * 1024 * 1024,
					CapacityBytes:   1024 * 1024 * 1024, // Match PVC's 1Gi
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

				ok, reason, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
				Expect(ok).To(BeTrue())
				Expect(reason).To(Equal("passing inodes threshold"))
				Expect(err).ToNot(HaveOccurred())

				event := <-eventRecorder.Events
				wantEvent := `Warning FreeInodesThresholdReached free inodes (9%) are less than the configured threshold (20%)`
				Expect(event).To(Equal(wantEvent))
			})

		})

		Context("Reconciling all PVCAs (reconcileAll)", func() {
			It("should not reconcile -- PVCA targets non-existent PVC", func() {
				// Patch shared pvca to target a non-existent PVC
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef.Name = "non-existent-pvc"
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				// Register metrics for the shared pvc (which exists but isn't targeted)
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
				metricsSource := &fake.AlwaysFailing{}
				withMetricsSourceOpt := WithMetricsSource(metricsSource)
				withMetricsSourceOpt(runner)

				// We should see an error since the metrics source is returning errors
				Expect(runner.reconcileAll(parentCtx)).NotTo(Succeed())
			})

			It("should set MetricsFetchError condition when no metrics for PVC", func() {
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

		Context("Starting the periodic runner (Start)", func() {
			It("should fail to reconcile because of metrics source", func() {
				withMetricsSourceOpt := WithMetricsSource(&fake.AlwaysFailing{})
				withMetricsSourceOpt(runner)
				withIntervalOpt := WithInterval(100 * time.Millisecond)
				withIntervalOpt(runner)

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

		Context("Resizing PVCs based on thresholds (resizePVC)", func() {
			It("should skip resize if pvc resize has been started", func() {
				// Patch shared pvc to simulate resizing condition
				patch := client.MergeFrom(pvc.DeepCopy())
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
					{
						Type:   corev1.PersistentVolumeClaimResizing,
						Status: corev1.ConditionTrue,
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

				// Patch shared pvca with volume recommendation
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
					{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:             ptr.To(resource.MustParse("1Gi")),
							UsedSpacePercent: ptr.To(95),
						},
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("resize has been started"))

				condition, err := getResizingCondition(parentCtx, k8sClient, client.ObjectKeyFromObject(pvca))
				Expect(err).NotTo(HaveOccurred())
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(ReasonReconcile))
				Expect(condition.Message).To(ContainSubstring("resize has been started"))
				Expect(condition.Message).To(ContainSubstring("storage threshold"))
			})

			It("should skip resize if filesystem resize is pending", func() {
				// Patch shared pvc to simulate fs resize pending condition
				patch := client.MergeFrom(pvc.DeepCopy())
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
					{
						Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
						Status: corev1.ConditionTrue,
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

				// Patch shared pvca with volume recommendation
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
					{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:              ptr.To(resource.MustParse("1Gi")),
							UsedInodesPercent: ptr.To(95),
						},
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				Expect(runner.resizePVC(newCtx, pvca, "passing inodes threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("filesystem resize is pending"))

				condition, err := getResizingCondition(parentCtx, k8sClient, client.ObjectKeyFromObject(pvca))
				Expect(err).NotTo(HaveOccurred())
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(ReasonReconcile))
				Expect(condition.Message).To(ContainSubstring("file system resize is pending"))
				Expect(condition.Message).To(ContainSubstring("passing inodes threshold"))
			})

			It("should skip resize if volume is being modified", func() {
				// Patch shared pvc to simulate volume modifying condition
				patch := client.MergeFrom(pvc.DeepCopy())
				pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
					{
						Type:   corev1.PersistentVolumeClaimVolumeModifyingVolume,
						Status: corev1.ConditionTrue,
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

				// Patch shared pvca with volume recommendation
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
					{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:             ptr.To(resource.MustParse("1Gi")),
							UsedSpacePercent: ptr.To(95),
						},
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("volume is being modified"))

				condition, err := getResizingCondition(parentCtx, k8sClient, client.ObjectKeyFromObject(pvca))
				Expect(err).NotTo(HaveOccurred())
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(ReasonReconcile))
				Expect(condition.Message).To(ContainSubstring("volume is being modified"))
				Expect(condition.Message).To(ContainSubstring("storage threshold"))
			})

			It("should skip resize if pvc is still being resized", func() {
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
					{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:              ptr.To(resource.MustParse("1Gi")),
							UsedInodesPercent: ptr.To(95),
						},
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				Expect(runner.resizePVC(newCtx, pvca, "passing inodes threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("persistent volume claim is still being resized"))

				condition, err := getResizingCondition(parentCtx, k8sClient, client.ObjectKeyFromObject(pvca))
				Expect(err).NotTo(HaveOccurred())
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(ReasonReconcile))
				Expect(condition.Message).To(ContainSubstring("persistent volume claim is still being resized"))
				Expect(condition.Message).To(ContainSubstring("passing inodes threshold"))
			})

			It("should successfully resize the pvc based on storage threshold", func() {
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
					{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:             ptr.To(resource.MustParse("2Gi")),
							UsedSpacePercent: ptr.To(95),
						},
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("resizing persistent volume claim"))

				var resizedPvc corev1.PersistentVolumeClaim
				increasedCapacity := resource.MustParse("2Gi")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(increasedCapacity))

				condition, err := getResizingCondition(parentCtx, k8sClient, client.ObjectKeyFromObject(pvca))
				Expect(err).NotTo(HaveOccurred())
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(ReasonReconcile))
				Expect(condition.Message).To(ContainSubstring("resizing from 1Gi to 2Gi"))
				Expect(condition.Message).To(ContainSubstring("passing storage threshold"))
			})

			It("should successfully resize the pvc based on inodes threshold", func() {
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
					{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:              ptr.To(resource.MustParse("2Gi")),
							UsedInodesPercent: ptr.To(95),
						},
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				Expect(runner.resizePVC(newCtx, pvca, "passing inodes threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("resizing persistent volume claim"))

				var resizedPvc corev1.PersistentVolumeClaim
				increasedCapacity := resource.MustParse("2Gi") // New capacity should be 2Gi
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(increasedCapacity))

				condition, err := getResizingCondition(parentCtx, k8sClient, client.ObjectKeyFromObject(pvca))
				Expect(err).NotTo(HaveOccurred())
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionTrue))
				Expect(condition.Reason).To(Equal(ReasonReconcile))
				Expect(condition.Message).To(ContainSubstring("resizing from 1Gi to 2Gi"))
				Expect(condition.Message).To(ContainSubstring("passing inodes threshold"))
			})

			It("should not resize if max capacity has been reached", func() {
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
					{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:             ptr.To(resource.MustParse("3Gi")),
							UsedSpacePercent: ptr.To(95),
						},
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				// First resize
				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())

				wantLog := `"resizing persistent volume claim","pvc":"test-pvc","from":"1Gi","to":"2Gi"}`
				Expect(buf.String()).To(ContainSubstring(wantLog))

				var resizedPvc corev1.PersistentVolumeClaim
				firstIncreaseCap := resource.MustParse("2Gi") // New capacity should be 2Gi
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(firstIncreaseCap))

				// Update status of the PVC, so that it seems like it actually resized
				patch := client.MergeFrom(resizedPvc.DeepCopy())
				resizedPvc.Status.Capacity[corev1.ResourceStorage] = firstIncreaseCap
				Expect(k8sClient.Status().Patch(parentCtx, &resizedPvc, patch)).To(Succeed())

				// Re-fetch the pvca to get the updated status
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())

				// Second resize
				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())

				wantLog = `"resizing persistent volume claim","pvc":"test-pvc","from":"2Gi","to":"3Gi"}`
				Expect(buf.String()).To(ContainSubstring(wantLog))

				secondIncreaseCap := resource.MustParse("3Gi") // New capacity should be 3Gi
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(secondIncreaseCap))

				// Update status of the PVC again, so that it seems like it
				// actually resized
				patch = client.MergeFrom(resizedPvc.DeepCopy())
				resizedPvc.Status.Capacity[corev1.ResourceStorage] = secondIncreaseCap
				Expect(k8sClient.Status().Patch(parentCtx, &resizedPvc, patch)).To(Succeed())

				// Third attempt should fail with max capacity reached
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())
				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("max capacity reached"))

				condition, err := getResizingCondition(parentCtx, k8sClient, client.ObjectKeyFromObject(pvca))
				Expect(err).NotTo(HaveOccurred())
				Expect(condition).NotTo(BeNil())
				Expect(condition.Status).To(Equal(metav1.ConditionFalse))
				Expect(condition.Reason).To(Equal(ReasonReconcile))
				Expect(condition.Message).To(ContainSubstring("max capacity reached"))
			})
		})
	})
})
