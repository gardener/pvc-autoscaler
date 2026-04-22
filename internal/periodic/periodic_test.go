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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
		WithPVCFetcher(pvcFetcher),
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

		It("should fail without pvc fetcher", func() {
			runner, err := New(
				WithClient(k8sClient),
				WithEventRecorder(eventRecorder),
				WithInterval(time.Second),
				WithMetricsSource(fake.New()),
				WithPVCFetcher(nil), // should not be nil
			)
			Expect(err).To(MatchError(ErrNoPVCFetcher))
			Expect(runner).To(BeNil())
		})

		It("should create instance successfully", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
		})
	})

	Context("With runner instance", func() {
		var (
			runner                *Runner
			pvc                   *corev1.PersistentVolumeClaim
			pvca                  *v1alpha1.PersistentVolumeClaimAutoscaler
			defaultVolumePolicies []v1alpha1.VolumePolicy
		)

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

			By("Creating PVC for tests")
			pvc, err = testutils.CreatePVC(parentCtx, k8sClient, "test-pvc", "1Gi", ptr.To(testutils.StorageClassName), nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			DeferCleanup(func() {
				By("Deleting PVC")
				Expect(testutils.CleanupObject(parentCtx, k8sClient, pvc)).To(Succeed())
				Eventually(func() error {
					return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), pvc)
				}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
			})

			By("Creating PVCA for tests")
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
				By("Deleting PVCA")
				Expect(testutils.CleanupObject(parentCtx, k8sClient, pvca)).To(Succeed())
				Eventually(func() error {
					return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)
				}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
			})
		})

		Describe("#updatePVCAStatus", func() {
			It("should update the pvca with unknown values", func() {
				Expect(runner.updatePVCAStatus(parentCtx, pvca, nil)).To(Succeed())
				Expect(pvca.Status.LastCheck).NotTo(Equal(metav1.Time{}))
				Expect(pvca.Status.NextCheck).NotTo(Equal(metav1.Time{}))
				Expect(pvca.Status.VolumeRecommendations).To(ConsistOf(v1alpha1.VolumeRecommendation{
					Name: pvc.Name,
				}))
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
				Expect(pvca.Status.VolumeRecommendations).To(ConsistOf(v1alpha1.VolumeRecommendation{
					Name: pvc.Name,
					Current: v1alpha1.CurrentVolumeStatus{
						UsedSpacePercent:  ptr.To(usedSpace),
						UsedInodesPercent: ptr.To(usedInodes),
					},
				}))
			})
		})

		Describe("#shouldReconcilePVC", func() {
			DescribeTable("error scenarios",
				func(targetPVCName string, volInfo *metricssource.VolumeInfo, expectedErr error) {
					if targetPVCName != "" {
						By("Patching shared pvca to target " + targetPVCName)
						patch := client.MergeFrom(pvca.DeepCopy())
						pvca.Spec.TargetRef.Name = targetPVCName
						Expect(k8sClient.Patch(parentCtx, pvca, patch)).To(Succeed())
					}

					ok, _, err := runner.shouldReconcilePVC(parentCtx, pvca, volInfo)
					Expect(ok).To(BeFalse())
					if expectedErr != nil {
						Expect(err).To(MatchError(expectedErr))
					} else {
						Expect(err).To(HaveOccurred())
					}
				},
				Entry("should return error when PVC is not found",
					"non-existent-pvc",
					nil,
					nil,
				),
				Entry("should return ErrNoMetrics when volInfo is nil",
					"",
					nil,
					common.ErrNoMetrics,
				),
				Entry("should return ErrStaleMetrics when metrics capacity deviates by more than max(2%, 0.5Gi)",
					"",
					&metricssource.VolumeInfo{
						AvailableBytes:  9 * 1024 * 1024,
						CapacityBytes:   200 * 1024 * 1024, // delta ~824MiB > 0.5Gi tolerance
						AvailableInodes: 1000,
						CapacityInodes:  1000,
					},
					common.ErrStaleMetrics,
				),
			)

			It("should return ErrStorageClassNotFound", func() {
				By("Creating a PVC without a StorageClass")
				pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-without-storageclass", "1Gi", nil, nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() {
					By("Deleting PVC without a StorageClass")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvc)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), pvc)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating PVCA targeting the PVC without StorageClass")
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
				DeferCleanup(func() {
					By("Deleting PVCA targeting the PVC without StorageClass")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvca)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

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
				By("Creating a StorageClass that does not support volume expansion")
				scName := "storageclass-without-expansion"
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
				DeferCleanup(func() {
					By("Deleting StorageClass that does not support volume expansion")
					Expect(client.IgnoreNotFound(k8sClient.Delete(parentCtx, sc))).To(Succeed())
				})

				By("Creating a test PVC using the StorageClass")
				pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-sc-no-expansion", "1Gi", ptr.To(scName), nil)
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() {
					By("Deleting the PVC with StorageClass")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvc)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), pvc)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating PVCA targeting the PVC with StorageClass")
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
				DeferCleanup(func() {
					By("Deleting PVCA targeting the PVC with StorageClass")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvca)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

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
				By("Creating PVC with block volume")
				pvc, err := testutils.CreatePVC(parentCtx, k8sClient, "pvc-block-mode", "1Gi", ptr.To(testutils.StorageClassName), ptr.To(corev1.PersistentVolumeBlock))
				Expect(err).NotTo(HaveOccurred())
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvc)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), pvc)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating PVCA targeting the PVC with block volume")
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
				DeferCleanup(func() {
					By("Deleting the PVCA targeting the PVC with block volume")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvca)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Calling shouldReconcilePVC with sample volume info metrics")
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

			It("should not reconcile when threshold is not reached", func() {
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

			It("should not reconcile - lost pvc claim", func() {
				By("Patching test PVC to simulate lost claim")
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

			Context("Should reconcile when thresholds are reached", func() {
				var (
					testRunner    *Runner
					eventRecorder *record.FakeRecorder
				)

				BeforeEach(func() {
					var err error
					eventRecorder = record.NewFakeRecorder(128)
					testRunner, err = newRunner()
					Expect(err).NotTo(HaveOccurred())
					Expect(testRunner).NotTo(BeNil())
					WithEventRecorder(eventRecorder)(testRunner)
				})

				It("should reconcile - free space threshold reached", func() {
					volInfo := &metricssource.VolumeInfo{
						AvailableBytes:  90 * 1024 * 1024,
						CapacityBytes:   1024 * 1024 * 1024,
						AvailableInodes: 1000,
						CapacityInodes:  1000,
					}

					ok, reason, err := testRunner.shouldReconcilePVC(parentCtx, pvca, volInfo)
					Expect(ok).To(BeTrue())
					Expect(reason).To(Equal("passing storage threshold"))
					Expect(err).ToNot(HaveOccurred())

					event := <-eventRecorder.Events
					wantEvent := `Warning FreeSpaceThresholdReached free space (8%) is less than the configured threshold (20%)`
					Expect(event).To(Equal(wantEvent))
				})

				It("should reconcile when free inodes threshold reached", func() {
					volInfo := &metricssource.VolumeInfo{
						AvailableBytes:  1024 * 1024 * 1024,
						CapacityBytes:   1024 * 1024 * 1024,
						AvailableInodes: 90,
						CapacityInodes:  1000,
					}

					ok, reason, err := testRunner.shouldReconcilePVC(parentCtx, pvca, volInfo)
					Expect(ok).To(BeTrue())
					Expect(reason).To(Equal("passing inodes threshold"))
					Expect(err).ToNot(HaveOccurred())

					event := <-eventRecorder.Events
					wantEvent := `Warning FreeInodesThresholdReached free inodes (9%) are less than the configured threshold (20%)`
					Expect(event).To(Equal(wantEvent))
				})
			})
		})

		Describe("#reconcileAll", func() {
			It("should not reconcile when PVCA targets non-existent PVC", func() {
				By("Patching PVCA to target a non-existent PVC")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef.Name = "non-existent-pvc"
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				By("Registering metrics for the test PVC")
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

				By("Reconfiguring the periodic runner with the metrics source")
				withMetricsSourceOpt := WithMetricsSource(metricsSource)
				withMetricsSourceOpt(runner)

				By("Verifying PVC is not resized since PVCA targets a different PVC")
				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the RecommendationAvailable condition")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonPVCFetchError),
				)))
			})

			It("should retrun error when metrics cannot be fetched", func() {
				metricsSource := &fake.AlwaysFailing{}
				withMetricsSourceOpt := WithMetricsSource(metricsSource)
				withMetricsSourceOpt(runner)

				Expect(runner.reconcileAll(parentCtx)).NotTo(Succeed())
			})

			It("should set MetricsFetchError condition when no metrics for PVC", func() {
				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the RecommendationAvailable condition")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonMetricsFetchError),
				)))
			})

			It("should reconcile when threshold has been reached", func() {
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

				By("Reconfiguring the periodic runner with the metrics source")
				withMetricsSourceOpt := WithMetricsSource(metricsSource)
				withMetricsSourceOpt(runner)

				By("Expecting PVC to be reconciled when threshold is reached")
				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the RecommendationAvailable condition")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionTrue),
					HaveField("Reason", ReasonMetricsFetched),
				)))
			})

			It("should set RecommendationAvailable condition to false and not enqueue when two PVCAs manage the same PVC", func() {
				By("Creating PVCA that points to a PVC already managed by a different PVCA")
				conflictingPVCA, err := testutils.CreatePersistentVolumeClaimAutoscaler(
					parentCtx, k8sClient, "pvca-with-conflict", pvca.Spec.TargetRef, pvca.Spec.VolumePolicies,
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(conflictingPVCA).NotTo(BeNil())

				DeferCleanup(func() {
					By("Deleting conflicting PVCA")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, conflictingPVCA)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(conflictingPVCA), conflictingPVCA)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the RecommendationAvailable conditions for both PVCAs")
				for _, pvca := range []*v1alpha1.PersistentVolumeClaimAutoscaler{pvca, conflictingPVCA} {
					updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
					Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed(), "should successfully fetch pvca "+client.ObjectKeyFromObject(pvca).String())
					Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
						HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
						HaveField("Status", metav1.ConditionFalse),
						HaveField("Reason", ReasonAmbiguousPVCA),
					)), client.ObjectKeyFromObject(pvca).String()+" pvca should have expected condition")
				}
			})

			It("should set RecommendationAvailable condition to true when conflict is resolved", func() {
				By("Creating PVCA that points to a PVC already managed by a different PVCA")
				conflictingPVCA, err := testutils.CreatePersistentVolumeClaimAutoscaler(
					parentCtx, k8sClient, "pvca-with-conflict", pvca.Spec.TargetRef, pvca.Spec.VolumePolicies,
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(conflictingPVCA).NotTo(BeNil())

				DeferCleanup(func() {
					By("Deleting conflicting PVCA")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, conflictingPVCA)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(conflictingPVCA), conflictingPVCA)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying pvca's RecommendationAvailable condition has AmbiguousPersistentVolumeClaimAutoscaler reason")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonAmbiguousPVCA),
				)))

				By("Deleting conflicting pvca")
				Expect(testutils.CleanupObject(parentCtx, k8sClient, conflictingPVCA)).To(Succeed())
				Eventually(func() error {
					return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(conflictingPVCA), conflictingPVCA)
				}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the RecommendationAvailable condition no longer has AmbiguousPersistentVolumeClaimAutoscaler reason")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Reason", Not(Equal(ReasonAmbiguousPVCA))),
				)))
			})
		})

		Describe("Start", func() {
			It("should fail to reconcile when metrics source always returns errors", func() {
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

		Describe("#resizePVC", func() {
			DescribeTable("should handle resize based on PVC conditions and threshold type",
				func(
					pvcConditionType *corev1.PersistentVolumeClaimConditionType,
					recommendedSize string,
					usedSpacePercent *int,
					usedInodesPercent *int,
					reason string,
					expectedLogSubstring string,
					expectedMessageRegex string,
					expectResize bool,
				) {
					if pvcConditionType != nil {
						By("Patching shared pvc with condition")
						patch := client.MergeFrom(pvc.DeepCopy())
						pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
							{Type: *pvcConditionType, Status: corev1.ConditionTrue},
						}
						Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())
					}

					By("Patching shared pvca with volume recommendation")
					pvcaPatch := client.MergeFrom(pvca.DeepCopy())
					pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
						{
							Name: "test-pvc",
							Current: v1alpha1.CurrentVolumeStatus{
								Size:              ptr.To(resource.MustParse(recommendedSize)),
								UsedSpacePercent:  usedSpacePercent,
								UsedInodesPercent: usedInodesPercent,
							},
						},
					}
					Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

					var buf strings.Builder
					w := io.MultiWriter(GinkgoWriter, &buf)
					logger := zap.New(zap.WriteTo(w))
					newCtx := log.IntoContext(parentCtx, logger)

					Expect(runner.resizePVC(newCtx, pvca, reason)).To(Succeed())
					Expect(buf.String()).To(ContainSubstring(expectedLogSubstring))

					if expectResize {
						By("Verifying PVC was resized")
						var resizedPvc corev1.PersistentVolumeClaim
						Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
						Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse(recommendedSize)))
					}

					Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())
					Expect(pvca.Status.Conditions).To(ContainElement(And(
						HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
						HaveField("Status", metav1.ConditionTrue),
						HaveField("Reason", ReasonReconcile),
						HaveField("Message", MatchRegexp(expectedMessageRegex)),
					)))
				},
				Entry("should skip resize if pvc resize has been started",
					ptr.To(corev1.PersistentVolumeClaimResizing),
					"1Gi",
					ptr.To(95),
					nil,
					"passing storage threshold",
					"resize has been started",
					`storage threshold.*resize has been started`,
					false,
				),
				Entry("should skip resize if filesystem resize is pending",
					ptr.To(corev1.PersistentVolumeClaimFileSystemResizePending),
					"1Gi",
					nil,
					ptr.To(95),
					"passing inodes threshold",
					"filesystem resize is pending",
					`passing inodes threshold.*file system resize is pending`,
					false,
				),
				Entry("should skip resize if volume is being modified",
					ptr.To(corev1.PersistentVolumeClaimVolumeModifyingVolume),
					"1Gi",
					ptr.To(95),
					nil,
					"passing storage threshold",
					"volume is being modified",
					`storage threshold.*volume is being modified`,
					false,
				),
				Entry("should skip resize if pvc is still being resized",
					nil,
					"1Gi",
					nil,
					ptr.To(95),
					"passing inodes threshold",
					"persistent volume claim is still being resized",
					`passing inodes threshold.*persistent volume claim is still being resized`,
					false,
				),
				Entry("should successfully resize the pvc based on storage threshold",
					nil,
					"2Gi",
					ptr.To(95),
					nil,
					"passing storage threshold",
					"resizing persistent volume claim",
					`resizing from 1Gi to 2Gi.*passing storage threshold`,
					true,
				),
				Entry("should successfully resize the pvc based on inodes threshold",
					nil,
					"2Gi",
					nil,
					ptr.To(95),
					"passing inodes threshold",
					"resizing persistent volume claim",
					`resizing from 1Gi to 2Gi.*passing inodes threshold`,
					true,
				),
			)

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

				By("Performing first resize")
				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())

				wantLog := `"resizing persistent volume claim","pvc":"test-pvc","from":"1Gi","to":"2Gi"}`
				Expect(buf.String()).To(ContainSubstring(wantLog))

				var resizedPvc corev1.PersistentVolumeClaim
				firstIncreaseCap := resource.MustParse("2Gi")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(firstIncreaseCap))

				By("Updating PVC status to simulate actual resize")
				patch := client.MergeFrom(resizedPvc.DeepCopy())
				resizedPvc.Status.Capacity[corev1.ResourceStorage] = firstIncreaseCap
				Expect(k8sClient.Status().Patch(parentCtx, &resizedPvc, patch)).To(Succeed())

				By("Re-fetching the pvca to get the updated status")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())

				By("Performing second resize")
				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())

				wantLog = `"resizing persistent volume claim","pvc":"test-pvc","from":"2Gi","to":"3Gi"}`
				Expect(buf.String()).To(ContainSubstring(wantLog))

				secondIncreaseCap := resource.MustParse("3Gi")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(secondIncreaseCap))

				By("Updating PVC status again to simulate actual resize")
				patch = client.MergeFrom(resizedPvc.DeepCopy())
				resizedPvc.Status.Capacity[corev1.ResourceStorage] = secondIncreaseCap
				Expect(k8sClient.Status().Patch(parentCtx, &resizedPvc, patch)).To(Succeed())

				By("Expecting third attempt to fail with max capacity reached")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())
				Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())
				Expect(buf.String()).To(ContainSubstring("max capacity reached"))

				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())
				Expect(pvca.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonReconcile),
					HaveField("Message", ContainSubstring("max capacity reached")),
				)))
			})

			DescribeTable("should handle cooldown duration",
				func(lastResizeOffset time.Duration, expectResize bool, expectedLog string) {
					pvcaPatch := client.MergeFrom(pvca.DeepCopy())
					lastResizeTime := metav1.NewTime(time.Now().Add(lastResizeOffset))
					pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{
						{
							Name: "test-pvc",
							Current: v1alpha1.CurrentVolumeStatus{
								Size:             ptr.To(resource.MustParse("2Gi")),
								UsedSpacePercent: ptr.To(95),
							},
							LastResizeTime: &lastResizeTime,
						},
					}
					Expect(k8sClient.Status().Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

					pvcaPatch = client.MergeFrom(pvca.DeepCopy())
					pvca.Spec.VolumePolicies[0].ScaleUp.CooldownDuration = &metav1.Duration{Duration: time.Hour}
					Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

					var buf strings.Builder
					logger := zap.New(zap.WriteTo(io.MultiWriter(GinkgoWriter, &buf)))
					newCtx := log.IntoContext(parentCtx, logger)

					beforeResize := time.Now()
					Expect(runner.resizePVC(newCtx, pvca, "passing storage threshold")).To(Succeed())
					Expect(buf.String()).To(ContainSubstring(expectedLog))

					var pvcObj corev1.PersistentVolumeClaim
					Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &pvcObj)).To(Succeed())
					if expectResize {
						Expect(pvcObj.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("2Gi")))

						Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)).To(Succeed())
						Expect(pvca.Status.VolumeRecommendations[0].LastResizeTime).NotTo(BeNil())
						Expect(pvca.Status.VolumeRecommendations[0].LastResizeTime.Time).To(BeTemporally("~", beforeResize, time.Second))
					} else {
						Expect(pvcObj.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("1Gi")))
					}
				},
				Entry("should not resize if cooldown period has not elapsed",
					time.Duration(0),
					false,
					"cooldown period not elapsed",
				),
				Entry("should resize if cooldown period has elapsed and update LastResizeTime",
					-2*time.Hour,
					true,
					"resizing persistent volume claim",
				),
			)
		})
	})
})
