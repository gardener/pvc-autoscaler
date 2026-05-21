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

		Describe("#updateVolumeRecommendationForPVC", func() {
			It("should return ErrNoMetrics when volInfo is nil", func() {
				volumeRecommendation, err := runner.updateVolumeRecommendationForPVC(nil, pvc, nil)
				Expect(volumeRecommendation).To(Equal(v1alpha1.VolumeRecommendation{}))
				Expect(err).To(MatchError(common.ErrNoMetrics))
			})

			It("should return ErrStaleMetrics when metrics capacity deviates by more than 0.5Gi (small PVC)", func() {
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  9 * 1024 * 1024,
					CapacityBytes:   200 * 1024 * 1024, // delta ~824MiB > 0.5Gi tolerance
					AvailableInodes: 1000,
					CapacityInodes:  1000,
				}

				volumeRecommendation, err := runner.updateVolumeRecommendationForPVC(nil, pvc, volInfo)
				Expect(volumeRecommendation).To(Equal(v1alpha1.VolumeRecommendation{}))
				Expect(err).To(MatchError(common.ErrStaleMetrics))
			})

			It("should apply 2% tolerance for stale metrics detection (large PVC)", func() {
				By("Patching PVC to 100Gi and PVCA maxCapacity to 200Gi")
				specPatch := client.MergeFrom(pvc.DeepCopy())
				pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse("100Gi")
				Expect(k8sClient.Patch(parentCtx, pvc, specPatch)).To(Succeed())

				statusPatch := client.MergeFrom(pvc.DeepCopy())
				pvc.Status.Capacity[corev1.ResourceStorage] = resource.MustParse("100Gi")
				Expect(k8sClient.Status().Patch(parentCtx, pvc, statusPatch)).To(Succeed())

				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.VolumePolicies[0].MaxCapacity = resource.MustParse("200Gi")
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				By("Calling updateVolumeRecommendationForPVC with metrics deviating by 3Gi")
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  9 * 1024 * 1024,
					CapacityBytes:   97 * 1024 * 1024 * 1024, // delta 3Gi > 2% tolerance
					AvailableInodes: 1000,
					CapacityInodes:  1000,
				}
				volumeRecommendation, err := runner.updateVolumeRecommendationForPVC(nil, pvc, volInfo)
				Expect(volumeRecommendation).To(Equal(v1alpha1.VolumeRecommendation{}))
				Expect(err).To(MatchError(common.ErrStaleMetrics))

				By("Calling updateVolumeRecommendationForPVC with metrics deviating by 1Gi")
				volInfo = &metricssource.VolumeInfo{
					AvailableBytes:  9 * 1024 * 1024,
					CapacityBytes:   99 * 1024 * 1024 * 1024, // delta 1Gi < 2% tolerance
					AvailableInodes: 1000,
					CapacityInodes:  1000,
				}
				usedSpace, _ := volInfo.UsedSpacePercentage()
				usedInodes, _ := volInfo.UsedInodesPercentage()

				volumeRecommendation, err = runner.updateVolumeRecommendationForPVC(nil, pvc, volInfo)
				Expect(err).NotTo(HaveOccurred())
				Expect(volumeRecommendation).To(Equal(v1alpha1.VolumeRecommendation{
					Name: pvc.Name,
					Current: v1alpha1.CurrentVolumeStatus{
						UsedSpacePercent:  ptr.To(usedSpace),
						UsedInodesPercent: ptr.To(usedInodes),
					},
				}))
			})

			It("should return a recommendation with valid percentage values", func() {
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  9 * 1024 * 1024,
					CapacityBytes:   1024 * 1024 * 1024,
					CapacityInodes:  1000,
					AvailableInodes: 1000,
				}
				usedSpace, _ := volInfo.UsedSpacePercentage()
				usedInodes, _ := volInfo.UsedInodesPercentage()

				volumeRecommendation, err := runner.updateVolumeRecommendationForPVC(nil, pvc, volInfo)
				Expect(err).NotTo(HaveOccurred())
				Expect(volumeRecommendation).To(Equal(v1alpha1.VolumeRecommendation{
					Name: pvc.Name,
					Current: v1alpha1.CurrentVolumeStatus{
						UsedSpacePercent:  ptr.To(usedSpace),
						UsedInodesPercent: ptr.To(usedInodes),
					},
				}))
			})
		})

		Describe("#validatePVC", func() {
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

				err = runner.validatePVC(parentCtx, pvc, volumePolicyForPVC(pvca, pvc))
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

				err = runner.validatePVC(parentCtx, pvc, volumePolicyForPVC(pvca, pvc))
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

				err = runner.validatePVC(parentCtx, pvc, volumePolicyForPVC(pvca, pvc))
				Expect(err).To(MatchError(ErrVolumeModeIsNotFilesystem))
			})

			It("should return ErrPVCNotBound when PVC is not bound", func() {
				By("Patching test PVC to simulate lost claim")
				patch := client.MergeFrom(pvc.DeepCopy())
				pvc.Status.Phase = corev1.ClaimLost
				Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

				err := runner.validatePVC(parentCtx, pvc, volumePolicyForPVC(pvca, pvc))
				Expect(err).To(MatchError(ErrPVCNotBound))
			})
		})

		Describe("#shouldResizePVC", func() {
			It("should not reconcile when threshold is not reached", func() {
				volumeRecommendation := v1alpha1.VolumeRecommendation{
					Name: pvc.Name,
					Current: v1alpha1.CurrentVolumeStatus{
						UsedSpacePercent:  ptr.To(50),
						UsedInodesPercent: ptr.To(50),
					},
				}

				ok, reason := runner.shouldResizePVC(pvc, volumePolicyForPVC(pvca, pvc), volumeRecommendation)
				Expect(ok).To(BeFalse())
				Expect(reason).To(BeEmpty())
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

				It("should reconcile - used space threshold reached", func() {
					volumeRecommendation := v1alpha1.VolumeRecommendation{
						Name: pvc.Name,
						Current: v1alpha1.CurrentVolumeStatus{
							UsedSpacePercent:  ptr.To(92),
							UsedInodesPercent: ptr.To(0),
						},
					}

					ok, reason := testRunner.shouldResizePVC(pvc, volumePolicyForPVC(pvca, pvc), volumeRecommendation)
					Expect(ok).To(BeTrue())
					Expect(reason).To(Equal("passing storage threshold"))

					event := <-eventRecorder.Events
					wantEvent := `Warning UsedSpaceThresholdReached used space (92%) exceeds the configured threshold (80%)`
					Expect(event).To(Equal(wantEvent))
				})

				It("should reconcile when used inodes threshold reached", func() {
					volumeRecommendation := v1alpha1.VolumeRecommendation{
						Name: pvc.Name,
						Current: v1alpha1.CurrentVolumeStatus{
							UsedSpacePercent:  ptr.To(0),
							UsedInodesPercent: ptr.To(91),
						},
					}

					ok, reason := testRunner.shouldResizePVC(pvc, volumePolicyForPVC(pvca, pvc), volumeRecommendation)
					Expect(ok).To(BeTrue())
					Expect(reason).To(Equal("passing inodes threshold"))

					event := <-eventRecorder.Events
					wantEvent := `Warning UsedInodesThresholdReached used inodes (91%) exceeds the configured threshold (80%)`
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
					HaveField("Reason", ReasonRecommendationsProvided),
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

			It("should set Resizing to Unknown on PVC fetch failure when it was previously set", func() {
				By("Seeding the PVCA with an existing Resizing condition")
				patch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.Conditions = []metav1.Condition{
					{
						Type:               string(v1alpha1.ConditionTypeResizing),
						Status:             metav1.ConditionTrue,
						Reason:             ReasonReconcile,
						Message:            "previous resize",
						LastTransitionTime: metav1.Now(),
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, patch)).To(Succeed())

				By("Patching PVCA to target a non-existent PVC so the fetcher fails")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef.Name = "non-existent-pvc"
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the Resizing condition transitions to Unknown")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
					HaveField("Status", metav1.ConditionUnknown),
					HaveField("Reason", ReasonPVCFetchError),
					HaveField("Message", Equal("Resizing state is unknown: failed to fetch PersistentVolumeClaims")),
				)))
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonPVCFetchError),
				)))
			})

			It("should not set a Resizing condition on PVC fetch failure when none existed before", func() {
				By("Patching PVCA to target a non-existent PVC so the fetcher fails")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef.Name = "non-existent-pvc"
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				for _, cond := range updatedPVCA.Status.Conditions {
					Expect(cond.Type).NotTo(Equal(string(v1alpha1.ConditionTypeResizing)))
				}
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

		Describe("#isResizeInProgress", func() {
			DescribeTable("should detect whether resize is in progress based on PVC conditions",
				func(
					pvcConditionType *corev1.PersistentVolumeClaimConditionType,
					recommendedSize string,
					usedSpacePercent *int,
					usedInodesPercent *int,
					reason string,
					expectedLogSubstring string,
					expectedMessageRegex string,
					expectInProgress bool,
				) {
					if pvcConditionType != nil {
						By("Patching shared pvc with condition")
						patch := client.MergeFrom(pvc.DeepCopy())
						pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
							{Type: *pvcConditionType, Status: corev1.ConditionTrue},
						}
						Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())
					}

					volumeRecommendation := v1alpha1.VolumeRecommendation{
						Name: "test-pvc",
						Current: v1alpha1.CurrentVolumeStatus{
							Size:              ptr.To(resource.MustParse(recommendedSize)),
							UsedSpacePercent:  usedSpacePercent,
							UsedInodesPercent: usedInodesPercent,
						},
					}

					var buf strings.Builder
					w := io.MultiWriter(GinkgoWriter, &buf)
					logger := zap.New(zap.WriteTo(w))
					newCtx := log.IntoContext(parentCtx, logger)

					aggregator := &resizingConditionAggregator{}
					inProgress := runner.isResizeInProgress(newCtx, pvc, reason, volumeRecommendation, aggregator)
					Expect(inProgress).To(Equal(expectInProgress))

					if expectInProgress {
						if expectedLogSubstring != "" {
							Expect(buf.String()).To(ContainSubstring(expectedLogSubstring))
						}

						Expect(aggregator.getAggregatedCondition()).To(And(
							HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
							HaveField("Status", metav1.ConditionTrue),
							HaveField("Reason", ReasonReconcile),
							HaveField("Message", MatchRegexp(expectedMessageRegex)),
						))
					} else {
						Expect(aggregator.getAggregatedCondition().Message).To(BeEmpty())
					}
				},
				Entry("should detect resize has been started",
					ptr.To(corev1.PersistentVolumeClaimResizing),
					"1Gi",
					ptr.To(95),
					nil,
					"passing storage threshold",
					"resize has been started",
					`storage threshold.*resize has been started`,
					true,
				),
				Entry("should detect filesystem resize is pending",
					ptr.To(corev1.PersistentVolumeClaimFileSystemResizePending),
					"1Gi",
					nil,
					ptr.To(95),
					"passing inodes threshold",
					"filesystem resize is pending",
					`passing inodes threshold.*file system resize is pending`,
					true,
				),
				Entry("should detect volume is being modified",
					ptr.To(corev1.PersistentVolumeClaimVolumeModifyingVolume),
					"1Gi",
					ptr.To(95),
					nil,
					"passing storage threshold",
					"volume is being modified",
					`storage threshold.*volume is being modified`,
					true,
				),
				Entry("should detect pvc is still being resized",
					nil,
					"1Gi",
					nil,
					ptr.To(95),
					"passing inodes threshold",
					"persistent volume claim is still being resized",
					`passing inodes threshold.*persistent volume claim is still being resized`,
					true,
				),
				Entry("should return false when no resize is in progress",
					nil,
					"2Gi",
					ptr.To(95),
					nil,
					"passing storage threshold",
					"",
					"",
					false,
				),
			)
		})

		Describe("#resizePVC", func() {
			DescribeTable("should resize based on threshold type",
				func(
					recommendedSize string,
					usedSpacePercent *int,
					usedInodesPercent *int,
					reason string,
					expectedLogSubstring string,
					expectedMessageRegex string,
				) {
					volumeRecommendation := v1alpha1.VolumeRecommendation{
						Name: pvc.Name,
						Current: v1alpha1.CurrentVolumeStatus{
							UsedSpacePercent:  usedSpacePercent,
							UsedInodesPercent: usedInodesPercent,
						},
					}

					var buf strings.Builder
					w := io.MultiWriter(GinkgoWriter, &buf)
					logger := zap.New(zap.WriteTo(w))
					newCtx := log.IntoContext(parentCtx, logger)

					aggregator := &resizingConditionAggregator{}
					updatedRecommendation, err := runner.resizePVC(newCtx, pvc, volumePolicyForPVC(pvca, pvc), reason, volumeRecommendation, aggregator)
					Expect(err).NotTo(HaveOccurred())
					Expect(buf.String()).To(ContainSubstring(expectedLogSubstring))

					By("Verifying PVC was resized")
					var resizedPvc corev1.PersistentVolumeClaim
					Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
					Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse(recommendedSize)))

					Expect(updatedRecommendation.Target.Size).NotTo(BeNil())
					Expect(*updatedRecommendation.Target.Size).To(Equal(resource.MustParse(recommendedSize)))

					Expect(aggregator.getAggregatedCondition()).To(And(
						HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
						HaveField("Status", metav1.ConditionTrue),
						HaveField("Reason", ReasonReconcile),
						HaveField("Message", MatchRegexp(expectedMessageRegex)),
					))
				},
				Entry("should successfully resize the pvc based on storage threshold",
					"2Gi",
					ptr.To(95),
					nil,
					"passing storage threshold",
					"resizing persistent volume claim",
					`resizing from 1Gi to 2Gi.*passing storage threshold`,
				),
				Entry("should successfully resize the pvc based on inodes threshold",
					"2Gi",
					nil,
					ptr.To(95),
					"passing inodes threshold",
					"resizing persistent volume claim",
					`resizing from 1Gi to 2Gi.*passing inodes threshold`,
				),
			)

			It("should not resize if max capacity has been reached", func() {
				volumeRecommendation := v1alpha1.VolumeRecommendation{
					Name: pvc.Name,
					Current: v1alpha1.CurrentVolumeStatus{
						UsedSpacePercent: ptr.To(95),
					},
				}

				var buf strings.Builder
				w := io.MultiWriter(GinkgoWriter, &buf)
				logger := zap.New(zap.WriteTo(w))
				newCtx := log.IntoContext(parentCtx, logger)

				By("Performing first resize")
				aggregator := &resizingConditionAggregator{}
				volumeRecommendation, err := runner.resizePVC(newCtx, pvc, volumePolicyForPVC(pvca, pvc), "passing storage threshold", volumeRecommendation, aggregator)
				Expect(err).NotTo(HaveOccurred())

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

				By("Performing second resize")
				aggregator = &resizingConditionAggregator{}
				volumeRecommendation, err = runner.resizePVC(newCtx, &resizedPvc, volumePolicyForPVC(pvca, &resizedPvc), "passing storage threshold", volumeRecommendation, aggregator)
				Expect(err).NotTo(HaveOccurred())

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
				aggregator = &resizingConditionAggregator{}
				_, err = runner.resizePVC(newCtx, &resizedPvc, volumePolicyForPVC(pvca, &resizedPvc), "passing storage threshold", volumeRecommendation, aggregator)
				Expect(err).NotTo(HaveOccurred())
				Expect(buf.String()).To(ContainSubstring("max capacity reached"))

				Expect(aggregator.getAggregatedCondition()).To(And(
					HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonReconcile),
					HaveField("Message", ContainSubstring("max capacity reached")),
				))
			})

			DescribeTable("should handle cooldown duration",
				func(lastResizeOffset time.Duration, expectResize bool, expectedLog string) {
					lastResizeTime := metav1.NewTime(time.Now().Add(lastResizeOffset))
					volumeRecommendation := v1alpha1.VolumeRecommendation{
						Name: pvc.Name,
						Current: v1alpha1.CurrentVolumeStatus{
							UsedSpacePercent: ptr.To(95),
						},
						LastResizeTime: &lastResizeTime,
					}

					pvcaPatch := client.MergeFrom(pvca.DeepCopy())
					pvca.Spec.VolumePolicies[0].ScaleUp.CooldownDuration = &metav1.Duration{Duration: time.Hour}
					Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

					var buf strings.Builder
					logger := zap.New(zap.WriteTo(io.MultiWriter(GinkgoWriter, &buf)))
					newCtx := log.IntoContext(parentCtx, logger)

					beforeResize := time.Now()
					aggregator := &resizingConditionAggregator{}
					updatedRecommendation, err := runner.resizePVC(newCtx, pvc, volumePolicyForPVC(pvca, pvc), "passing storage threshold", volumeRecommendation, aggregator)
					Expect(err).NotTo(HaveOccurred())
					Expect(buf.String()).To(ContainSubstring(expectedLog))

					var pvcObj corev1.PersistentVolumeClaim
					Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &pvcObj)).To(Succeed())
					if expectResize {
						Expect(pvcObj.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("2Gi")))

						Expect(updatedRecommendation.LastResizeTime).NotTo(BeNil())
						Expect(updatedRecommendation.LastResizeTime.Time).To(BeTemporally("~", beforeResize, time.Second))
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

		Describe("#SetStatus", func() {
			It("should set LastCheck and NextCheck based on the runner interval", func() {
				WithInterval(2 * time.Minute)(runner)

				before := time.Now()
				emptyRec := metav1.Condition{Type: string(v1alpha1.ConditionTypeRecommendationAvailable)}
				emptyRes := metav1.Condition{Type: string(v1alpha1.ConditionTypeResizing)}
				Expect(runner.setStatus(parentCtx, pvca, emptyRec, emptyRes, nil)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.LastCheck.Time).To(BeTemporally("~", before, 5*time.Second))
				Expect(updatedPVCA.Status.NextCheck.Time).To(BeTemporally("~", before.Add(2*time.Minute), 5*time.Second))
			})

			It("should persist the recommendations condition with the aggregated message", func() {
				recAgg := &recommendationsConditionAggregator{}
				recAgg.addCondition(metav1.Condition{
					Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
					Status:  metav1.ConditionTrue,
					Reason:  ReasonMetricsFetched,
					Message: "pvc-a: metrics fetched successfully",
				})
				recAgg.addCondition(metav1.Condition{
					Type:    string(v1alpha1.ConditionTypeRecommendationAvailable),
					Status:  metav1.ConditionFalse,
					Reason:  ReasonMetricsFetchError,
					Message: "pvc-b: stale metrics",
				})

				emptyRes := metav1.Condition{Type: string(v1alpha1.ConditionTypeResizing)}
				Expect(runner.setStatus(parentCtx, pvca, recAgg.getAggregatedCondition(), emptyRes, nil)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonMetricsFetchError),
					HaveField("Message", ContainSubstring("pvc-b: stale metrics")),
				)))
			})

			It("should persist the resizing condition when a non-empty condition is provided", func() {
				resAgg := &resizingConditionAggregator{}
				resAgg.addCondition(metav1.Condition{
					Type:    string(v1alpha1.ConditionTypeResizing),
					Status:  metav1.ConditionTrue,
					Reason:  ReasonReconcile,
					Message: "pvc-a: resizing from 1Gi to 2Gi",
				})

				emptyRec := metav1.Condition{Type: string(v1alpha1.ConditionTypeRecommendationAvailable)}
				Expect(runner.setStatus(parentCtx, pvca, emptyRec, resAgg.getAggregatedCondition(), nil)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
					HaveField("Status", metav1.ConditionTrue),
					HaveField("Reason", ReasonReconcile),
					HaveField("Message", Equal("PersistentVolumeClaims are being resized:\n- pvc-a: resizing from 1Gi to 2Gi")),
				)))
			})

			It("should remove an existing resizing condition when an empty condition is provided", func() {
				By("Seeding the PVCA with an existing Resizing condition")
				patch := client.MergeFrom(pvca.DeepCopy())
				pvca.Status.Conditions = []metav1.Condition{
					{
						Type:               string(v1alpha1.ConditionTypeResizing),
						Status:             metav1.ConditionTrue,
						Reason:             ReasonReconcile,
						Message:            "stale resize",
						LastTransitionTime: metav1.Now(),
					},
				}
				Expect(k8sClient.Status().Patch(parentCtx, pvca, patch)).To(Succeed())

				emptyRec := metav1.Condition{Type: string(v1alpha1.ConditionTypeRecommendationAvailable)}
				emptyRes := metav1.Condition{Type: string(v1alpha1.ConditionTypeResizing)}
				Expect(runner.setStatus(parentCtx, pvca, emptyRec, emptyRes, nil)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				for _, cond := range updatedPVCA.Status.Conditions {
					Expect(cond.Type).NotTo(Equal(string(v1alpha1.ConditionTypeResizing)))
				}
			})

			It("should sort volume recommendations by name before persisting", func() {
				recommendations := []v1alpha1.VolumeRecommendation{
					{Name: "pvc-c"},
					{Name: "pvc-a"},
					{Name: "pvc-b"},
				}

				emptyRec := metav1.Condition{Type: string(v1alpha1.ConditionTypeRecommendationAvailable)}
				emptyRes := metav1.Condition{Type: string(v1alpha1.ConditionTypeResizing)}
				Expect(runner.setStatus(parentCtx, pvca, emptyRec, emptyRes, recommendations)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				names := make([]string, 0, len(updatedPVCA.Status.VolumeRecommendations))
				for _, vr := range updatedPVCA.Status.VolumeRecommendations {
					names = append(names, vr.Name)
				}
				Expect(names).To(Equal([]string{"pvc-a", "pvc-b", "pvc-c"}))
			})
		})
	})
})
