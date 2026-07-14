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
	appsv1 "k8s.io/api/apps/v1"
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

const nonExistentPVCName = "non-existent-pvc"

// createPVC creates a PVC via k8sClient and waits until mgrClient's cache observes it.
func createPVC(ctx context.Context, name string, storageClassName *string, volumeMode *corev1.PersistentVolumeMode) *corev1.PersistentVolumeClaim {
	pvc, err := testutils.CreatePVC(ctx, k8sClient, name, "1Gi", storageClassName, volumeMode)
	Expect(err).NotTo(HaveOccurred())
	Expect(pvc).NotTo(BeNil())

	cached := &corev1.PersistentVolumeClaim{}
	Eventually(func() error {
		return mgrClient.Get(ctx, client.ObjectKeyFromObject(pvc), cached)
	}).Should(Succeed())

	return pvc
}

// createPVCA creates a PVCA via k8sClient and waits until mgrClient's cache observes it,
// so the field index is populated before the runner runs.
// autoscalerName is optional — pass "" for an unclassed PVCA.
func createPVCA(ctx context.Context, name, autoscalerName string, targetRef autoscalingv1.CrossVersionObjectReference, volumePolicies []v1alpha1.VolumePolicy) *v1alpha1.PersistentVolumeClaimAutoscaler {
	pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(ctx, k8sClient, name, targetRef, volumePolicies)
	Expect(err).NotTo(HaveOccurred())
	Expect(pvca).NotTo(BeNil())

	if autoscalerName != "" {
		patch := client.MergeFrom(pvca.DeepCopy())
		pvca.Spec.AutoscalerName = autoscalerName
		Expect(k8sClient.Patch(ctx, pvca, patch)).To(Succeed())
	}
	cached := &v1alpha1.PersistentVolumeClaimAutoscaler{}
	Eventually(func() error {
		return mgrClient.Get(ctx, client.ObjectKeyFromObject(pvca), cached)
	}).Should(Succeed())

	return pvca
}

// waitForPVCACacheSync waits until mgrClient's cache reflects the given PVCA's current
// ResourceVersion, ensuring a preceding k8sClient.Patch is visible to the runner.
func waitForPVCACacheSync(ctx context.Context, pvca *v1alpha1.PersistentVolumeClaimAutoscaler) {
	rv := pvca.ResourceVersion
	Eventually(func(g Gomega) {
		cached := &v1alpha1.PersistentVolumeClaimAutoscaler{}
		err := mgrClient.Get(ctx, client.ObjectKeyFromObject(pvca), cached)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(cached.ResourceVersion).To(Equal(rv))
	}).Should(Succeed())
}

// creates a new test periodic runner
func newRunner() (*Runner, error) {
	metricsSource := fake.New(
		fake.WithInterval(time.Second),
	)

	runner, err := New(
		WithClient(mgrClient),
		WithEventRecorder(eventRecorder),
		WithInterval(time.Second),
		WithMetricsSource(metricsSource),
		WithPVCFetcher(pvcFetcher),
		WithAutoscalerName(""),
	)

	return runner, err
}

// createPodWithPVC creates a Pod in the "default" namespace with the given
// labels and a single PVC volume referencing claimName. The Pod is registered
// for cleanup via DeferCleanup.
func createPodWithPVC(ctx context.Context, name string, labels map[string]string, claimName string) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
				},
			}},
		},
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	DeferCleanup(func() {
		Expect(testutils.CleanupObject(ctx, k8sClient, pod)).To(Succeed())
	})
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

	Context("getVolumePolicy", func() {
		It("should return error on invalid glob pattern", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "[",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := getVolumePolicy("data-pvc", volumePolicies)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid volume policy name \"[\""))
			Expect(policy).To(BeNil())
		})

		It("should return nil when no policy matches", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "other-pvc",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := getVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).To(BeNil())
		})

		It("should match exact name", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "data-pvc",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := getVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("data-pvc"))
		})

		It("should match glob pattern", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "*-logs",
					},
					MaxCapacity: resource.MustParse("15Gi"),
				},
			}

			policy, err := getVolumePolicy("app-logs", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*-logs"))
		})

		It("should match default policy", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := getVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*"))
		})

		It("should fall back to default policy when no other policy matches", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "other-pvc",
					},
					MaxCapacity: resource.MustParse("20Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := getVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*"))
			Expect(policy.MaxCapacity).To(Equal(resource.MustParse("5Gi")))
		})

		It("should return first seen matching policy", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "data-*",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "data-pvc",
					},
					MaxCapacity: resource.MustParse("20Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := getVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("data-*"))
			Expect(policy.MaxCapacity).To(Equal(resource.MustParse("10Gi")))
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
			pvc = createPVC(parentCtx, "test-pvc", ptr.To(testutils.StorageClassName), nil)

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
			pvca = createPVCA(parentCtx, "test-pvca", "", targetRef, defaultVolumePolicies)

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

			It("should apply 4% tolerance for stale metrics detection (large PVC)", func() {
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
				waitForPVCACacheSync(parentCtx, pvca)

				By("Calling updateVolumeRecommendationForPVC with metrics deviating by 3Gi")
				volInfo := &metricssource.VolumeInfo{
					AvailableBytes:  9 * 1024 * 1024,
					CapacityBytes:   95 * 1024 * 1024 * 1024, // delta 5Gi > 4% tolerance
					AvailableInodes: 1000,
					CapacityInodes:  1000,
				}
				volumeRecommendation, err := runner.updateVolumeRecommendationForPVC(nil, pvc, volInfo)
				Expect(volumeRecommendation).To(Equal(v1alpha1.VolumeRecommendation{}))
				Expect(err).To(MatchError(common.ErrStaleMetrics))

				By("Calling updateVolumeRecommendationForPVC with metrics deviating by 1Gi")
				volInfo = &metricssource.VolumeInfo{
					AvailableBytes:  9 * 1024 * 1024,
					CapacityBytes:   98 * 1024 * 1024 * 1024, // delta 2Gi < 4% tolerance
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
						Size:              pvc.Status.Capacity.Storage(),
						UsedSpacePercent:  ptr.To(usedSpace),
						UsedInodesPercent: ptr.To(usedInodes),
					},
					Target: v1alpha1.TargetRecommendation{
						Size: pvc.Spec.Resources.Requests.Storage(),
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
						Size:              pvc.Status.Capacity.Storage(),
						UsedSpacePercent:  ptr.To(usedSpace),
						UsedInodesPercent: ptr.To(usedInodes),
					},
					Target: v1alpha1.TargetRecommendation{
						Size: pvc.Spec.Resources.Requests.Storage(),
					},
				}))
			})
		})

		Describe("#validatePVC", func() {
			It("should return ErrStorageClassNotFound", func() {
				By("Creating a PVC without a StorageClass")
				pvc := createPVC(parentCtx, "pvc-without-storageclass", nil, nil)
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

				pvca := createPVCA(parentCtx, "pvca-without-storageclass", "",
					targetRef,
					defaultVolumePolicies)
				DeferCleanup(func() {
					By("Deleting PVCA targeting the PVC without StorageClass")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvca)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				volumePolicy, err := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
				Expect(err).NotTo(HaveOccurred())
				Expect(volumePolicy).NotTo(BeNil())
				err = runner.validatePVC(parentCtx, pvc, *volumePolicy)
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
				pvc := createPVC(parentCtx, "pvc-sc-no-expansion", ptr.To(scName), nil)
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

				pvca := createPVCA(parentCtx, "pvca-sc-no-expansion", "",
					targetRef,
					defaultVolumePolicies)
				DeferCleanup(func() {
					By("Deleting PVCA targeting the PVC with StorageClass")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvca)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				volumePolicy, err := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
				Expect(err).NotTo(HaveOccurred())
				Expect(volumePolicy).NotTo(BeNil())
				err = runner.validatePVC(parentCtx, pvc, *volumePolicy)
				Expect(err).To(MatchError(ErrStorageClassDoesNotSupportExpansion))
			})

			It("should return ErrVolumeModeIsNotFilesystem", func() {
				By("Creating PVC with block volume")
				pvc := createPVC(parentCtx, "pvc-block-mode", ptr.To(testutils.StorageClassName), ptr.To(corev1.PersistentVolumeBlock))
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
				pvca := createPVCA(parentCtx, "pvca-block-mode", "",
					targetRef,
					defaultVolumePolicies)
				DeferCleanup(func() {
					By("Deleting the PVCA targeting the PVC with block volume")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvca)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), pvca)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				volumePolicy, err := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
				Expect(err).NotTo(HaveOccurred())
				Expect(volumePolicy).NotTo(BeNil())
				err = runner.validatePVC(parentCtx, pvc, *volumePolicy)
				Expect(err).To(MatchError(ErrVolumeModeIsNotFilesystem))
			})

			It("should return ErrPVCNotBound when PVC is not bound", func() {
				By("Patching test PVC to simulate lost claim")
				patch := client.MergeFrom(pvc.DeepCopy())
				pvc.Status.Phase = corev1.ClaimLost
				Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())

				volumePolicy, err := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
				Expect(err).NotTo(HaveOccurred())
				Expect(volumePolicy).NotTo(BeNil())
				err = runner.validatePVC(parentCtx, pvc, *volumePolicy)
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

				volumePolicy, err := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
				Expect(err).NotTo(HaveOccurred())
				Expect(volumePolicy).NotTo(BeNil())

				ok, reason := runner.shouldResizePVC(pvc, *volumePolicy, volumeRecommendation)
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

					volumePolicy, err := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
					Expect(err).NotTo(HaveOccurred())
					Expect(volumePolicy).NotTo(BeNil())

					ok, reason := testRunner.shouldResizePVC(pvc, *volumePolicy, volumeRecommendation)
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

					volumePolicy, err := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
					Expect(err).NotTo(HaveOccurred())
					Expect(volumePolicy).NotTo(BeNil())

					ok, reason := testRunner.shouldResizePVC(pvc, *volumePolicy, volumeRecommendation)
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
				pvca.Spec.TargetRef.Name = nonExistentPVCName
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

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

			It("should return error when metrics cannot be fetched", func() {
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
				conflictingPVCA := createPVCA(parentCtx, "pvca-with-conflict", "", pvca.Spec.TargetRef, pvca.Spec.VolumePolicies)
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
				conflictingPVCA := createPVCA(parentCtx, "pvca-with-conflict", "", pvca.Spec.TargetRef, pvca.Spec.VolumePolicies)
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

			It("should set RecommendationAvailable condition to false when no matching volume policy exists", func() {
				By("Creating a PVC that has no volumePolicy match")
				noMatchPVC := createPVC(parentCtx, "no-match-pvc", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					By("Deleting no-match PVC")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, noMatchPVC)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(noMatchPVC), noMatchPVC)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating a PVCA with only a named policy that doesn't match the PVC")
				noMatchTargetRef := autoscalingv1.CrossVersionObjectReference{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       noMatchPVC.Name,
				}
				noMatchPVCA := createPVCA(parentCtx, "pvca-no-match", "", noMatchTargetRef, []v1alpha1.VolumePolicy{
					{
						Match: v1alpha1.Match{
							Name: "non-matching-pvc-name",
						},
						MaxCapacity: resource.MustParse("10Gi"),
					},
				})
				DeferCleanup(func() {
					By("Deleting no-match PVCA")
					Expect(testutils.CleanupObject(parentCtx, k8sClient, noMatchPVCA)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(noMatchPVCA), noMatchPVCA)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the RecommendationAvailable condition indicates no matching policy")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(noMatchPVCA), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonRecommendationError),
					HaveField("Message", ContainSubstring("no matching volume policy")),
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
				pvca.Spec.TargetRef.Name = nonExistentPVCName
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

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
				pvca.Spec.TargetRef.Name = nonExistentPVCName
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				for _, cond := range updatedPVCA.Status.Conditions {
					Expect(cond.Type).NotTo(Equal(string(v1alpha1.ConditionTypeResizing)))
				}
			})

			It("should aggregate recommendations for multiple PVCs when targeting a Deployment", func() {
				selectorLabels := map[string]string{"app": "deployment-target"}

				By("Creating PVCs referenced by the Deployment's pods")
				pvcA := createPVC(parentCtx, "deploy-pvc-a", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcA)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcA), pvcA)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				pvcB := createPVC(parentCtx, "deploy-pvc-b", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcB)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcB), pvcB)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating a Deployment with a label selector")
				deployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "deployment-target", Namespace: "default"},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
							},
						},
					},
				}
				Expect(k8sClient.Create(parentCtx, deployment)).To(Succeed())
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, deployment)).To(Succeed())
				})

				By("Creating Pods with PVC volumes that match the Deployment selector")
				createPodWithPVC(parentCtx, "deploy-pod-a", selectorLabels, pvcA.Name)
				createPodWithPVC(parentCtx, "deploy-pod-b", selectorLabels, pvcB.Name)

				By("Patching the PVCA to target the Deployment")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef = autoscalingv1.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployment.Name,
				}
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

				By("Registering metrics for both PVCs")
				metricsSource := fake.New(fake.WithInterval(10 * time.Millisecond))
				for _, p := range []*corev1.PersistentVolumeClaim{pvcA, pvcB} {
					metricsSource.Register(&fake.Item{
						NamespacedName:         client.ObjectKeyFromObject(p),
						CapacityBytes:          1073741824,
						AvailableBytes:         1073741824,
						CapacityInodes:         10000,
						AvailableInodes:        10000,
						ConsumeBytesIncrement:  1000,
						ConsumeInodesIncrement: 1000,
					})
				}
				metricsCtx, metricsCancel := context.WithCancel(parentCtx)
				go func() {
					<-time.After(500 * time.Millisecond)
					metricsCancel()
				}()
				metricsSource.Start(metricsCtx)
				WithMetricsSource(metricsSource)(runner)

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying aggregated RecommendationAvailable condition and per-PVC recommendations")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionTrue),
					HaveField("Reason", ReasonRecommendationsProvided),
					HaveField("Message", Equal("Recommendations have been provided")),
				)))
				names := make([]string, 0, len(updatedPVCA.Status.VolumeRecommendations))
				for _, vr := range updatedPVCA.Status.VolumeRecommendations {
					names = append(names, vr.Name)
				}
				Expect(names).To(ConsistOf(pvcA.Name, pvcB.Name))
			})

			It("should aggregate recommendations for multiple PVCs when targeting a StatefulSet", func() {
				selectorLabels := map[string]string{"app": "statefulset-target"}

				By("Creating PVCs referenced by the StatefulSet's pods")
				pvcA := createPVC(parentCtx, "sts-pvc-a", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcA)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcA), pvcA)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				pvcB := createPVC(parentCtx, "sts-pvc-b", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcB)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcB), pvcB)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating a StatefulSet with a label selector")
				statefulSet := &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: "statefulset-target", Namespace: "default"},
					Spec: appsv1.StatefulSetSpec{
						Replicas:    ptr.To(int32(2)),
						ServiceName: "statefulset-target",
						Selector:    &metav1.LabelSelector{MatchLabels: selectorLabels},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
							},
						},
					},
				}
				Expect(k8sClient.Create(parentCtx, statefulSet)).To(Succeed())
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, statefulSet)).To(Succeed())
				})

				By("Creating Pods with PVC volumes that match the StatefulSet selector")
				createPodWithPVC(parentCtx, "sts-pod-a", selectorLabels, pvcA.Name)
				createPodWithPVC(parentCtx, "sts-pod-b", selectorLabels, pvcB.Name)

				By("Patching the PVCA to target the StatefulSet")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef = autoscalingv1.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       statefulSet.Name,
				}
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

				By("Registering metrics for both PVCs")
				metricsSource := fake.New(fake.WithInterval(10 * time.Millisecond))
				for _, p := range []*corev1.PersistentVolumeClaim{pvcA, pvcB} {
					metricsSource.Register(&fake.Item{
						NamespacedName:         client.ObjectKeyFromObject(p),
						CapacityBytes:          1073741824,
						AvailableBytes:         1073741824,
						CapacityInodes:         10000,
						AvailableInodes:        10000,
						ConsumeBytesIncrement:  1000,
						ConsumeInodesIncrement: 1000,
					})
				}
				metricsCtx, metricsCancel := context.WithCancel(parentCtx)
				go func() {
					<-time.After(500 * time.Millisecond)
					metricsCancel()
				}()
				metricsSource.Start(metricsCtx)
				WithMetricsSource(metricsSource)(runner)

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying aggregated RecommendationAvailable condition and per-PVC recommendations")
				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionTrue),
					HaveField("Reason", ReasonRecommendationsProvided),
					HaveField("Message", Equal("Recommendations have been provided")),
				)))
				names := make([]string, 0, len(updatedPVCA.Status.VolumeRecommendations))
				for _, vr := range updatedPVCA.Status.VolumeRecommendations {
					names = append(names, vr.Name)
				}
				Expect(names).To(ConsistOf(pvcA.Name, pvcB.Name))
			})

			It("should set PVCFetchError when targeting a Deployment with no matching pods", func() {
				selectorLabels := map[string]string{"app": "no-matching-pods"}

				By("Creating a Deployment with a selector that matches no pods")
				deployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "deployment-no-pods", Namespace: "default"},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
							},
						},
					},
				}
				Expect(k8sClient.Create(parentCtx, deployment)).To(Succeed())
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, deployment)).To(Succeed())
				})

				By("Patching the PVCA to target the Deployment")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef = autoscalingv1.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployment.Name,
				}
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonPVCFetchError),
					HaveField("Message", ContainSubstring("no pods found")),
				)))
			})

			It("should set PVCFetchError when matching pods reference a non-existent PVC", func() {
				selectorLabels := map[string]string{"app": "missing-pvc-target"}

				By("Creating a Deployment with a label selector")
				deployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "deployment-missing-pvc", Namespace: "default"},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(1)),
						Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
							},
						},
					},
				}
				Expect(k8sClient.Create(parentCtx, deployment)).To(Succeed())
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, deployment)).To(Succeed())
				})

				By("Creating a Pod that references a PVC which does not exist")
				createPodWithPVC(parentCtx, "missing-pvc-pod", selectorLabels, nonExistentPVCName)

				By("Patching the PVCA to target the Deployment")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef = autoscalingv1.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployment.Name,
				}
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonPVCFetchError),
				)))
			})

			It("should set PVCFetchError when targeting a non-existent Deployment", func() {
				By("Patching the PVCA to target a Deployment that does not exist")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef = autoscalingv1.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "non-existent-deployment",
				}
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonPVCFetchError),
				)))
			})

			It("should aggregate to False listing only the failing PVC when one of multiple PVCs has no metrics", func() {
				selectorLabels := map[string]string{"app": "partial-metrics"}

				By("Creating two PVCs referenced by the Deployment's pods")
				pvcA := createPVC(parentCtx, "partial-pvc-a", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcA)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcA), pvcA)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				pvcB := createPVC(parentCtx, "partial-pvc-b", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcB)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcB), pvcB)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating a Deployment with a label selector")
				deployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "deployment-partial-metrics", Namespace: "default"},
					Spec: appsv1.DeploymentSpec{
						Replicas: ptr.To(int32(2)),
						Selector: &metav1.LabelSelector{MatchLabels: selectorLabels},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels},
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{{Name: "main", Image: "busybox"}},
							},
						},
					},
				}
				Expect(k8sClient.Create(parentCtx, deployment)).To(Succeed())
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, deployment)).To(Succeed())
				})

				By("Creating Pods with PVC volumes that match the Deployment selector")
				createPodWithPVC(parentCtx, "partial-pod-a", selectorLabels, pvcA.Name)
				createPodWithPVC(parentCtx, "partial-pod-b", selectorLabels, pvcB.Name)

				By("Patching the PVCA to target the Deployment")
				pvcaPatch := client.MergeFrom(pvca.DeepCopy())
				pvca.Spec.TargetRef = autoscalingv1.CrossVersionObjectReference{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployment.Name,
				}
				Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())
				waitForPVCACacheSync(parentCtx, pvca)

				By("Registering metrics for only one of the two PVCs")
				metricsSource := fake.New(fake.WithInterval(10 * time.Millisecond))
				metricsSource.Register(&fake.Item{
					NamespacedName:         client.ObjectKeyFromObject(pvcA),
					CapacityBytes:          1073741824,
					AvailableBytes:         1073741824,
					CapacityInodes:         10000,
					AvailableInodes:        10000,
					ConsumeBytesIncrement:  1000,
					ConsumeInodesIncrement: 1000,
				})
				metricsCtx, metricsCancel := context.WithCancel(parentCtx)
				go func() {
					<-time.After(500 * time.Millisecond)
					metricsCancel()
				}()
				metricsSource.Start(metricsCtx)
				WithMetricsSource(metricsSource)(runner)

				Expect(runner.reconcileAll(parentCtx)).To(Succeed())

				updatedPVCA := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvca), updatedPVCA)).To(Succeed())
				Expect(updatedPVCA.Status.Conditions).To(ContainElement(And(
					HaveField("Type", string(v1alpha1.ConditionTypeRecommendationAvailable)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonMetricsFetchError),
					HaveField("Message", And(
						ContainSubstring(pvcB.Name),
						Not(ContainSubstring(pvcA.Name+":")),
					)),
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

		Describe("#isResizeInProgress", func() {
			DescribeTable("should detect whether resize is in progress",
				func(
					pvcConditionType *corev1.PersistentVolumeClaimConditionType,
					prevSizeAnnotation *string,
					reason string,
					expectedLogSubstring string,
					expectedMessageRegex string,
					expectInProgress bool,
					expectedConditionStatus metav1.ConditionStatus,
				) {
					if pvcConditionType != nil {
						By("Patching shared pvc with condition")
						patch := client.MergeFrom(pvc.DeepCopy())
						pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
							{Type: *pvcConditionType, Status: corev1.ConditionTrue},
						}
						Expect(k8sClient.Status().Patch(parentCtx, pvc, patch)).To(Succeed())
					}
					if prevSizeAnnotation != nil {
						By("Patching shared pvc with annotation")
						annotationPatch := client.MergeFrom(pvc.DeepCopy())
						if pvc.Annotations == nil {
							pvc.Annotations = map[string]string{}
						}
						pvc.Annotations[common.AnnotationPreviousSize] = *prevSizeAnnotation
						Expect(k8sClient.Patch(parentCtx, pvc, annotationPatch)).To(Succeed())
					}

					var buf strings.Builder
					w := io.MultiWriter(GinkgoWriter, &buf)
					logger := zap.New(zap.WriteTo(w))

					aggregator := &resizingConditionAggregator{}
					inProgress := runner.isResizeInProgress(logger, pvc, reason, aggregator)
					Expect(inProgress).To(Equal(expectInProgress))

					if !expectInProgress {
						Expect(aggregator.getAggregatedCondition().Message).To(BeEmpty())

						return
					}

					if expectedLogSubstring != "" {
						Expect(buf.String()).To(ContainSubstring(expectedLogSubstring))
					}
					Expect(aggregator.getAggregatedCondition()).To(And(
						HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
						HaveField("Status", expectedConditionStatus),
						HaveField("Reason", ReasonReconcile),
						HaveField("Message", MatchRegexp(expectedMessageRegex)),
					))
				},
				Entry("should detect resize has been started",
					ptr.To(corev1.PersistentVolumeClaimResizing),
					nil,
					"passing storage threshold",
					"resize has been started",
					`storage threshold.*resize has been started`,
					true,
					metav1.ConditionTrue,
				),
				Entry("should detect filesystem resize is pending",
					ptr.To(corev1.PersistentVolumeClaimFileSystemResizePending),
					nil,
					"passing inodes threshold",
					"filesystem resize is pending",
					`passing inodes threshold.*file system resize is pending`,
					true,
					metav1.ConditionTrue,
				),
				Entry("should detect volume is being modified",
					ptr.To(corev1.PersistentVolumeClaimVolumeModifyingVolume),
					nil,
					"passing storage threshold",
					"volume is being modified",
					`storage threshold.*volume is being modified`,
					true,
					metav1.ConditionTrue,
				),
				Entry("should detect pvc is still being resized when annotation matches status",
					nil,
					ptr.To("1Gi"),
					"passing inodes threshold",
					"persistent volume claim is still being resized",
					`passing inodes threshold.*persistent volume claim is still being resized`,
					true,
					metav1.ConditionTrue,
				),
				Entry("should return false when annotation is missing",
					nil,
					nil,
					"passing storage threshold",
					"",
					"",
					false,
					metav1.ConditionTrue,
				),
				Entry("should return false when annotation no longer matches status (resize completed)",
					nil,
					ptr.To("512Mi"),
					"passing storage threshold",
					"",
					"",
					false,
					metav1.ConditionTrue,
				),
				Entry("should return true on unparseable annotation and surface the parse error in the aggregated condition",
					nil,
					ptr.To("not-a-quantity"),
					"passing storage threshold",
					"",
					`could not parse pvc.autoscaling.gardener.cloud/prev-size annotation with value not-a-quantity`,
					true,
					metav1.ConditionUnknown,
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

					aggregator := &resizingConditionAggregator{}
					volumePolicy, errPolicy := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
					Expect(errPolicy).NotTo(HaveOccurred())
					updatedRecommendation, err := runner.resizePVC(parentCtx, logger, pvc, *volumePolicy, reason, volumeRecommendation, aggregator)
					Expect(err).NotTo(HaveOccurred())
					Expect(buf.String()).To(ContainSubstring(expectedLogSubstring))

					By("Verifying PVC was resized")
					var resizedPvc corev1.PersistentVolumeClaim
					Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
					Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse(recommendedSize)))
					Expect(resizedPvc.Annotations).To(HaveKeyWithValue(common.AnnotationPreviousSize, "1Gi"))

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
				logger := zap.New(zap.WriteTo(w)).WithValues("pvc", "test-pvc")

				By("Performing first resize")
				aggregator := &resizingConditionAggregator{}
				volumePolicy, errPolicy := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
				Expect(errPolicy).NotTo(HaveOccurred())
				volumeRecommendation, err := runner.resizePVC(parentCtx, logger, pvc, *volumePolicy, "passing storage threshold", volumeRecommendation, aggregator)
				Expect(err).NotTo(HaveOccurred())

				wantLog := `"resizing persistent volume claim","pvc":"test-pvc","from":"1Gi","to":"2Gi"}`
				Expect(buf.String()).To(ContainSubstring(wantLog))

				var resizedPvc corev1.PersistentVolumeClaim
				firstIncreaseCap := resource.MustParse("2Gi")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(firstIncreaseCap))
				Expect(resizedPvc.Annotations).To(HaveKeyWithValue(common.AnnotationPreviousSize, "1Gi"))

				By("Updating PVC status to simulate actual resize")
				patch := client.MergeFrom(resizedPvc.DeepCopy())
				resizedPvc.Status.Capacity[corev1.ResourceStorage] = firstIncreaseCap
				Expect(k8sClient.Status().Patch(parentCtx, &resizedPvc, patch)).To(Succeed())

				By("Performing second resize")
				aggregator = &resizingConditionAggregator{}
				volumePolicy, errPolicy = getVolumePolicy(resizedPvc.Name, pvca.Spec.VolumePolicies)
				Expect(errPolicy).NotTo(HaveOccurred())
				volumeRecommendation, err = runner.resizePVC(parentCtx, logger, &resizedPvc, *volumePolicy, "passing storage threshold", volumeRecommendation, aggregator)
				Expect(err).NotTo(HaveOccurred())

				wantLog = `"resizing persistent volume claim","pvc":"test-pvc","from":"2Gi","to":"3Gi"}`
				Expect(buf.String()).To(ContainSubstring(wantLog))

				secondIncreaseCap := resource.MustParse("3Gi")
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
				Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(secondIncreaseCap))
				Expect(resizedPvc.Annotations).To(HaveKeyWithValue(common.AnnotationPreviousSize, "2Gi"))

				By("Updating PVC status again to simulate actual resize")
				patch = client.MergeFrom(resizedPvc.DeepCopy())
				resizedPvc.Status.Capacity[corev1.ResourceStorage] = secondIncreaseCap
				Expect(k8sClient.Status().Patch(parentCtx, &resizedPvc, patch)).To(Succeed())

				By("Expecting third attempt to fail with max capacity reached (already at max)")
				aggregator = &resizingConditionAggregator{}
				volumePolicy, errPolicy = getVolumePolicy(resizedPvc.Name, pvca.Spec.VolumePolicies)
				Expect(errPolicy).NotTo(HaveOccurred())
				_, err = runner.resizePVC(parentCtx, logger, &resizedPvc, *volumePolicy, "passing storage threshold", volumeRecommendation, aggregator)
				Expect(err).NotTo(HaveOccurred())
				Expect(buf.String()).To(ContainSubstring("max capacity reached"))

				Expect(aggregator.getAggregatedCondition()).To(And(
					HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
					HaveField("Status", metav1.ConditionFalse),
					HaveField("Reason", ReasonReconcile),
					HaveField("Message", ContainSubstring("max capacity reached")),
				))
			})

			DescribeTable("clamp resize to max capacity",
				func(maxCapacity resource.Quantity, minStep resource.Quantity, expectResize bool, expectedSize resource.Quantity) {
					volumeRecommendation := v1alpha1.VolumeRecommendation{
						Name: pvc.Name,
						Current: v1alpha1.CurrentVolumeStatus{
							UsedSpacePercent: ptr.To(95),
						},
					}

					var buf strings.Builder
					logger := zap.New(zap.WriteTo(io.MultiWriter(GinkgoWriter, &buf))).WithValues("pvc", "test-pvc")

					pvcaPatch := client.MergeFrom(pvca.DeepCopy())
					pvca.Spec.VolumePolicies[0].MaxCapacity = maxCapacity
					pvca.Spec.VolumePolicies[0].ScaleUp.MinStepAbsolute = ptr.To(minStep)
					Expect(k8sClient.Patch(parentCtx, pvca, pvcaPatch)).To(Succeed())

					aggregator := &resizingConditionAggregator{}
					volumePolicy, errPolicy := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
					Expect(errPolicy).NotTo(HaveOccurred())
					_, err := runner.resizePVC(parentCtx, logger, pvc, *volumePolicy, "passing storage threshold", volumeRecommendation, aggregator)
					Expect(err).NotTo(HaveOccurred())

					var updatedPvc corev1.PersistentVolumeClaim
					Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvc), &updatedPvc)).To(Succeed())
					Expect(updatedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(expectedSize))

					if expectResize {
						Expect(buf.String()).To(ContainSubstring("resizing persistent volume claim"))
					} else {
						Expect(buf.String()).To(ContainSubstring("max capacity reached"))
						Expect(aggregator.getAggregatedCondition()).To(And(
							HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
							HaveField("Status", metav1.ConditionFalse),
							HaveField("Reason", ReasonReconcile),
							HaveField("Message", ContainSubstring("max capacity reached")),
						))
					}
				},
				Entry("should not resize when headroom is below scaling resolution",
					resource.MustParse("1500Mi"), resource.MustParse("1Gi"), false, resource.MustParse("1Gi"),
				),
				Entry("should clamp resize to max capacity when step would overshoot",
					resource.MustParse("2Gi"), resource.MustParse("2Gi"), true, resource.MustParse("2Gi"),
				),
			)

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
					waitForPVCACacheSync(parentCtx, pvca)

					var buf strings.Builder
					logger := zap.New(zap.WriteTo(io.MultiWriter(GinkgoWriter, &buf)))

					beforeResize := time.Now()
					aggregator := &resizingConditionAggregator{}
					volumePolicy, errPolicy := getVolumePolicy(pvc.Name, pvca.Spec.VolumePolicies)
					Expect(errPolicy).NotTo(HaveOccurred())
					updatedRecommendation, err := runner.resizePVC(parentCtx, logger, pvc, *volumePolicy, "passing storage threshold", volumeRecommendation, aggregator)
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

	Context("autoscalerName filtering", func() {
		DescribeTable("should only reconcile PVCAs matching the runner's autoscalerName",
			func(runnerAutoscalerName, matchingAutoscalerName, nonMatchingAutoscalerName string) {
				By("Creating PVCs for each PVCA")
				pvcMatching := createPVC(parentCtx, "filter-pvc-matching", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcMatching)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcMatching), pvcMatching)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				pvcNonMatching := createPVC(parentCtx, "filter-pvc-nonmatching", ptr.To(testutils.StorageClassName), nil)
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcNonMatching)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcNonMatching), pvcNonMatching)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating the PVCA that should be reconciled")
				pvcaMatching := createPVCA(parentCtx, "filter-pvca-matching", matchingAutoscalerName, autoscalingv1.CrossVersionObjectReference{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       pvcMatching.Name,
				}, []v1alpha1.VolumePolicy{{MaxCapacity: resource.MustParse("10Gi")}})
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcaMatching)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcaMatching), pvcaMatching)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Creating the PVCA that should NOT be reconciled")
				pvcaNonMatching := createPVCA(parentCtx, "filter-pvca-nonmatching", nonMatchingAutoscalerName, autoscalingv1.CrossVersionObjectReference{
					APIVersion: "v1",
					Kind:       "PersistentVolumeClaim",
					Name:       pvcNonMatching.Name,
				}, []v1alpha1.VolumePolicy{{MaxCapacity: resource.MustParse("10Gi")}})
				DeferCleanup(func() {
					Expect(testutils.CleanupObject(parentCtx, k8sClient, pvcaNonMatching)).To(Succeed())
					Eventually(func() error {
						return k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcaNonMatching), pvcaNonMatching)
					}).Should(MatchError(apierrors.IsNotFound, "IsNotFound"))
				})

				By("Registering metrics for both PVCs and running the metrics source briefly")
				ms := fake.New(fake.WithInterval(10 * time.Millisecond))
				for _, p := range []*corev1.PersistentVolumeClaim{pvcMatching, pvcNonMatching} {
					ms.Register(&fake.Item{
						NamespacedName:  client.ObjectKeyFromObject(p),
						CapacityBytes:   1073741824,
						AvailableBytes:  1073741824,
						CapacityInodes:  10000,
						AvailableInodes: 10000,
					})
				}
				msCtx, msCancel := context.WithCancel(parentCtx)
				go func() {
					<-time.After(500 * time.Millisecond)
					msCancel()
				}()
				ms.Start(msCtx)

				By("Running the runner")
				r, err := New(
					WithClient(mgrClient),
					WithEventRecorder(eventRecorder),
					WithInterval(time.Second),
					WithMetricsSource(ms),
					WithPVCFetcher(pvcFetcher),
					WithAutoscalerName(runnerAutoscalerName),
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(r.reconcileAll(parentCtx)).To(Succeed())

				By("Verifying the matching PVCA has been reconciled (RecommendationAvailable condition set)")
				updatedMatching := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcaMatching), updatedMatching)).To(Succeed())
				Expect(updatedMatching.Status.Conditions).NotTo(BeEmpty(), "matching PVCA should have been reconciled")

				By("Verifying the non-matching PVCA has NOT been reconciled (no conditions)")
				updatedNonMatching := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				Expect(k8sClient.Get(parentCtx, client.ObjectKeyFromObject(pvcaNonMatching), updatedNonMatching)).To(Succeed())
				Expect(updatedNonMatching.Status.Conditions).To(BeEmpty(), "non-matching PVCA should not have been reconciled")
			},
			Entry("named runner reconciles only PVCAs with matching autoscalerName", "foo", "foo", ""),
			Entry("default runner reconciles only PVCAs with empty autoscalerName", "", "", "bar"),
		)
	})
})
