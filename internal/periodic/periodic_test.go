package periodic

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/common"
	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
	"github.com/gardener/pvc-autoscaler/internal/metrics/source/fake"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// helper function to create a new fake metrics source

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

	Context("stamping a pvc", func() {
		It("should stamp the pvc with unknown values", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ctx := context.Background()
			obj, err := testutils.CreatePVC(ctx, k8sClient, "pvc-stamp-1", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(obj).NotTo(BeNil())

			// No volume info provided, we should see default "unknown" values
			Expect(runner.stampPVC(ctx, obj, nil)).To(Succeed())
			Expect(obj.Annotations[annotation.LastCheck]).NotTo(BeEmpty())
			Expect(obj.Annotations[annotation.NextCheck]).NotTo(BeEmpty())
			Expect(obj.Annotations[annotation.UsedSpacePercentage]).To(Equal(UnknownUtilizationValue))
			Expect(obj.Annotations[annotation.FreeSpacePercentage]).To(Equal(UnknownUtilizationValue))
			Expect(obj.Annotations[annotation.UsedInodesPercentage]).To(Equal(UnknownUtilizationValue))
			Expect(obj.Annotations[annotation.FreeInodesPercentage]).To(Equal(UnknownUtilizationValue))
		})

		It("should stamp the pvc with valid percentage values", func() {
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ctx := context.Background()
			obj, err := testutils.CreatePVC(ctx, k8sClient, "pvc-stamp-2", "1Gi")
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

			// No volume info provided, we should see default "unknown" values
			Expect(runner.stampPVC(ctx, obj, volInfo)).To(Succeed())
			Expect(obj.Annotations[annotation.LastCheck]).NotTo(BeEmpty())
			Expect(obj.Annotations[annotation.NextCheck]).NotTo(BeEmpty())
			Expect(obj.Annotations[annotation.UsedSpacePercentage]).To(Equal(fmt.Sprintf("%.2f%%", usedSpace)))
			Expect(obj.Annotations[annotation.FreeSpacePercentage]).To(Equal(fmt.Sprintf("%.2f%%", freeSpace)))
			Expect(obj.Annotations[annotation.UsedInodesPercentage]).To(Equal(fmt.Sprintf("%.2f%%", usedInodes)))
			Expect(obj.Annotations[annotation.FreeInodesPercentage]).To(Equal(fmt.Sprintf("%.2f%%", freeInodes)))
		})
	})

	Context("shouldReconcilePVC predicate", func() {
		It("should return ErrNoMetrics", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-without-volinfo", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			// No metrics at all
			ok, err := runner.shouldReconcilePVC(ctx, pvc, nil)
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrNoMetrics))

			// Provide an "empty" volume info, as if we got zero
			// values for available and capacity space
			ok, err = runner.shouldReconcilePVC(ctx, pvc, &metricssource.VolumeInfo{})
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrNoMetrics))
		})

		It("should return error because of invalid/missing annotations", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-without-annotations", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ok, err := runner.shouldReconcilePVC(ctx, pvc, &metricssource.VolumeInfo{})
			Expect(ok).To(BeFalse())
			Expect(err).To(HaveOccurred())
		})

		It("should return ErrStorageClassNotFound", func() {
			ctx := context.Background()

			// This PVC does not define a storageclass
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-without-storageclass",
					Namespace: "default",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "100Gi",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					// StorageClassName is not specified
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())

			// Update status of the pvc
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			ok, err := runner.shouldReconcilePVC(ctx, pvc, &metricssource.VolumeInfo{})
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrStorageClassNotFound))
		})

		It("should return ErrStorageClassDoesNotSupportExpansion", func() {
			ctx := context.Background()

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
			Expect(k8sClient.Create(ctx, sc)).To(Succeed())

			// Create a test PVC using the storageclass we've created above
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-sc-no-expansion",
					Namespace: "default",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "100Gi",
					},
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
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())

			// Update status of the pvc
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			ok, err := runner.shouldReconcilePVC(ctx, pvc, &metricssource.VolumeInfo{})
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrStorageClassDoesNotSupportExpansion))
		})

		It("should return ErrVolumeModeIsNotFilesystem", func() {
			ctx := context.Background()
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-block-mode",
					Namespace: "default",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "100Gi",
					},
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
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())

			// Update status of the pvc to make it a bit more "real"
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimBound,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			// Sample volume info metrics
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1000,
				CapacityBytes:   1000,
				AvailableInodes: 1000,
				CapacityInodes:  1000,
			}
			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())
			ok, err := runner.shouldReconcilePVC(ctx, pvc, volInfo)
			Expect(ok).To(BeFalse())
			Expect(err).To(MatchError(ErrVolumeModeIsNotFilesystem))
		})

		It("shoult not reconcile - pvc is not bound", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-lost", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.ObjectMeta.Annotations = map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			pvc.Status = corev1.PersistentVolumeClaimStatus{
				Phase: corev1.ClaimLost,
				Capacity: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			// Sample volume info metrics
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1000,
				CapacityBytes:   1000,
				AvailableInodes: 1000,
				CapacityInodes:  1000,
			}

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ok, err := runner.shouldReconcilePVC(ctx, pvc, volInfo)
			Expect(ok).To(BeFalse())
			Expect(err).To(BeNil())
		})

		It("should reconcile - free space threshold reached", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-free-space-threshold-reached", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			// Sample volume info metrics with free space less < 10%
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  90,
				CapacityBytes:   1000,
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

			ok, err := runner.shouldReconcilePVC(ctx, pvc, volInfo)
			Expect(ok).To(BeTrue())
			Expect(err).To(BeNil())

			event := <-eventRecorder.Events
			wantEvent := `Warning FreeSpaceThresholdReached free space (9.00%) is less than the configured threshold (10.00%)`
			Expect(event).To(Equal(wantEvent))
		})

		It("should reconcile - free inodes threshold reached", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-free-inodes-threshold-reached", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			// Sample volume info metrics with free inodes less < 10%
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  1000,
				CapacityBytes:   1000,
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

			ok, err := runner.shouldReconcilePVC(ctx, pvc, volInfo)
			Expect(ok).To(BeTrue())
			Expect(err).To(BeNil())

			event := <-eventRecorder.Events
			wantEvent := `Warning FreeInodesThresholdReached free inodes (9.00%) are less than the configured threshold (10.00%)`
			Expect(event).To(Equal(wantEvent))
		})

		It("should not reconcile - free space and inodes threshold was not reached", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-plenty-of-space-and-inodes", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			// Sample volume info metrics with free inodes less < 10%
			volInfo := &metricssource.VolumeInfo{
				AvailableBytes:  10000,
				CapacityBytes:   10000,
				AvailableInodes: 10000,
				CapacityInodes:  10000,
			}

			runner, err := newRunner()
			Expect(err).NotTo(HaveOccurred())
			Expect(runner).NotTo(BeNil())

			ok, err := runner.shouldReconcilePVC(ctx, pvc, volInfo)
			Expect(ok).To(BeFalse())
			Expect(err).To(BeNil())
		})
	})
})