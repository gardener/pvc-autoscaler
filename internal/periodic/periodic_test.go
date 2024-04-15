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

	Context("Stamp PVC", func() {
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
})
