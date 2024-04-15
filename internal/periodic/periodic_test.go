package periodic_test

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/metrics/source/fake"
	"github.com/gardener/pvc-autoscaler/internal/periodic"
)

// helper function to create a new fake metrics source

// creates a new test periodic runner
func newRunner() (*periodic.Runner, error) {
	metricsSource := fake.New(
		fake.WithInterval(time.Second),
	)

	runner, err := periodic.New(
		periodic.WithClient(k8sClient),
		periodic.WithEventChannel(eventCh),
		periodic.WithEventRecorder(eventRecorder),
		periodic.WithInterval(time.Second),
		periodic.WithMetricsSource(metricsSource),
	)

	return runner, err
}

var _ = Describe("Periodic Runner", func() {
	Context("Create Runner instance", func() {
		It("should fail without any options", func() {
			runner, err := periodic.New()
			Expect(err).To(HaveOccurred())
			Expect(runner).To(BeNil())
		})

		It("should fail without metrics source", func() {
			runner, err := periodic.New(
				periodic.WithClient(k8sClient),
				periodic.WithEventChannel(eventCh),
				periodic.WithEventRecorder(eventRecorder),
				periodic.WithInterval(time.Second),
				periodic.WithMetricsSource(nil), // should not be nil
			)
			Expect(err).To(MatchError(periodic.ErrNoMetricsSource))
			Expect(runner).To(BeNil())
		})

		It("should fail without event channel", func() {
			runner, err := periodic.New(
				periodic.WithClient(k8sClient),
				periodic.WithEventChannel(nil), // should not be nil
				periodic.WithEventRecorder(eventRecorder),
				periodic.WithInterval(time.Second),
				periodic.WithMetricsSource(fake.New()),
			)
			Expect(err).To(MatchError(common.ErrNoEventChannel))
			Expect(runner).To(BeNil())
		})

		It("should fail without client", func() {
			runner, err := periodic.New(
				periodic.WithClient(nil), // should not be nil
				periodic.WithEventChannel(eventCh),
				periodic.WithEventRecorder(eventRecorder),
				periodic.WithInterval(time.Second),
				periodic.WithMetricsSource(fake.New()),
			)
			Expect(err).To(MatchError(periodic.ErrNoClient))
			Expect(runner).To(BeNil())
		})

		It("should fail without event recorder", func() {
			runner, err := periodic.New(
				periodic.WithClient(k8sClient),
				periodic.WithEventChannel(eventCh),
				periodic.WithEventRecorder(nil), // should not be nil
				periodic.WithInterval(time.Second),
				periodic.WithMetricsSource(fake.New()),
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
})
