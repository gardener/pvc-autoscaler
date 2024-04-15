package periodic

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/index"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const testStorageClassName = "my-storage-class"

var cfg *rest.Config
var k8sClient client.Client
var testEnv *envtest.Environment
var eventCh = make(chan event.GenericEvent)
var eventRecorder = record.NewFakeRecorder(1024)
var k8sCache cache.Cache
var parentCtx context.Context
var cancelFunc context.CancelFunc

func TestPeriodic(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Periodic Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		ErrorIfCRDPathMissing: false,

		// The BinaryAssetsDirectory is only required if you want to run the tests directly
		// without call the makefile target test. If not informed it will look for the
		// default path defined in controller-runtime which is /usr/local/kubebuilder/.
		// Note that you must have the required binaries setup under the bin directory to perform
		// the tests directly. When we run make test it will be setup and used automatically.
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.29.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
	}

	parentCtx, cancelFunc = context.WithCancel(context.Background())

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = corev1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	// Create cache and register our index
	cacheOpts := cache.Options{
		Scheme: scheme.Scheme,
	}
	k8sCache, err = cache.New(cfg, cacheOpts)
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sCache).NotTo(BeNil())
	err = k8sCache.IndexField(parentCtx, &corev1.PersistentVolumeClaim{}, index.Key, index.IndexerFunc)
	Expect(err).NotTo(HaveOccurred())

	go func() {
		Expect(k8sCache.Start(parentCtx)).NotTo(HaveOccurred())
	}()

	opts := client.Options{
		Scheme: scheme.Scheme,
		Cache: &client.CacheOptions{
			Reader: k8sCache,
		},
	}
	k8sClient, err = client.New(cfg, opts)
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Create test storage class
	Expect(k8sClient.Create(context.Background(), &testutils.StorageClass)).To(Succeed())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancelFunc()
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
