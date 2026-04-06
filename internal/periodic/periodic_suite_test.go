// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package periodic

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/scale"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/target/pvcfetcher"
	"github.com/gardener/pvc-autoscaler/internal/target/selectorfetcher"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"
)

var cfg *rest.Config
var k8sClient client.Client
var testEnv *envtest.Environment
var eventRecorder = record.NewFakeRecorder(1024)
var pvcFetcher pvcfetcher.Fetcher
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
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,

		// The BinaryAssetsDirectory is only required if you want to run the tests directly
		// without call the makefile target test. If not informed it will look for the
		// default path defined in controller-runtime which is /usr/local/kubebuilder/.
		// Note that you must have the required binaries setup under the bin directory to perform
		// the tests directly. When we run make test it will be setup and used automatically.
		BinaryAssetsDirectory: filepath.Join("..", "..", "bin", "k8s",
			fmt.Sprintf("1.31.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
	}

	parentCtx, cancelFunc = context.WithCancel(context.Background())

	var err error
	// cfg is defined in this file globally.
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	err = corev1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	err = v1alpha1.AddToScheme(scheme.Scheme)
	Expect(err).NotTo(HaveOccurred())

	opts := client.Options{Scheme: scheme.Scheme}
	k8sClient, err = client.New(cfg, opts)
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	// Create test storage class
	Expect(k8sClient.Create(context.Background(), &testutils.StorageClass)).To(Succeed())

	clientSet, err := clientset.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())

	groupResources, err := restmapper.GetAPIGroupResources(clientSet.Discovery())
	Expect(err).NotTo(HaveOccurred())
	restMapper := restmapper.NewDiscoveryRESTMapper(groupResources)

	scaleKindResolver := scale.NewDiscoveryScaleKindResolver(clientSet.Discovery())
	scalesClient, err := scale.NewForConfig(cfg, restMapper, dynamic.LegacyAPIPathResolverFunc, scaleKindResolver)
	Expect(err).NotTo(HaveOccurred())

	selectorFetcher, err := selectorfetcher.New(selectorfetcher.WithRESTMapper(restMapper), selectorfetcher.WithScaleClient(scalesClient))
	Expect(err).NotTo(HaveOccurred())

	pvcFetcher, err = pvcfetcher.New(pvcfetcher.WithClient(k8sClient), pvcfetcher.WithSelectorFetcher(selectorFetcher))
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancelFunc()
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})
