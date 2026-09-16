// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package offlineresize_test

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"
)

var (
	cfg           *rest.Config
	k8sClient     client.Client
	mgrClient     client.Client
	testEnv       *envtest.Environment
	eventRecorder = record.NewFakeRecorder(1024)
	parentCtx     context.Context
	cancelFunc    context.CancelFunc
)

func TestOfflineResize(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Offline Resize Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	By("bootstrapping test environment")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: filepath.Join("..", "..", "..", "bin", "k8s",
			fmt.Sprintf("1.31.0-%s-%s", runtime.GOOS, runtime.GOARCH)),
	}

	parentCtx, cancelFunc = context.WithCancel(context.Background())

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	Expect(corev1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(appsv1.AddToScheme(scheme.Scheme)).To(Succeed())
	Expect(v1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	Expect(err).NotTo(HaveOccurred())

	Expect(v1alpha1.AddAutoscalerNameFieldIndexer(context.Background(), mgr.GetFieldIndexer())).To(Succeed())

	mgrClient = mgr.GetClient()

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(parentCtx)).To(Succeed())
	}()

	Expect(mgr.GetCache().WaitForCacheSync(parentCtx)).To(BeTrue())

	Expect(k8sClient.Create(context.Background(), &testutils.StorageClass)).To(Succeed())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancelFunc()
	Expect(testEnv.Stop()).To(Succeed())
})
