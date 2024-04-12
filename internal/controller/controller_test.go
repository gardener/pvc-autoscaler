// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package controller_test

import (
	"context"
	"io"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/controller"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// creates a new reconciler instance
func newReconciler() (*controller.PersistentVolumeClaimReconciler, error) {
	reconciler, err := controller.New(
		controller.WithClient(k8sClient),
		controller.WithScheme(k8sClient.Scheme()),
		controller.WithEventChannel(eventCh),
		controller.WithEventRecorder(eventRecorder),
	)

	return reconciler, err
}

var _ = Describe("PersistentVolumeClaim Controller", func() {
	Context("Create reconciler instance", func() {
		It("should fail without event channel", func() {
			_, err := controller.New(
				controller.WithClient(k8sClient),
				controller.WithScheme(k8sClient.Scheme()),
				controller.WithEventRecorder(eventRecorder),
			)
			Expect(err).To(MatchError(common.ErrNoEventChannel))
		})

		It("should fail without event recorder", func() {
			_, err := controller.New(
				controller.WithClient(k8sClient),
				controller.WithScheme(k8sClient.Scheme()),
				controller.WithEventChannel(eventCh),
			)
			Expect(err).To(MatchError(common.ErrNoEventRecorder))
		})

		It("should create new reconciler instance", func() {
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())
		})

	})

	Context("When reconciling a resource", func() {
		It("should ignore missing pvc", func() {
			key := types.NamespacedName{
				Name:      "missing-pvc",
				Namespace: "default",
			}
			req := ctrl.Request{NamespacedName: key}
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())
			result, err := reconciler.Reconcile(context.Background(), req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).NotTo(HaveOccurred())
		})

		It("should refuse to reconcile pvc with invalid annotations", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-invalid-annotations",
					Namespace: "default",
					Annotations: map[string]string{
						annotation.IsEnabled:   "true",
						annotation.MaxCapacity: "100Gi",
						annotation.IncreaseBy:  "bad-increase-by",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
			ctx := context.Background()
			Expect(k8sClient.Create(ctx, pvc)).To(Succeed())

			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())

			// Inspect the log messages
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			result, err := reconciler.Reconcile(newCtx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).NotTo(HaveOccurred())

			Expect(buf.String()).To(ContainSubstring("refusing to proceed with reconciling"))
			Expect(k8sClient.Delete(newCtx, pvc)).To(Succeed())
		})
	})
})
