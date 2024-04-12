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
	"k8s.io/utils/ptr"
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

// helper function to create a new test PVC object.
func createPvc(ctx context.Context, name string, capacity string) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To(testStorageClassName),
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(capacity),
				},
			},
		},
	}

	if err := k8sClient.Create(ctx, pvc); err != nil {
		return nil, err
	}

	// Bind the PVC and update the status resources in order to make it look
	// a bit more like a "real" PVC.
	patch := client.MergeFrom(pvc.DeepCopy())
	pvc.Status = corev1.PersistentVolumeClaimStatus{
		Phase: corev1.ClaimBound,
		Capacity: corev1.ResourceList{
			corev1.ResourceStorage: resource.MustParse(capacity),
		},
	}
	if err := k8sClient.Status().Patch(ctx, pvc, patch); err != nil {
		return nil, err
	}

	return pvc, nil
}

// helper function to annotate the PVC with the given annotations
func annotatePvc(ctx context.Context, pvc *corev1.PersistentVolumeClaim, annotations map[string]string) error {
	patch := client.MergeFrom(pvc.DeepCopy())
	pvc.ObjectMeta.Annotations = annotations
	return k8sClient.Patch(ctx, pvc, patch)
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
			ctx := context.Background()
			pvc, err := createPvc(ctx, "pvc-with-invalid-annotations", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Annotate it with some invalid annotations
			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
				annotation.IncreaseBy:  "bad-increase-by",
			}
			Expect(annotatePvc(ctx, pvc, annotations)).To(Succeed())

			// Reconciling this PVC should fail, because of the bad annotations
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
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

		It("should skip reconcile if pvc resize has been started", func() {
			ctx := context.Background()
			pvc, err := createPvc(ctx, "pvc-is-resizing", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(annotatePvc(ctx, pvc, annotations)).To(Succeed())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimResizing,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			// We should see this PVC being skipped because it is resizing
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
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

			Expect(buf.String()).To(ContainSubstring("resize has been started"))
			Expect(k8sClient.Delete(newCtx, pvc)).To(Succeed())
		})

		It("should skip reconcile if filesystem resize is pending", func() {
			ctx := context.Background()
			pvc, err := createPvc(ctx, "pvc-fs-resize-is-pending", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(annotatePvc(ctx, pvc, annotations)).To(Succeed())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			// We should see this PVC being skipped because the filesystem resize is pending
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
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

			Expect(buf.String()).To(ContainSubstring("filesystem resize is pending"))
			Expect(k8sClient.Delete(newCtx, pvc)).To(Succeed())
		})

		It("should skip reconcile if volume is being modified", func() {
			ctx := context.Background()
			pvc, err := createPvc(ctx, "pvc-vol-is-being-modified", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(annotatePvc(ctx, pvc, annotations)).To(Succeed())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimVolumeModifyingVolume,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			// We should see this PVC being skipped because the volume is being modified
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
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

			Expect(buf.String()).To(ContainSubstring("volume is being modified"))
			Expect(k8sClient.Delete(newCtx, pvc)).To(Succeed())
		})

		It("should error out on invalid prev-size annotation", func() {
			ctx := context.Background()
			pvc, err := createPvc(ctx, "pvc-invalid-prev-size", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
				annotation.PrevSize:    "invalid-prev-size",
			}
			Expect(annotatePvc(ctx, pvc, annotations)).To(Succeed())

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())

			result, err := reconciler.Reconcile(ctx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot parse prev-size"))
		})
	})
})
