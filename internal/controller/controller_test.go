// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"io"
	"math"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/annotation"
	"github.com/gardener/pvc-autoscaler/internal/common"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"

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
func newReconciler() (*PersistentVolumeClaimReconciler, error) {
	reconciler, err := New(
		WithClient(k8sClient),
		WithScheme(k8sClient.Scheme()),
		WithEventChannel(eventCh),
		WithEventRecorder(eventRecorder),
	)

	return reconciler, err
}

var _ = Describe("PersistentVolumeClaim Controller", func() {
	Context("SetupWithManager", func() {
		It("should register with manager successfully", func() {
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())

			mgr, err := ctrl.NewManager(cfg, ctrl.Options{})
			Expect(err).NotTo(HaveOccurred())
			Expect(mgr).NotTo(BeNil())

			Expect(reconciler.SetupWithManager(mgr)).To(Succeed())
		})
	})

	Context("Create reconciler instance", func() {
		It("should fail without event channel", func() {
			_, err := New(
				WithClient(k8sClient),
				WithScheme(k8sClient.Scheme()),
				WithEventRecorder(eventRecorder),
			)
			Expect(err).To(MatchError(common.ErrNoEventChannel))
		})

		It("should fail without event recorder", func() {
			_, err := New(
				WithClient(k8sClient),
				WithScheme(k8sClient.Scheme()),
				WithEventChannel(eventCh),
			)
			Expect(err).To(MatchError(common.ErrNoEventRecorder))
		})

		It("should create new reconciler instance", func() {
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())
		})

	})

	Context("getMinIncrementBytes", func() {
		It("should return the correct value when increment > threshold", func() {
			result, err := getMinIncrementBytes(&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						annotation.IncreaseBy:   "30%",
						annotation.Threshold:    "20%",
						annotation.MinThreshold: "1Gi",
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(math.Round(result*1000) / 1000).To(Equal(math.Round(float64(1024*1024*1024) * 30 / 20)))
		})

		It("should return the correct value when increment < threshold", func() {
			result, err := getMinIncrementBytes(&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						annotation.IncreaseBy:   "10%",
						annotation.Threshold:    "20%",
						annotation.MinThreshold: "1Gi",
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(math.Round(result*1000) / 1000).To(Equal(math.Round(float64(1024*1024*1024) * 10 / 20)))
		})

		It("should return the correct value when using defaults", func() {
			result, err := getMinIncrementBytes(&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						annotation.MinThreshold: "1Gi",
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(math.Round(result*1000) / 1000).To(Equal(float64(1024 * 1024 * 1024)))
		})

		It("should return zero when minimum threshold is not configured", func() {
			result, err := getMinIncrementBytes(&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(float64(0)))
		})

		It("should allow for reasonably large increment/threshold ratio", func() {
			result, err := getMinIncrementBytes(&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						annotation.IncreaseBy:   "100%",
						annotation.Threshold:    "5%",
						annotation.MinThreshold: "1Gi",
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(math.Round(result*1000) / 1000).To(Equal(float64(20 * 1024 * 1024 * 1024)))
		})

		It("should moderate its response to an extreme increment/threshold ratio", func() {
			result, err := getMinIncrementBytes(&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						annotation.IncreaseBy:   "100%",
						annotation.Threshold:    "1%",
						annotation.MinThreshold: "1Gi",
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(math.Round(result*1000) / 1000).To(Equal(float64(20 * 1024 * 1024 * 1024)))
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
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-with-invalid-annotations", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Annotate it with some invalid annotations
			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
				annotation.IncreaseBy:  "bad-increase-by",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

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
		})

		It("should skip reconcile if pvc resize has been started", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-is-resizing", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

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
		})

		It("should skip reconcile if filesystem resize is pending", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-fs-resize-is-pending", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

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
		})

		It("should skip reconcile if volume is being modified", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-vol-is-being-modified", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

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
		})

		It("should error out on invalid prev-size annotation", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-invalid-prev-size", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
				annotation.PrevSize:    "invalid-prev-size",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())

			result, err := reconciler.Reconcile(ctx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot parse prev-size"))
		})

		It("should skip reconcile if pvc is still being resized", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-vol-is-still-being-resized", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
				annotation.PrevSize:    "1Gi", // Prev size matches current size
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			// We should see this PVC being skipped because current
			// and previous recorded size are the same, which means
			// that the pvc hasn't transitioned at all.
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

			Expect(buf.String()).To(ContainSubstring("persistent volume claim is still being resized"))
		})

		It("should successfully resize the pvc", func() {
			ctx := context.Background()
			initialCapacity := "1Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-should-resize", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "100Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())

			// Inspect the log messages and confirm that we've resized the pvc
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			result, err := reconciler.Reconcile(newCtx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(buf.String()).To(ContainSubstring("resizing persistent volume claim"))

			// We should see a prev-size annotation and the new size
			// should be increased
			var resizedPvc corev1.PersistentVolumeClaim
			increasedCapacity := resource.MustParse("2Gi") // New capacity should be 2Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(increasedCapacity))
		})

		It("should account for min-threshold when resizing the pvc", func() {
			ctx := context.Background()
			initialCapacity := "10Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-should-resize-on-min-threshold", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:    "true",
				annotation.MaxCapacity:  "100Gi",
				annotation.IncreaseBy:   "30%",
				annotation.Threshold:    "20%",
				annotation.MinThreshold: "2Gi",
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())

			// Inspect the log messages and confirm that we've resized the pvc
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			result, err := reconciler.Reconcile(newCtx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(buf.String()).To(ContainSubstring("resizing persistent volume claim"))

			// We should see a prev-size annotation and the new size
			// should be increased
			var resizedPvc corev1.PersistentVolumeClaim
			increasedCapacity := resource.MustParse("13Gi") // New capacity should be 13Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(increasedCapacity))
		})

		It("should not resize if max capacity has been reached", func() {
			ctx := context.Background()
			initialCapacity := "1Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-max-capacity-reached", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			annotations := map[string]string{
				annotation.IsEnabled:   "true",
				annotation.MaxCapacity: "3Gi", // We can resize 2 times only using the default increase-by
			}
			Expect(testutils.AnnotatePVC(ctx, k8sClient, pvc, annotations)).To(Succeed())

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvc)}
			reconciler, err := newReconciler()
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())

			// Inspect the log messages and confirm that we've resized the pvc
			var buf strings.Builder
			w := io.MultiWriter(GinkgoWriter, &buf)
			logger := zap.New(zap.WriteTo(w))
			newCtx := log.IntoContext(ctx, logger)

			// First resize
			result, err := reconciler.Reconcile(newCtx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).NotTo(HaveOccurred())

			wantLog := `"resizing persistent volume claim","from":"1Gi","to":"2Gi"}`
			Expect(buf.String()).To(ContainSubstring(wantLog))

			// We should see a prev-size annotation and the new size
			// should be increased
			var resizedPvc corev1.PersistentVolumeClaim
			firstIncreaseCap := resource.MustParse("2Gi") // New capacity should be 2Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(firstIncreaseCap))

			// Update status of the PVC, so that it seems like it
			// actually resized
			patch := client.MergeFrom(resizedPvc.DeepCopy())
			resizedPvc.Status.Capacity[corev1.ResourceStorage] = firstIncreaseCap
			Expect(k8sClient.Status().Patch(ctx, &resizedPvc, patch)).To(Succeed())

			// Reconcile for the second time
			result, err = reconciler.Reconcile(newCtx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).NotTo(HaveOccurred())

			wantLog = `"resizing persistent volume claim","from":"2Gi","to":"3Gi"}`
			Expect(buf.String()).To(ContainSubstring(wantLog))

			secondIncreaseCap := resource.MustParse("3Gi") // New capacity should be 3Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(secondIncreaseCap))

			// Update status of the PVC again, so that it seems like it
			// actually resized
			patch = client.MergeFrom(resizedPvc.DeepCopy())
			resizedPvc.Status.Capacity[corev1.ResourceStorage] = secondIncreaseCap
			Expect(k8sClient.Status().Patch(ctx, &resizedPvc, patch)).To(Succeed())

			// Trying to reconcile for the third time should result
			// in max-capacity reached events
			result, err = reconciler.Reconcile(newCtx, req)
			Expect(result).To(Equal(ctrl.Result{}))
			Expect(err).NotTo(HaveOccurred())
			Expect(buf.String()).To(ContainSubstring("max capacity reached"))
		})
	})
})
