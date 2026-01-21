// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package autoscaling_test

import (
	"context"
	"io"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	controller "github.com/gardener/pvc-autoscaler/internal/controller/autoscaling"
	"github.com/gardener/pvc-autoscaler/internal/utils"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"
)

// creates a new reconciler instance
func newReconciler() (*controller.PersistentVolumeClaimAutoscalerReconciler, error) {
	reconciler, err := controller.New(
		controller.WithClient(k8sClient),
		controller.WithScheme(k8sClient.Scheme()),
		controller.WithEventChannel(eventCh),
		controller.WithEventRecorder(eventRecorder),
	)

	return reconciler, err
}

// getHealthyCondition gets and returns the [utils.ConditionTypeHealthy] status
// condition for the given PVC Autoscaler resource.
func getHealthyCondition(ctx context.Context, c client.Client, key client.ObjectKey) (*metav1.Condition, error) {
	obj := &v1alpha1.PersistentVolumeClaimAutoscaler{}
	if err := c.Get(ctx, key, obj); err != nil {
		return nil, err
	}

	return meta.FindStatusCondition(obj.Status.Conditions, utils.ConditionTypeHealthy), nil
}

var _ = Describe("PersistentVolumeClaimAutoscaler Controller", func() {
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
		It("should ignore missing pvca", func() {
			key := types.NamespacedName{
				Name:      "missing-pvca",
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

		It("should skip reconcile if pvc resize has been started", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-is-resizing", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimResizing,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-is-resizing",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: ptr.To(resource.MustParse("1Gi")),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-is-resizing",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// We should see this PVC being skipped because it is resizing
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvca)}
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

			// Check status condition
			condition, err := getHealthyCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Reconciling"))
			Expect(condition.Message).To(Equal("Resize has been started"))
		})

		It("should skip reconcile if filesystem resize is pending", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-fs-resize-is-pending", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimFileSystemResizePending,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-fs-resize-is-pending",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: ptr.To(resource.MustParse("1Gi")),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-fs-resize-is-pending",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// We should see this PVC being skipped because the filesystem resize is pending
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvca)}
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

			// Check status condition
			condition, err := getHealthyCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Reconciling"))
			Expect(condition.Message).To(Equal("File system resize is pending"))
		})

		It("should skip reconcile if volume is being modified", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-vol-is-being-modified", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			// Add the status conditions
			patch := client.MergeFrom(pvc.DeepCopy())
			pvc.Status.Conditions = []corev1.PersistentVolumeClaimCondition{
				{
					Type:   corev1.PersistentVolumeClaimVolumeModifyingVolume,
					Status: corev1.ConditionTrue,
				},
			}
			Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-vol-is-being-modified",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: ptr.To(resource.MustParse("1Gi")),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-vol-is-being-modified",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			// We should see this PVC being skipped because the volume is being modified
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvca)}
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

			// Check status condition
			condition, err := getHealthyCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Reconciling"))
			Expect(condition.Message).To(Equal("Volume is being modified"))
		})

		It("should skip reconcile if pvc is still being resized", func() {
			ctx := context.Background()
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-vol-is-still-being-resized", "1Gi")
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-vol-is-still-being-resized",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: ptr.To(resource.MustParse("1Gi")),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-vol-is-still-being-resized",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			pvcaPatch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.PrevSize = resource.MustParse("1Gi")
			Expect(k8sClient.Status().Patch(ctx, pvca, pvcaPatch)).To(Succeed())

			// We should see this PVC being skipped because current
			// and previous recorded size are the same, which means
			// that the pvc hasn't transitioned at all.
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvca)}
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

			// Check status condition
			condition, err := getHealthyCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Reconciling"))
			Expect(condition.Message).To(Equal("Persistent volume claim is still being resized"))
		})

		It("should successfully resize the pvc autoscaler resource", func() {
			ctx := context.Background()
			initialCapacity := "1Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-should-resize", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-should-resize",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: ptr.To(resource.MustParse("1Gi")),
					MaxCapacity: resource.MustParse("5Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-should-resize",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvca)}
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

			// We should see new size here
			var resizedPvc corev1.PersistentVolumeClaim
			increasedCapacity := resource.MustParse("2Gi") // New capacity should be 2Gi
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), &resizedPvc)).To(Succeed())
			Expect(resizedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(increasedCapacity))

			// Check status condition
			condition, err := getHealthyCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Reconciling"))
			Expect(condition.Message).To(Equal("Resizing from 1Gi to 2Gi"))
		})

		It("should not resize if max capacity has been reached", func() {
			ctx := context.Background()
			initialCapacity := "1Gi"
			pvc, err := testutils.CreatePVC(ctx, k8sClient, "pvc-max-capacity-reached", initialCapacity)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvc).NotTo(BeNil())

			targetRef := autoscalingv1.CrossVersionObjectReference{
				APIVersion: "v1",
				Kind:       "PersistentVolumeClaim",
				Name:       "pvc-max-capacity-reached",
			}

			volumePolicies := []v1alpha1.VolumePolicy{
				{
					MinCapacity: ptr.To(resource.MustParse("1Gi")),
					MaxCapacity: resource.MustParse("3Gi"),
					ScaleUp: v1alpha1.ScaleUpPolicy{
						UtilizationThresholdPercent: ptr.To(common.DefaultThresholdValue),
						StepPercent:                 ptr.To(common.DefaultIncreaseByValue),
						MinStepAbsolute:             resource.MustParse("1Gi"),
						CooldownDuration:            metav1.Duration{Duration: 3600},
					},
				},
			}

			// The PVC Autoscaler resource targeting our test PVC
			pvca, err := testutils.CreatePersistentVolumeClaimAutoscaler(
				ctx,
				k8sClient,
				"pvca-max-capacity-reached",
				targetRef,
				volumePolicies,
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(pvca).NotTo(BeNil())

			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pvca)}
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

			wantLog := `"resizing persistent volume claim","pvc":"pvc-max-capacity-reached","from":"1Gi","to":"2Gi"}`
			Expect(buf.String()).To(ContainSubstring(wantLog))

			// PVC should be resized
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

			wantLog = `"resizing persistent volume claim","pvc":"pvc-max-capacity-reached","from":"2Gi","to":"3Gi"}`
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

			// Check status condition
			condition, err := getHealthyCondition(ctx, k8sClient, client.ObjectKeyFromObject(pvca))
			Expect(err).NotTo(HaveOccurred())
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("Reconciling"))
			Expect(condition.Message).To(Equal("Max capacity reached"))
		})
	})
})
