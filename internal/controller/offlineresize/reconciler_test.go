// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package offlineresize_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/controller/offlineresize"
	testutils "github.com/gardener/pvc-autoscaler/test/utils"
)

var _ = Describe("Offline Resize Reconciler", func() {
	var (
		ctx        context.Context
		reconciler *offlineresize.Reconciler

		pvc  *corev1.PersistentVolumeClaim
		pod  *corev1.Pod
		pvca *v1alpha1.PersistentVolumeClaimAutoscaler
	)

	setPVCConditions := func(conditions ...corev1.PersistentVolumeClaimConditionType) {
		GinkgoHelper()
		patch := client.MergeFrom(pvc.DeepCopy())
		pvc.Status.Phase = corev1.ClaimBound
		for _, condType := range conditions {
			pvc.Status.Conditions = append(pvc.Status.Conditions, corev1.PersistentVolumeClaimCondition{
				Type:   condType,
				Status: corev1.ConditionTrue,
			})
		}
		Expect(k8sClient.Status().Patch(ctx, pvc, patch)).To(Succeed())
		Eventually(func(g Gomega) {
			observed := &corev1.PersistentVolumeClaim{}
			g.Expect(mgrClient.Get(ctx, client.ObjectKeyFromObject(pvc), observed)).To(Succeed())
			g.Expect(observed.Status.Conditions).To(HaveLen(len(conditions)))
		}).Should(Succeed())
	}

	setPodPhase := func(phase corev1.PodPhase) {
		GinkgoHelper()
		patch := client.MergeFrom(pod.DeepCopy())
		pod.Status.Phase = phase
		Expect(k8sClient.Status().Patch(ctx, pod, patch)).To(Succeed())
		Eventually(func(g Gomega) {
			observed := &corev1.Pod{}
			g.Expect(mgrClient.Get(ctx, client.ObjectKeyFromObject(pod), observed)).To(Succeed())
			g.Expect(observed.Status.Phase).To(Equal(phase))
		}).Should(Succeed())
	}

	BeforeEach(func() {
		ctx = context.Background()

		var err error
		reconciler, err = offlineresize.New(
			offlineresize.WithClient(mgrClient),
			offlineresize.WithEventRecorder(eventRecorder),
			offlineresize.WithAutoscalerName(""),
		)
		Expect(err).NotTo(HaveOccurred())

		pvc = &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
				Annotations: map[string]string{
					common.AnnotationPreviousSize: "10Gi",
				},
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				StorageClassName: ptr.To(testutils.StorageClassName),
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				VolumeMode:       ptr.To(corev1.PersistentVolumeFilesystem),
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Gi")},
				},
			},
		}

		pod = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "c", Image: "nginx"}},
				Volumes: []corev1.Volume{{
					Name: "data",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name},
					},
				}},
			},
		}

		pvca = &v1alpha1.PersistentVolumeClaimAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "test-pvca", Namespace: "default"},
			Spec: v1alpha1.PersistentVolumeClaimAutoscalerSpec{
				TargetRef: autoscalingv1.CrossVersionObjectReference{
					Kind:       "StatefulSet",
					Name:       "test-sts",
					APIVersion: "apps/v1",
				},
				VolumePolicies: []v1alpha1.VolumePolicy{{
					Match:       v1alpha1.Match{Name: "*"},
					MaxCapacity: resource.MustParse("100Gi"),
					ScaleUp:     &v1alpha1.ScalingRules{ResizeStrategy: v1alpha1.PreferInPlaceVolumeResizeStrategy},
				}},
			},
		}

		Expect(k8sClient.Create(ctx, pvc)).To(Succeed())
		DeferCleanup(func() {
			Expect(testutils.CleanupObject(ctx, k8sClient, pvc)).To(Succeed())
		})

		Eventually(func() error {
			return mgrClient.Get(ctx, client.ObjectKeyFromObject(pvc), &corev1.PersistentVolumeClaim{})
		}).Should(Succeed())
	})

	JustBeforeEach(func() {
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(func() {
			Expect(testutils.CleanupObject(ctx, k8sClient, pod)).To(Succeed())
		})

		Eventually(func() error {
			return mgrClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
		}).Should(Succeed())

		setPodPhase(corev1.PodRunning)
	})

	Context("PVC managed by this autoscaler instance", func() {
		BeforeEach(func() {
			Expect(k8sClient.Create(ctx, pvca)).To(Succeed())
			DeferCleanup(func() {
				Expect(testutils.CleanupObject(ctx, k8sClient, pvca)).To(Succeed())
			})

			patch := client.MergeFrom(pvca.DeepCopy())
			pvca.Status.VolumeRecommendations = []v1alpha1.VolumeRecommendation{{Name: pvc.Name}}
			Expect(k8sClient.Status().Patch(ctx, pvca, patch)).To(Succeed())

			Eventually(func(g Gomega) {
				observed := &v1alpha1.PersistentVolumeClaimAutoscaler{}
				g.Expect(mgrClient.Get(ctx, client.ObjectKeyFromObject(pvca), observed)).To(Succeed())
				g.Expect(observed.Status.VolumeRecommendations).To(HaveLen(1))
			}).Should(Succeed())
		})

		When("the PVC has the ControllerResizeError condition", func() {
			BeforeEach(func() {
				setPVCConditions(corev1.PersistentVolumeClaimControllerResizeError)
			})

			It("should evict the pod using the PVC", func() {
				result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{}))

				Eventually(func() bool {
					return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{}))
				}).Should(BeTrue())
			})

			When("the pod is not running", func() {
				JustBeforeEach(func() {
					setPodPhase(corev1.PodPending)
				})

				It("should not evict the pod", func() {
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
					Expect(err).NotTo(HaveOccurred())
					Expect(result).To(Equal(reconcile.Result{}))

					Consistently(func() error {
						return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
					}).Should(Succeed())
				})
			})

			When("the resize strategy is not PreferInPlace", func() {
				setStrategy := func(strategy v1alpha1.VolumeResizeStrategy) {
					GinkgoHelper()
					patch := client.MergeFrom(pvca.DeepCopy())
					pvca.Spec.VolumePolicies[0].ScaleUp.ResizeStrategy = strategy
					Expect(k8sClient.Patch(ctx, pvca, patch)).To(Succeed())
					Eventually(func(g Gomega) {
						observed := &v1alpha1.PersistentVolumeClaimAutoscaler{}
						g.Expect(mgrClient.Get(ctx, client.ObjectKeyFromObject(pvca), observed)).To(Succeed())
						g.Expect(observed.Spec.VolumePolicies[0].ScaleUp.ResizeStrategy).To(Equal(strategy))
					}).Should(Succeed())
				}

				When("the strategy is InPlace", func() {
					BeforeEach(func() {
						setStrategy(v1alpha1.InPlaceVolumeResizeStrategy)
					})

					It("should not evict the pod", func() {
						result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
						Expect(err).NotTo(HaveOccurred())
						Expect(result).To(Equal(reconcile.Result{}))

						Consistently(func() error {
							return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
						}).Should(Succeed())
					})
				})

				When("the strategy is Off", func() {
					BeforeEach(func() {
						setStrategy(v1alpha1.OffVolumeResizeStrategy)
					})

					It("should not evict the pod", func() {
						result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
						Expect(err).NotTo(HaveOccurred())
						Expect(result).To(Equal(reconcile.Result{}))

						Consistently(func() error {
							return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
						}).Should(Succeed())
					})
				})
			})

			When("the PVCA targets a PersistentVolumeClaim directly", func() {
				BeforeEach(func() {
					patch := client.MergeFrom(pvca.DeepCopy())
					pvca.Spec.TargetRef = autoscalingv1.CrossVersionObjectReference{
						Kind:       "PersistentVolumeClaim",
						Name:       pvc.Name,
						APIVersion: "v1",
					}
					Expect(k8sClient.Patch(ctx, pvca, patch)).To(Succeed())
					Eventually(func(g Gomega) {
						observed := &v1alpha1.PersistentVolumeClaimAutoscaler{}
						g.Expect(mgrClient.Get(ctx, client.ObjectKeyFromObject(pvca), observed)).To(Succeed())
						g.Expect(observed.Spec.TargetRef.Kind).To(Equal("PersistentVolumeClaim"))
					}).Should(Succeed())
				})

				It("should not evict the pod", func() {
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
					Expect(err).NotTo(HaveOccurred())
					Expect(result).To(Equal(reconcile.Result{}))

					Consistently(func() error {
						return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
					}).Should(Succeed())
				})
			})

		})

		When("the PVC has no ControllerResizeError condition", func() {
			It("should not evict the pod", func() {
				result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{}))

				Consistently(func() error {
					return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
				}).Should(Succeed())
			})
		})

		When("the PVC has the FileSystemResizePending condition", func() {
			BeforeEach(func() {
				setPVCConditions(corev1.PersistentVolumeClaimFileSystemResizePending)
			})

			When("the pod carries the offline-resize scheduling gate", func() {
				BeforeEach(func() {
					pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: common.SchedulingGateOfflineResize}}
				})

				It("should remove the offline-resize scheduling gate", func() {
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
					Expect(err).NotTo(HaveOccurred())
					Expect(result).To(Equal(reconcile.Result{}))

					Eventually(func(g Gomega) {
						updated := &corev1.Pod{}
						g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), updated)).To(Succeed())
						g.Expect(updated.Spec.SchedulingGates).To(BeEmpty())
					}).Should(Succeed())
				})

				When("the resize strategy has since been flipped to InPlace", func() {
					BeforeEach(func() {
						patch := client.MergeFrom(pvca.DeepCopy())
						pvca.Spec.VolumePolicies[0].ScaleUp.ResizeStrategy = v1alpha1.InPlaceVolumeResizeStrategy
						Expect(k8sClient.Patch(ctx, pvca, patch)).To(Succeed())
						Eventually(func(g Gomega) {
							observed := &v1alpha1.PersistentVolumeClaimAutoscaler{}
							g.Expect(mgrClient.Get(ctx, client.ObjectKeyFromObject(pvca), observed)).To(Succeed())
							g.Expect(observed.Spec.VolumePolicies[0].ScaleUp.ResizeStrategy).To(Equal(v1alpha1.InPlaceVolumeResizeStrategy))
						}).Should(Succeed())
					})

					It("should remove the offline-resize scheduling gate", func() {
						result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
						Expect(err).NotTo(HaveOccurred())
						Expect(result).To(Equal(reconcile.Result{}))

						Eventually(func(g Gomega) {
							updated := &corev1.Pod{}
							g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), updated)).To(Succeed())
							g.Expect(updated.Spec.SchedulingGates).To(BeEmpty())
						}).Should(Succeed())
					})
				})
			})

			When("the pod has no scheduling gate", func() {
				It("should leave the pod untouched", func() {
					result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
					Expect(err).NotTo(HaveOccurred())
					Expect(result).To(Equal(reconcile.Result{}))

					updated := &corev1.Pod{}
					Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), updated)).To(Succeed())
					Expect(updated.Spec.SchedulingGates).To(BeEmpty())
				})
			})
		})

		When("the PVC has both ControllerResizeError and FileSystemResizePending", func() {
			BeforeEach(func() {
				pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: common.SchedulingGateOfflineResize}}

				setPVCConditions(
					corev1.PersistentVolumeClaimControllerResizeError,
					corev1.PersistentVolumeClaimFileSystemResizePending,
				)
			})

			It("should remove the gate and not evict the pod", func() {
				result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{}))

				Eventually(func(g Gomega) {
					updated := &corev1.Pod{}
					g.Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), updated)).To(Succeed())
					g.Expect(updated.Spec.SchedulingGates).To(BeEmpty())
					g.Expect(updated.DeletionTimestamp).To(BeNil())
				}).Should(Succeed())
			})
		})
	})

	Context("PVC not managed by any PVCA of this instance", func() {
		When("the PVC has the ControllerResizeError condition", func() {
			BeforeEach(func() {
				setPVCConditions(corev1.PersistentVolumeClaimControllerResizeError)
			})

			It("should not evict the pod", func() {
				result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(pvc)})
				Expect(err).NotTo(HaveOccurred())
				Expect(result).To(Equal(reconcile.Result{}))

				Consistently(func() error {
					return k8sClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})
				}).Should(Succeed())
			})
		})
	})

	Context("PVC no longer exists", func() {
		It("should do nothing", func() {
			result, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: client.ObjectKey{Namespace: "default", Name: "does-not-exist"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})
	})
})
