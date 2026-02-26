// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package pvcfetcher_test

import (
	"context"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/target/pvcfetcher"
)

var _ = Describe("PVCFetcher", func() {
	var (
		ctx context.Context

		fakeClient      client.Client
		selectorFetcher *fakeSelectorFetcher
	)

	BeforeEach(func() {
		ctx = context.Background()

		scheme := runtime.NewScheme()
		Expect(corev1.AddToScheme(scheme)).To(Succeed())
		Expect(v1alpha1.AddToScheme(scheme)).To(Succeed())

		fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()
		selectorFetcher = &fakeSelectorFetcher{}
	})

	Describe("New", func() {
		It("should return an error when no client is provided", func() {
			_, err := pvcfetcher.New(
				pvcfetcher.WithSelectorFetcher(selectorFetcher),
			)
			Expect(err).To(Equal(pvcfetcher.ErrNoClient))
		})

		It("should return an error when no selector fetcher is provided", func() {
			_, err := pvcfetcher.New(
				pvcfetcher.WithClient(fakeClient),
			)
			Expect(err).To(Equal(pvcfetcher.ErrNoSelectorFetcher))
		})

		It("should create a fetcher when all required options are provided", func() {
			fetcher, err := pvcfetcher.New(
				pvcfetcher.WithClient(fakeClient),
				pvcfetcher.WithSelectorFetcher(selectorFetcher),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(fetcher).ToNot(BeNil())
		})
	})

	Describe("Fetch", func() {
		var (
			fetcher pvcfetcher.Fetcher

			pvca *v1alpha1.PersistentVolumeClaimAutoscaler
		)

		BeforeEach(func() {
			selector, err := labels.Parse("app=test")
			Expect(err).NotTo(HaveOccurred())

			selectorFetcher.selector = selector

			fetcher, err = pvcfetcher.New(
				pvcfetcher.WithClient(fakeClient),
				pvcfetcher.WithSelectorFetcher(selectorFetcher),
			)
			Expect(err).ToNot(HaveOccurred())

			pvca = &v1alpha1.PersistentVolumeClaimAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pvca",
					Namespace: "default",
				},
				Spec: v1alpha1.PersistentVolumeClaimAutoscalerSpec{
					TargetRef: autoscalingv1.CrossVersionObjectReference{
						Kind:       "StatefulSet",
						Name:       "test-sts",
						APIVersion: "apps/v1",
					},
				},
			}
		})

		It("should return an error when selector fetcher fails", func() {
			selectorFetcher.err = errors.New("scale not found")

			_, err := fetcher.Fetch(ctx, pvca)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to fetch selector"))
		})

		It("should return empty list when no pods match the selector", func() {
			selectorFetcher.selector, _ = labels.Parse("app=test")

			pvcs, err := fetcher.Fetch(ctx, pvca)
			Expect(err).ToNot(HaveOccurred())
			Expect(pvcs).To(BeEmpty())
		})

		It("should return empty list when pods have no PVC volumes", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "test-config",
									},
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pod)).To(Succeed())

			pvcs, err := fetcher.Fetch(ctx, pvca)
			Expect(err).ToNot(HaveOccurred())
			Expect(pvcs).To(BeEmpty())
		})

		It("should return PVCs from pods with PVC volumes", func() {
			pod1 := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "data-test-pod-0",
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pod1)).To(Succeed())

			pod2 := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-1",
					Namespace: "default",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "data-test-pod-1",
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pod2)).To(Succeed())

			pvc1 := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "data-test-pod-0",
					Namespace: "default",
				},
			}
			Expect(fakeClient.Create(ctx, pvc1)).To(Succeed())

			pvc2 := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "data-test-pod-1",
					Namespace: "default",
				},
			}
			Expect(fakeClient.Create(ctx, pvc2)).To(Succeed())

			pvcs, err := fetcher.Fetch(ctx, pvca)
			Expect(err).ToNot(HaveOccurred())
			Expect(pvcs).To(HaveLen(2))
			Expect(pvcs).To(ConsistOf(HaveField("Name", "data-test-pod-0"), HaveField("Name", "data-test-pod-1")))
		})

		It("should deduplicate PVCs when multiple pods reference the same PVC", func() {
			sharedPVC := "shared-data"

			pod1 := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-0",
					Namespace: "default",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: sharedPVC,
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pod1)).To(Succeed())

			pod2 := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-1",
					Namespace: "default",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: sharedPVC,
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pod2)).To(Succeed())

			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      sharedPVC,
					Namespace: "default",
				},
			}
			Expect(fakeClient.Create(ctx, pvc)).To(Succeed())

			pvcs, err := fetcher.Fetch(ctx, pvca)
			Expect(err).ToNot(HaveOccurred())
			Expect(pvcs).To(HaveLen(1))
			Expect(pvcs[0].Name).To(Equal(sharedPVC))
		})

		It("should return an error when a referenced PVC does not exist", func() {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "missing-pvc",
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pod)).To(Succeed())

			_, err := fetcher.Fetch(ctx, pvca)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("failed to get PersistentVolumeClaim"))
		})

		It("should only return PVCs from pods in the same namespace as the PVCA", func() {
			podInNamespace := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-ns1",
					Namespace: "default",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc-in-default",
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, podInNamespace)).To(Succeed())

			podInOtherNamespace := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-ns2",
					Namespace: "other",
					Labels: map[string]string{
						"app": "test",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
					Volumes: []corev1.Volume{
						{
							Name: "data",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: "pvc-in-other",
								},
							},
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, podInOtherNamespace)).To(Succeed())

			pvcInDefault := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-in-default",
					Namespace: "default",
				},
			}
			Expect(fakeClient.Create(ctx, pvcInDefault)).To(Succeed())

			pvcInOther := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pvc-in-other",
					Namespace: "other",
				},
			}
			Expect(fakeClient.Create(ctx, pvcInOther)).To(Succeed())

			pvcs, err := fetcher.Fetch(ctx, pvca)
			Expect(err).ToNot(HaveOccurred())
			Expect(pvcs).To(HaveLen(1))
			Expect(pvcs[0].Name).To(Equal("pvc-in-default"))
			Expect(pvcs[0].Namespace).To(Equal("default"))
		})
	})
})

type fakeSelectorFetcher struct {
	selector labels.Selector
	err      error
}

func (s *fakeSelectorFetcher) Fetch(ctx context.Context, namespace string, targetRef autoscalingv1.CrossVersionObjectReference) (labels.Selector, error) {
	if s.err != nil {
		return nil, s.err
	}

	return s.selector, nil
}
