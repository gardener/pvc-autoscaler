// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package expansionfailure_test

import (
	"context"
	"io"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/expansionfailure"
)

var _ = Describe("Expansion Failure", func() {
	Describe("#IsResizeInfeasible", func() {
		newPVC := func(status corev1.ClaimResourceStatus) *corev1.PersistentVolumeClaim {
			return &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default"},
				Status: corev1.PersistentVolumeClaimStatus{
					AllocatedResourceStatuses: map[corev1.ResourceName]corev1.ClaimResourceStatus{
						corev1.ResourceStorage: status,
					},
				},
			}
		}

		It("returns false when the allocated resource statuses map is empty", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default"},
			}
			Expect(expansionfailure.IsResizeInfeasible(pvc)).To(BeFalse())
		})

		It("returns false when the storage status indicates progress", func() {
			Expect(expansionfailure.IsResizeInfeasible(newPVC(corev1.PersistentVolumeClaimControllerResizeInProgress))).To(BeFalse())
			Expect(expansionfailure.IsResizeInfeasible(newPVC(corev1.PersistentVolumeClaimNodeResizeInProgress))).To(BeFalse())
			Expect(expansionfailure.IsResizeInfeasible(newPVC(corev1.PersistentVolumeClaimNodeResizePending))).To(BeFalse())
		})

		It("returns true for ControllerResizeInfeasible", func() {
			Expect(expansionfailure.IsResizeInfeasible(newPVC(corev1.PersistentVolumeClaimControllerResizeInfeasible))).To(BeTrue())
		})

		It("returns true for NodeResizeInfeasible", func() {
			Expect(expansionfailure.IsResizeInfeasible(newPVC(corev1.PersistentVolumeClaimNodeResizeInfeasible))).To(BeTrue())
		})
	})

	Describe("#Recover", func() {
		var (
			ctx        context.Context
			testScheme *runtime.Scheme
			recorder   *record.FakeRecorder
		)

		BeforeEach(func() {
			ctx = context.Background()
			testScheme = runtime.NewScheme()
			Expect(corev1.AddToScheme(testScheme)).To(Succeed())
			recorder = record.NewFakeRecorder(1024)
		})

		DescribeTable("should reduce requested storage after an infeasible resize",
			func(failed, capacity string, annotationValue *string, expectedTarget string, status corev1.ClaimResourceStatus, expectRecovery bool) {
				annotations := map[string]string{}
				if annotationValue != nil {
					annotations[common.AnnotationPreviousSize] = *annotationValue
				}
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default", Annotations: annotations},
					Spec: corev1.PersistentVolumeClaimSpec{
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(failed)},
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{
						Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(capacity)},
						AllocatedResourceStatuses: map[corev1.ResourceName]corev1.ClaimResourceStatus{
							corev1.ResourceStorage: status,
						},
					},
				}

				c := fake.NewClientBuilder().
					WithScheme(testScheme).
					WithObjects(pvc).
					WithStatusSubresource(&corev1.PersistentVolumeClaim{}).
					Build()

				// Fetch a fresh copy so the object carries the resourceVersion the
				// store expects for the optimistic-lock patch.
				Expect(c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)).To(Succeed())

				var buf strings.Builder
				logger := zap.New(zap.WriteTo(io.MultiWriter(GinkgoWriter, &buf))).WithValues("pvc", pvc.Name)

				recoverer := expansionfailure.New(c, recorder)
				volumeRecommendation := v1alpha1.VolumeRecommendation{Name: pvc.Name}
				updatedRecommendation, resizingCondition, err := recoverer.Recover(ctx, logger, pvc, volumeRecommendation)
				Expect(err).NotTo(HaveOccurred())

				if !expectRecovery {
					Expect(buf.String()).NotTo(ContainSubstring("recovering from failed pvc resize"))
					Expect(resizingCondition).To(BeNil())
					Expect(updatedRecommendation).To(Equal(volumeRecommendation))

					return
				}

				Expect(buf.String()).To(ContainSubstring("recovering from failed pvc resize"))

				By("Verifying PVC spec was reduced to the expected target")
				var updatedPvc corev1.PersistentVolumeClaim
				Expect(c.Get(ctx, client.ObjectKeyFromObject(pvc), &updatedPvc)).To(Succeed())
				Expect(updatedPvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse(expectedTarget)))

				Expect(updatedRecommendation.Target.Size).NotTo(BeNil())
				Expect(*updatedRecommendation.Target.Size).To(Equal(resource.MustParse(expectedTarget)))
				Expect(updatedRecommendation.LastResizeTime).To(BeNil())

				Expect(resizingCondition).NotTo(BeNil())
				Expect(*resizingCondition).To(And(
					HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
					HaveField("Status", metav1.ConditionTrue),
					HaveField("Reason", expansionfailure.ReasonResizeFailureRecovery),
					HaveField("Message", MatchRegexp(`reduced requested storage from `+failed+` to `+expectedTarget+` after infeasible resize \(`+string(status)+`\)`)),
				))
			},
			Entry("should retry at a half-step (capacity baseline, ControllerResizeInfeasible)", "5Gi", "1Gi", nil, "3Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
			Entry("should retry at a half-step (capacity baseline, NodeResizeInfeasible)", "5Gi", "1Gi", nil, "3Gi", corev1.PersistentVolumeClaimNodeResizeInfeasible, true),
			Entry("should fall back to capacity when the half-step would meet or exceed the failed size", "2Gi", "1Gi", nil, "1Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
			Entry("should be a no-op when capacity already equals the requested size", "1Gi", "1Gi", nil, "1Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, false),
			Entry("should use the annotation as baseline when present", "5Gi", "900Mi", strPtr("1Gi"), "3Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
			Entry("should fall back to the annotation size when the half-step would meet or exceed the failed size", "2Gi", "900Mi", strPtr("1Gi"), "1Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
		)

		It("should return an error when the prev-size annotation cannot be parsed", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "sample-pvc",
					Namespace:   "default",
					Annotations: map[string]string{common.AnnotationPreviousSize: "not-a-quantity"},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("5Gi")},
					},
				},
				Status: corev1.PersistentVolumeClaimStatus{
					Capacity: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
					AllocatedResourceStatuses: map[corev1.ResourceName]corev1.ClaimResourceStatus{
						corev1.ResourceStorage: corev1.PersistentVolumeClaimControllerResizeInfeasible,
					},
				},
			}

			c := fake.NewClientBuilder().
				WithScheme(testScheme).
				WithObjects(pvc).
				Build()
			Expect(c.Get(ctx, client.ObjectKeyFromObject(pvc), pvc)).To(Succeed())

			logger := zap.New(zap.WriteTo(GinkgoWriter))
			recoverer := expansionfailure.New(c, recorder)
			_, _, err := recoverer.Recover(ctx, logger, pvc, v1alpha1.VolumeRecommendation{Name: pvc.Name})
			Expect(err).To(MatchError(ContainSubstring("could not parse " + common.AnnotationPreviousSize + " annotation with value not-a-quantity")))
		})
	})
})

func strPtr(s string) *string { return &s }
