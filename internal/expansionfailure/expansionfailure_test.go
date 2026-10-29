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
	"github.com/gardener/pvc-autoscaler/internal/expansionfailure"
)

var _ = Describe("Expansion Failure", func() {
	Describe("#ShouldRecover", func() {
		pvcWithStatus := func(status corev1.ClaimResourceStatus) *corev1.PersistentVolumeClaim {
			return &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default"},
				Status: corev1.PersistentVolumeClaimStatus{
					AllocatedResourceStatuses: map[corev1.ResourceName]corev1.ClaimResourceStatus{
						corev1.ResourceStorage: status,
					},
				},
			}
		}

		pvcWithResizeError := func(conditionType corev1.PersistentVolumeClaimConditionType, status corev1.ConditionStatus, message string) *corev1.PersistentVolumeClaim {
			return &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default"},
				Status: corev1.PersistentVolumeClaimStatus{
					Conditions: []corev1.PersistentVolumeClaimCondition{
						{
							Type:    conditionType,
							Status:  status,
							Message: message,
						},
					},
				},
			}
		}

		It("returns false for an empty PVC", func() {
			Expect(expansionfailure.ShouldRecover(&corev1.PersistentVolumeClaim{})).To(BeFalse())
		})

		It("returns false when the resize is making progress and no error is reported", func() {
			Expect(expansionfailure.ShouldRecover(pvcWithStatus(corev1.PersistentVolumeClaimControllerResizeInProgress))).To(BeFalse())
		})

		DescribeTable("returns true when the CSI driver reports the resize as infeasible",
			func(status corev1.ClaimResourceStatus) {
				Expect(expansionfailure.ShouldRecover(pvcWithStatus(status))).To(BeTrue())
			},
			Entry("ControllerResizeInfeasible", corev1.PersistentVolumeClaimControllerResizeInfeasible),
			Entry("NodeResizeInfeasible", corev1.PersistentVolumeClaimNodeResizeInfeasible),
		)

		It("returns false when the resize error condition is not true", func() {
			Expect(expansionfailure.ShouldRecover(pvcWithResizeError(corev1.PersistentVolumeClaimControllerResizeError, corev1.ConditionFalse, "requested size 100Gi is too large for volume type gp3"))).To(BeFalse())
		})

		It("returns false when the resize error message is not a known provider resize-limit error", func() {
			Expect(expansionfailure.ShouldRecover(pvcWithResizeError(corev1.PersistentVolumeClaimControllerResizeError, corev1.ConditionTrue, "rpc error: code = DeadlineExceeded desc = context deadline exceeded"))).To(BeFalse())
		})

		DescribeTable("returns true for a known provider resize-limit error on the ControllerResizeError condition (embedded values and casing must not matter)",
			func(message string) {
				Expect(expansionfailure.ShouldRecover(pvcWithResizeError(corev1.PersistentVolumeClaimControllerResizeError, corev1.ConditionTrue, message))).To(BeTrue())
			},
			Entry("Azure", `disk.csi.azure.com: requested size 100Gi is out of range. The value must be between 1Gi and 64Gi`),
			Entry("AWS", `ebs.csi.aws.com: requested size 20000Gi is too large for volume type gp3`),
			Entry("GCP", `googleapi: Error 400: requested size 70000Gi Must be less than or equal to 65536Gi, invalidArgument`),
			Entry("OpenStack VolumeSizeExceedsLimit", `rpc error: code = OutOfRange desc = VolumeSizeExceedsLimit: requested 5Ti`),
			Entry("OpenStack larger than maximum", `requested size 5Ti is larger than maximum allowed limit 1Ti`),
			Entry("case-insensitive match", `REQUESTED SIZE IS TOO LARGE FOR VOLUME TYPE gp3`),
		)
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
			func(failed, capacity, expectedTarget string, status corev1.ClaimResourceStatus, expectRecovery bool) {
				pvc := &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{Name: "sample-pvc", Namespace: "default"},
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
				Expect(updatedPvc.Spec.Resources.Requests.Storage().String()).To(Equal(expectedTarget))

				Expect(updatedRecommendation.Target.Size).NotTo(BeNil())
				Expect(updatedRecommendation.Target.Size.String()).To(Equal(expectedTarget))
				Expect(updatedRecommendation.LastResizeTime).To(BeNil())

				Expect(resizingCondition).NotTo(BeNil())
				Expect(*resizingCondition).To(And(
					HaveField("Type", string(v1alpha1.ConditionTypeResizing)),
					HaveField("Status", metav1.ConditionTrue),
					HaveField("Reason", expansionfailure.ReasonResizeFailureRecovery),
					HaveField("Message", MatchRegexp(`reduced requested storage from `+failed+` to `+expectedTarget+` after failed resize \(`+string(status)+`\)`)),
				))
			},
			Entry("half-step reduction (ControllerResizeInfeasible)", "5Gi", "1Gi", "3Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
			Entry("half-step reduction (NodeResizeInfeasible)", "5Gi", "1Gi", "3Gi", corev1.PersistentVolumeClaimNodeResizeInfeasible, true),
			Entry("larger half-step reduction", "7Gi", "1Gi", "4Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
			Entry("odd failed step rounds the half-step up to the next 1Gi", "4Gi", "1Gi", "3Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
			Entry("failed step of 2Gi reduces by the minimum 1Gi step", "3Gi", "1Gi", "2Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, true),
			// Cases that do nothing (no feasible smaller size):
			Entry("no-op: request is only one step above capacity", "2Gi", "1Gi", "2Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, false),
			Entry("no-op: request already equals capacity", "1Gi", "1Gi", "1Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, false),
			Entry("no-op: capacity is not reported (zero)", "5Gi", "0", "5Gi", corev1.PersistentVolumeClaimControllerResizeInfeasible, false),
		)
	})
})
