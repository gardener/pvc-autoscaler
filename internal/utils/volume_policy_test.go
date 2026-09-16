// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package utils_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
	"github.com/gardener/pvc-autoscaler/internal/utils"
)

var _ = Describe("Volume Policy", func() {
	Describe("#GetVolumePolicy", func() {
		It("should return error on invalid glob pattern", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "[",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid volume policy name \"[\""))
			Expect(policy).To(BeNil())
		})

		It("should return nil when no policy matches", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "other-pvc",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).To(BeNil())
		})

		It("should match exact name", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "data-pvc",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("data-pvc"))
		})

		It("should match glob pattern", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "*-logs",
					},
					MaxCapacity: resource.MustParse("15Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("app-logs", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*-logs"))
		})

		It("should match default policy", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*"))
		})

		It("should fall back to default policy when no other policy matches", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "other-pvc",
					},
					MaxCapacity: resource.MustParse("20Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("*"))
			Expect(policy.MaxCapacity).To(Equal(resource.MustParse("5Gi")))
		})

		It("should return first seen matching policy", func() {
			volumePolicies := []v1alpha1.VolumePolicy{
				{
					Match: v1alpha1.Match{
						Name: "data-*",
					},
					MaxCapacity: resource.MustParse("10Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "data-pvc",
					},
					MaxCapacity: resource.MustParse("20Gi"),
				},
				{
					Match: v1alpha1.Match{
						Name: "*",
					},
					MaxCapacity: resource.MustParse("5Gi"),
				},
			}

			policy, err := utils.GetVolumePolicy("data-pvc", volumePolicies)
			Expect(err).NotTo(HaveOccurred())
			Expect(policy).NotTo(BeNil())
			Expect(policy.Match.Name).To(Equal("data-*"))
			Expect(policy.MaxCapacity).To(Equal(resource.MustParse("10Gi")))
		})
	})
})
