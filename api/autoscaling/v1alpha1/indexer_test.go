// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fakeFieldIndexer captures the field key and extractor function passed to IndexField
// so the extractor can be exercised directly, without a cache.
type fakeFieldIndexer struct {
	field   string
	extract client.IndexerFunc
}

func (f *fakeFieldIndexer) IndexField(_ context.Context, _ client.Object, field string, extractValue client.IndexerFunc) error {
	f.field = field
	f.extract = extractValue

	return nil
}

var _ = Describe("Indexer", func() {
	var fake *fakeFieldIndexer

	BeforeEach(func() {
		fake = &fakeFieldIndexer{}
	})

	Describe("#AddAutoscalerNameFieldIndexer", func() {
		BeforeEach(func() {
			Expect(AddAutoscalerNameFieldIndexer(context.Background(), fake)).To(Succeed())
		})

		It("should register the index under the autoscaler name key", func() {
			Expect(fake.field).To(Equal(AutoscalerNameIndexKey))
		})

		DescribeTable("should extract the expected index values",
			func(obj client.Object, expected []string) {
				Expect(fake.extract(obj)).To(Equal(expected))
			},
			Entry("returns the autoscaler name",
				&PersistentVolumeClaimAutoscaler{Spec: PersistentVolumeClaimAutoscalerSpec{AutoscalerName: "foo"}},
				[]string{"foo"}),
			Entry("indexes an empty autoscaler name under the empty key",
				&PersistentVolumeClaimAutoscaler{},
				[]string{""}),
			Entry("returns nil for a non-PVCA object",
				&corev1.PersistentVolumeClaim{},
				nil),
		)
	})

	Describe("#AddVolumeRecommendationFieldIndexer", func() {
		BeforeEach(func() {
			Expect(AddVolumeRecommendationFieldIndexer(context.Background(), fake)).To(Succeed())
		})

		It("should register the index under the volume recommendation key", func() {
			Expect(fake.field).To(Equal(VolumeRecommendationIndexKey))
		})

		DescribeTable("should extract the expected index values",
			func(obj client.Object, expected []string) {
				Expect(fake.extract(obj)).To(Equal(expected))
			},
			Entry("returns the name of every volume recommendation in order",
				&PersistentVolumeClaimAutoscaler{Status: PersistentVolumeClaimAutoscalerStatus{
					VolumeRecommendations: []VolumeRecommendation{{Name: "pvc-a"}, {Name: "pvc-b"}},
				}},
				[]string{"pvc-a", "pvc-b"}),
			Entry("returns an empty slice when there are no recommendations",
				&PersistentVolumeClaimAutoscaler{},
				[]string{}),
			Entry("returns nil for a non-PVCA object",
				&corev1.PersistentVolumeClaim{},
				nil),
		)
	})
})
