// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package fake_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/metrics/source/fake"

	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("Fake", func() {
	Context("Item", func() {
		It("should consume capacity", func() {
			item := &fake.Item{
				NamespacedName: types.NamespacedName{
					Name:      "sample-pvc",
					Namespace: "default",
				},
				CapacityBytes:          1000,
				AvailableBytes:         1000,
				CapacityInodes:         2000,
				AvailableInodes:        2000,
				ConsumeBytesIncrement:  100,
				ConsumeInodesIncrement: 200,
			}
			item.Consume()
			Expect(item.AvailableBytes).To(Equal(900))
			Expect(item.AvailableInodes).To(Equal(1800))

			// "consume" all available space and inodes, the
			// available inodes and space should never drop below
			// zero.
			for range 1000 {
				item.Consume()
			}

			Expect(item.AvailableBytes).To(Equal(0))
			Expect(item.AvailableInodes).To(Equal(0))
			Expect(item.CapacityBytes).To(Equal(1000))
			Expect(item.CapacityInodes).To(Equal(2000))
		})
	})

	Context("Create new fake.Fake instance", func() {
		It("should create instance successfully", func() {
			f := fake.New(fake.WithInterval(time.Second))
			Expect(f).NotTo(BeNil())
		})

		It("register items and consume", func() {
			key := types.NamespacedName{
				Name:      "sample-pvc",
				Namespace: "default",
			}
			item := &fake.Item{
				NamespacedName:         key,
				CapacityBytes:          10000,
				AvailableBytes:         10000,
				CapacityInodes:         20000,
				AvailableInodes:        20000,
				ConsumeBytesIncrement:  100,
				ConsumeInodesIncrement: 200,
			}

			// A fast consumer
			f := fake.New(fake.WithInterval(10 * time.Millisecond))
			f.Register(item)

			// Initially the free bytes and inodes should be 100%,
			// since we haven't started the fake metrics source yet.
			ctx, cancelFunc := context.WithCancel(context.Background())
			result, err := f.Get(ctx)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			freeSpace, _ := result[key].FreeSpacePercentage()
			Expect(freeSpace).To(Equal(100.0))

			usedSpace, _ := result[key].UsedSpacePercentage()
			Expect(usedSpace).To(Equal(0.0))

			freeInodes, _ := result[key].FreeInodesPercentage()
			Expect(freeInodes).To(Equal(100.0))

			usedInodes, _ := result[key].UsedInodesPercentage()
			Expect(usedInodes).To(Equal(0.0))

			// Start the fake source and give it some time to
			// consume it all
			go func() {
				ch := time.After(time.Second)
				<-ch
				cancelFunc()
			}()
			f.Start(ctx)

			result, err = f.Get(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			freeSpace, _ = result[key].FreeSpacePercentage()
			Expect(freeSpace).To(Equal(0.0))

			usedSpace, _ = result[key].UsedSpacePercentage()
			Expect(usedSpace).To(Equal(100.0))

			freeInodes, _ = result[key].FreeInodesPercentage()
			Expect(freeInodes).To(Equal(0.0))

			usedInodes, _ = result[key].UsedInodesPercentage()
			Expect(usedInodes).To(Equal(100.0))
		})
	})

	Context("Create a new AlwaysFailing metrics source", func() {
		It("should always return an error", func() {
			s := &fake.AlwaysFailing{}
			ctx := context.Background()
			result, err := s.Get(ctx)
			Expect(err).To(MatchError(common.ErrNoMetrics))
			Expect(result).To(BeNil())
		})
	})
})
