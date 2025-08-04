// SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	metricssource "github.com/gardener/pvc-autoscaler/internal/metrics/source"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Source", func() {
	Context("# FreeSpace / UsedSpace / FreeInodes / UsedInodes", func() {
		It("should return valid percentage", func() {
			tests := []struct {
				capacity           int
				available          int
				wantFreePercentage float64
				wantUsedPercentage float64
			}{
				{capacity: 1000, available: 100, wantFreePercentage: 10.0, wantUsedPercentage: 90.0},
				{capacity: 1000, available: 200, wantFreePercentage: 20.0, wantUsedPercentage: 80.0},
				{capacity: 1000, available: 500, wantFreePercentage: 50.0, wantUsedPercentage: 50.0},
				{capacity: 1000, available: 0, wantFreePercentage: 0.0, wantUsedPercentage: 100.0},
			}
			for _, test := range tests {
				// Space
				vol := metricssource.VolumeInfo{
					CapacityBytes:   test.capacity,
					AvailableBytes:  test.available,
					CapacityInodes:  test.capacity,
					AvailableInodes: test.available,
				}
				freeSpace, err := vol.FreeSpacePercentage()
				Expect(err).NotTo(HaveOccurred())
				Expect(freeSpace).To(Equal(test.wantFreePercentage))

				usedSpace, err := vol.UsedSpacePercentage()
				Expect(err).NotTo(HaveOccurred())
				Expect(usedSpace).To(Equal(test.wantUsedPercentage))

				// Inodes
				freeInodes, err := vol.FreeInodesPercentage()
				Expect(err).NotTo(HaveOccurred())
				Expect(freeInodes).To(Equal(test.wantFreePercentage))

				usedInodes, err := vol.UsedInodesPercentage()
				Expect(err).NotTo(HaveOccurred())
				Expect(usedInodes).To(Equal(test.wantUsedPercentage))
			}
		})

		It("should return ErrCapacityIsZero", func() {
			tests := []struct {
				capacity  int
				available int
				wantErr   error
			}{
				{capacity: 0, available: 0, wantErr: metricssource.ErrCapacityIsZero},
				{capacity: 0, available: 0, wantErr: metricssource.ErrCapacityIsZero},
			}
			for _, test := range tests {
				// Space
				vol := metricssource.VolumeInfo{
					CapacityBytes:   test.capacity,
					AvailableBytes:  test.available,
					CapacityInodes:  test.capacity,
					AvailableInodes: test.available,
				}
				_, err := vol.FreeSpacePercentage()
				Expect(err).To(MatchError(test.wantErr))

				_, err = vol.UsedSpacePercentage()
				Expect(err).To(MatchError(test.wantErr))

				_, err = vol.FreeInodesPercentage()
				Expect(err).To(MatchError(test.wantErr))

				_, err = vol.UsedInodesPercentage()
				Expect(err).To(MatchError(test.wantErr))
			}
		})
	})
})
