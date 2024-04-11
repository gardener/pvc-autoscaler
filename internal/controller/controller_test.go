// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package controller_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/internal/common"
	"github.com/gardener/pvc-autoscaler/internal/controller"
)

var _ = Describe("PersistentVolumeClaim Controller", func() {
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
			reconciler, err := controller.New(
				controller.WithClient(k8sClient),
				controller.WithScheme(k8sClient.Scheme()),
				controller.WithEventChannel(eventCh),
				controller.WithEventRecorder(eventRecorder),
			)
			Expect(err).NotTo(HaveOccurred())
			Expect(reconciler).NotTo(BeNil())
		})

	})

	Context("When reconciling a resource", func() {
		It("should successfully reconcile the resource", func() {

		})
	})
})
