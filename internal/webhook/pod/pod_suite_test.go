// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package pod_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/gardener/pvc-autoscaler/api/autoscaling/v1alpha1"
)

func TestPodWebhook(t *testing.T) {
	RegisterFailHandler(Fail)
	Expect(v1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())
	RunSpecs(t, "Pod Webhook Suite")
}
