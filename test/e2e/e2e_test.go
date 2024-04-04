/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/gardener/pvc-autoscaler/test/utils"
)

const (
	// namespace to use during the e2e tests
	namespace = "pvc-autoscaler-system-e2e"

	// projectImage stores the name of the image used in the example
	projectImage = "example.com/pvc-autoscaler:v0.0.1"
)

var _ = Describe("controller", Ordered, func() {
	Context("Operator", func() {
		It("should start successfully", func() {
			By("building the manager(Operator) image")
			err := utils.Make("docker-build", fmt.Sprintf("IMG=%s", projectImage))
			Expect(err).NotTo(HaveOccurred())

			By("loading the manager(Operator) image in Minikube")
			err = utils.LoadImageToMinikubeProfileWithName(projectImage)
			Expect(err).NotTo(HaveOccurred())

			By("deploying the controller-manager")
			err = utils.Make("deploy-e2e", fmt.Sprintf("IMG=%s", projectImage))
			Expect(err).NotTo(HaveOccurred())

			By("validate that the controller-manager is up and running")
			err = utils.Kubectl("wait", "deployment.apps/pvc-autoscaler-controller-manager",
				"--for", "condition=Available",
				"--namespace", namespace,
				"--timeout", "5m",
			)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
