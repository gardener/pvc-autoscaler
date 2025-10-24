# Image URL to use all building/pushing image targets
IMG ?= europe-docker.pkg.dev/gardener-project/releases/gardener/pvc-autoscaler

VERSION := $(shell cat VERSION)
EFFECTIVE_VERSION ?= $(VERSION)-$(shell git rev-parse --short HEAD)

ifneq ($(strip $(shell git status --porcelain 2>/dev/null)),)
	EFFECTIVE_VERSION := $(EFFECTIVE_VERSION)-dirty
endif

IMAGE_TAG ?= $(EFFECTIVE_VERSION)

# ENVTEST_K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
ENVTEST_K8S_VERSION = 1.31.0

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

REPO_ROOT                         := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
KIND_KUBECONFIG                   := $(REPO_ROOT)/example/kind/local/kubeconfig
DEV_SETUP_WITH_LPP_RESIZE_SUPPORT ?= true

## Rules
kind-up kind-down pvc-autoscaler-up pvc-autoscaler-dev: export KUBECONFIG = $(KIND_KUBECONFIG)

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen  ## Generate WebhookConfiguration, ClusterRole and CRD objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt:  ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet:  ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet envtest  ## Run tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" \
		go test -v -coverprofile cover.out $$(go list ./... | grep -v -E 'test/e2e|test/utils|/cmd')

.PHONY: test-e2e  # Run the e2e tests against a minikube k8s instance that is spun up.
test-e2e:
	$(MAKE) e2e-env-setup
	./hack/run-e2e-tests.sh
	$(MAKE) e2e-env-teardown

.PHONY: lint
lint: golangci-lint  ## Run golangci-lint linter & yamllint
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: kind-up
kind-up: kind skaffold kustomize kubectl
	./hack/kind-up.sh \
	--with-lpp-resize-support $(DEV_SETUP_WITH_LPP_RESIZE_SUPPORT) \

.PHONY: kind-down
kind-down: kind skaffold kustomize kubectl
	$(KIND) delete cluster --name pvc-autoscaler

.PHONY: pvc-autoscaler-up
pvc-autoscaler-up: kind skaffold kustomize kubectl
	$(SKAFFOLD) run 

.PHONY: pvc-autoscaler-dev
pvc-autoscaler-dev: kind skaffold kustomize kubectl
	$(SKAFFOLD) dev

.PHONY: minikube-start
minikube-start: minikube yq  ## Start a local dev environment
	env MINIKUBE_PROFILE=$(MINIKUBE_PROFILE) ./hack/minikube-start.sh

.PHONY: minikube-stop
minikube-stop: minikube  ## Stop the local dev environment
	$(MINIKUBE) delete --profile $(MINIKUBE_PROFILE)

.PHONY: minikube-load-image
minikube-load-image: minikube docker-build  ## Load the operator image into the minikube nodes
	$(CONTAINER_TOOL) image save -o image.tar ${IMG}:${IMAGE_TAG}
	$(MINIKUBE) image load --overwrite=true image.tar
	rm -f image.tar

.PHONY: e2e-env-setup
e2e-env-setup: minikube  ## Create a new e2e test environment.
	$(MAKE) minikube-start minikube-load-image deploy

.PHONY: e2e-env-teardown
e2e-env-teardown: minikube  ## Teardown the e2e test environment.
	$(MAKE) minikube-stop

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG}:${IMAGE_TAG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}:${IMAGE_TAG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name pvc-autoscaler-builder
	$(CONTAINER_TOOL) buildx use pvc-autoscaler-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG}:${IMAGE_TAG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm pvc-autoscaler-builder
	rm Dockerfile.cross

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	echo "---" > dist/install.yaml  # Add a document separator before appending
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}:latest
	$(KUSTOMIZE) build config/default >> dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}:${IMAGE_TAG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
export PATH := $(abspath $(LOCALBIN)):$(PATH)
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint
MINIKUBE ?= $(LOCALBIN)/minikube
YQ ?= $(LOCALBIN)/yq
HELM ?= $(LOCALBIN)/helm
KIND ?= $(LOCALBIN)/kind
SKAFFOLD ?= $(LOCALBIN)/skaffold
KUBECTL ?= $(LOCALBIN)/kubectl

## Tool Versions
KUSTOMIZE_VERSION ?= v5.5.0
CONTROLLER_TOOLS_VERSION ?= v0.16.4
ENVTEST_VERSION ?= release-0.19
GOLANGCI_LINT_VERSION ?= v2.3.1
MINIKUBE_VERSION ?= v1.34.0
YQ_VERSION ?= v4.44.3
HELM_VERSION ?= v3.16.2
KIND_VERSION ?= v0.30.0
SKAFFOLD_VERSION ?= v2.16.1
KUBECTL_VERSION ?= v1.33.4

# minikube settings
MINIKUBE_PROFILE ?= pvc-autoscaler
MINIKUBE_DRIVER ?= qemu

# A target which is used to clean up previous versions of tools
$(LOCALBIN)/.version_%: | $(LOCALBIN)
	@file=$@; rm -f $${file%_*}*
	@touch $@

# gen-tool-version adds a prereq to a tool target with the given version
# $1 - target binary path
# $2 - version of the tool
gen-tool-version = $(LOCALBIN)/.version_$(subst $(LOCALBIN)/,,$(1))_$(2)

.PHONY: kustomize
kustomize: $(KUSTOMIZE) | $(LOCALBIN)  ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(call gen-tool-version,$(KUSTOMIZE),$(KUSTOMIZE_VERSION))
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: helm
helm: $(HELM) | $(LOCALBIN)  ## Download helm locally if necessary.
$(HELM): $(call gen-tool-version,$(HELM),$(HELM_VERSION))
	curl -L -o - \
		https://get.helm.sh/helm-$(HELM_VERSION)-$(GOOS)-$(GOARCH).tar.gz | \
		tar zxvf - -C $(LOCALBIN) --strip-components=1 $(GOOS)-$(GOARCH)/helm
	touch $(HELM) && chmod +x $(HELM)

.PHONY: minikube
minikube: $(MINIKUBE) | $(LOCALBIN)  ## Download minikube locally if necessary.
$(MINIKUBE): $(call gen-tool-version,$(MINIKUBE),$(MINIKUBE_VERSION))
	$(call download-tool,minikube,https://github.com/kubernetes/minikube/releases/download/$(MINIKUBE_VERSION)/minikube-$(GOOS)-$(GOARCH))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) | $(LOCALBIN)  ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(call gen-tool-version,$(CONTROLLER_GEN),$(CONTROLLER_TOOLS_VERSION))
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: envtest
envtest: $(ENVTEST) | $(LOCALBIN) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(call gen-tool-version,$(ENVTEST),$(ENVTEST_VERSION))
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) | $(LOCALBIN)  ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(call gen-tool-version,$(GOLANGCI_LINT),$(GOLANGCI_LINT_VERSION))
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

.PHONY: yq
yq: $(YQ) | $(LOCALBIN)  ## Download yq locally if necessary.
$(YQ): $(call gen-tool-version,$(YQ),$(YQ_VERSION))
	$(call download-tool,yq,https://github.com/mikefarah/yq/releases/download/$(YQ_VERSION)/yq_$(GOOS)_$(GOARCH))

.PHONY: kind
kind: $(KIND) | $(LOCALBIN)  ## Download kind locally if necessary.
$(KIND): $(call gen-tool-version,$(KIND),$(KIND_VERSION))
	$(call download-tool,kind,https://kind.sigs.k8s.io/dl/$(KIND_VERSION)/kind-$(GOOS)-$(GOARCH))

.PHONY: skaffold
skaffold: $(SKAFFOLD) | $(LOCALBIN)  ## Download skaffold locally if necessary.
$(SKAFFOLD): $(call gen-tool-version,$(SKAFFOLD),$(SKAFFOLD_VERSION))
		$(call download-tool,skaffold,https://storage.googleapis.com/skaffold/releases/$(SKAFFOLD_VERSION)/skaffold-$(GOOS)-$(GOARCH))

.PHONY: kubectl
kubectl: $(KUBECTL) | $(LOCALBIN)  ## Download kubectl locally if necessary.
$(KUBECTL): $(call gen-tool-version,$(KUBECTL),$(KUBECTL_VERSION))
		$(call download-tool,kubectl,https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(GOOS)/$(GOARCH)/kubectl)

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
#
# $1 - target path with name of binary (ideally with version)
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
GOBIN=$(LOCALBIN) go install $${package}
endef

# download-tool will download a binary package from the given URL.
#
# $1 - name of the tool
# $2 - HTTP URL to download the tool from
define download-tool
@set -e; \
tool=$(1) ;\
echo "Downloading $${tool}" ;\
curl -o $(LOCALBIN)/$(1) -sSfL $(2) ;\
chmod +x $(LOCALBIN)/$(1)
endef
