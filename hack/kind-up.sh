#!/usr/bin/env bash
# SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -o errexit
set -o nounset
set -o pipefail


WITH_LPP_RESIZE_SUPPORT=${WITH_LPP_RESIZE_SUPPORT:-true}
KINDEST_NODE_IMAGE_TAG=${KINDEST_NODE_IMAGE_TAG:-v1.33.4@sha256:25a6018e48dfcaee478f4a59af81157a437f15e6e140bf103f85a2e7cd0cbbf2}

parse_flags() {
  while test $# -gt 0; do
    case "$1" in
    --with-lpp-resize-support)
      shift
      WITH_LPP_RESIZE_SUPPORT="${1}"
      ;;
    --kindest-node-image-tag)
      shift
      KINDEST_NODE_IMAGE_TAG="${1}"
      ;;
    esac
    shift
  done
}

# The default StorageClass which comes with `kind' is configured to use
# rancher.io/local-path (see [1]) provisioner, which defaults to `hostPath'
# volume (see [2]).  However, `hostPath' does not expose any metrics via
# kubelet, while `local' (see [3]) does. On the other hand `kind' does not
# expose any mechanism for configuring the StorageClass it comes with (see [4]).
#
# This function annotates the default StorageClass with `defaultVolumeType: local',
# so that we can later scrape the various `kubelet_volume_stats_*' metrics
# exposed by kubelet (see [5]).
#
# References:
#
# [1]: https://github.com/rancher/local-path-provisioner
# [2]: https://kubernetes.io/docs/concepts/storage/volumes/#hostpath
# [3]: https://kubernetes.io/docs/concepts/storage/volumes/#local
# [4]: https://github.com/kubernetes-sigs/kind/blob/main/pkg/cluster/internal/create/actions/installstorage/storage.go
# [5]: https://kubernetes.io/docs/reference/instrumentation/metrics/
setup_kind_sc_default_volume_type() {
  echo "Configuring default StorageClass for kind cluster ..."
  kubectl annotate storageclass standard defaultVolumeType=local
}

# The rancher.io/local-path provisioner at the moment does not support volume
# resizing (see [1]). There is an open PR, which is scheduled for the next
# release around May, 2024 (see [2]). Until [2] is merged we will use a custom
# local-path provisioner with support for volume resizing.
#
# This function should be called after setting up the containerd registries on
# the kind nodes.
#
# References:
#
# [1]: https://github.com/rancher/local-path-provisioner
# [2]: https://github.com/rancher/local-path-provisioner/pull/350
#
# TODO(RadaBDimitrova): remove this once we have [2] merged into upstream
setup_kind_with_lpp_resize_support() {
  if [ "${WITH_LPP_RESIZE_SUPPORT}" != "true" ]; then
    return
  fi

  echo "Configuring kind local-path provisioner with volume resize support ..."

  # First configure allowVolumeExpansion on the default StorageClass
  kubectl patch storageclass standard --patch '{"allowVolumeExpansion": true}'

  # Apply the latest manifests and use our own image
  local _image="ghcr.io/ialidzhikov/local-path-provisioner:feature-external-resizer-c0c1c13"
  local _lpp_repo="https://github.com/marjus45/local-path-provisioner"
  local _lpp_branch="feature-external-resizer"
  local _timeout="90"

  kustomize build "${_lpp_repo}/deploy/?ref=${_lpp_branch}&timeout=${_timeout}" | \
    sed -e "s|image: rancher/local-path-provisioner:master-head|image: ${_image}|g" | \
    kubectl apply -f -

  # The default manifests from rancher/local-path come with another
  # StorageClass, which we don't need, so make sure to remove it.
  kubectl delete --ignore-not-found=true storageclass local-path
}

kind create cluster --name pvc-autoscaler --image "kindest/node:${KINDEST_NODE_IMAGE_TAG}"
setup_kind_sc_default_volume_type
setup_kind_with_lpp_resize_support

