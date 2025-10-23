#!/usr/bin/env bash
# SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -o errexit
set -o nounset
set -o pipefail


WITH_LPP_RESIZE_SUPPORT=${WITH_LPP_RESIZE_SUPPORT:-true}

parse_flags() {
  while test $# -gt 0; do
    case "$1" in
    --with-lpp-resize-support)
      shift
      WITH_LPP_RESIZE_SUPPORT="${1}"
      ;;
    esac

    shift
  done
}

kubectl patch storageclass standard -p '{"allowVolumeExpansion": true}'

nodes=$(kubectl get nodes -o jsonpath='{.items[*].metadata.name}')

# setup_containerd_registry_mirrors sets up all containerd registry mirrors.
# Resources:
# - https://github.com/containerd/containerd/blob/main/docs/hosts.md
# - https://kind.sigs.k8s.io/docs/user/local-registry/
setup_containerd_registry_mirrors() {
  NODES=("$@")
  REGISTRY_HOSTNAME="garden.local.gardener.cloud"

  for NODE in "${NODES[@]}"; do
    setup_containerd_registry_mirror $NODE "gcr.io" "https://gcr.io" "http://${REGISTRY_HOSTNAME}:5003"
    setup_containerd_registry_mirror $NODE "registry.k8s.io" "https://registry.k8s.io" "http://${REGISTRY_HOSTNAME}:5006"
    setup_containerd_registry_mirror $NODE "quay.io" "https://quay.io" "http://${REGISTRY_HOSTNAME}:5007"
    setup_containerd_registry_mirror $NODE "europe-docker.pkg.dev" "https://europe-docker.pkg.dev" "http://${REGISTRY_HOSTNAME}:5008"
    setup_containerd_registry_mirror $NODE "garden.local.gardener.cloud:5001" "http://garden.local.gardener.cloud:5001" "http://${REGISTRY_HOSTNAME}:5001"
  done
}

# setup_containerd_registry_mirror sets up a given contained registry mirror.
setup_containerd_registry_mirror() {
  NODE=$1
  UPSTREAM_HOST=$2
  UPSTREAM_SERVER=$3
  MIRROR_HOST=$4

  echo "[${NODE}] Setting up containerd registry mirror for host ${UPSTREAM_HOST}.";
  REGISTRY_DIR="/etc/containerd/certs.d/${UPSTREAM_HOST}"
  docker exec "${NODE}" mkdir -p "${REGISTRY_DIR}"
  cat <<EOF | docker exec -i "${NODE}" cp /dev/stdin "${REGISTRY_DIR}/hosts.toml"
server = "${UPSTREAM_SERVER}"

[host."${MIRROR_HOST}"]
  capabilities = ["pull", "resolve"]
EOF
}

setup_containerd_registry_mirrors $nodes

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

setup_kind_sc_default_volume_type
setup_kind_with_lpp_resize_support
