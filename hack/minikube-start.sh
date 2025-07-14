#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0


set -e

_SCRIPT_NAME="${0##*/}"
_SCRIPT_DIR=$( dirname `readlink -f -- "${0}"` )
_TEST_MANIFESTS_DIR="${_SCRIPT_DIR}/../test/manifests"

# Import common utils
source "${_SCRIPT_DIR}/common.sh"

# Name of the minikube profile
MINIKUBE_PROFILE=${MINIKUBE_PROFILE:-minikube}

# Because we need a real VM with /sys mounted in read-write mode and udev for
# device events, running the nodes within Docker is not supported.
#
# See the following links for more details.
#
# - https://www.freedesktop.org/wiki/Software/systemd/ContainerInterface/
# - https://github.com/kubernetes-sigs/kind/issues/1474
MINIKUBE_DRIVER=${MINIKUBE_DRIVER:-qemu}

# The repo which provides the kube-prometheus manifests
KUBE_PROMETHEUS_REPO=${KUBE_PROMETHEUS_REPO:-https://github.com/prometheus-operator/kube-prometheus}

# The branch to use for the kube-prometheus manifests
KUBE_PROMETHEUS_BRANCH=${KUBE_PROMETHEUS_BRANCH:-main}

# OpenEBS Operator
OPENEBS_HELM_REPO=${OPENEBS_HELM_REPO:-https://openebs.github.io/openebs}

# Start a new local minikube cluster
function _minikube_start() {
  _msg_info "Creating minikube cluster '${MINIKUBE_PROFILE}' using driver '${MINIKUBE_DRIVER}'"
  minikube start \
           --profile="${MINIKUBE_PROFILE}" \
           --driver="${MINIKUBE_DRIVER}" \
           --memory=8g \
           --bootstrapper=kubeadm \
           --extra-config=kubelet.authentication-token-webhook=true \
           --extra-config=kubelet.authorization-mode=Webhook \
           --extra-config=scheduler.bind-address=0.0.0.0 \
           --extra-config=controller-manager.bind-address=0.0.0.0

  minikube profile "${MINIKUBE_PROFILE}"
  minikube addons disable metrics-server

  # List of additional modules to load, if needed
  local _modules=()
  for _module in "${_modules[@]}"; do
    _msg_info "Loading kernel module in node: ${_module}"
    minikube ssh -- sudo modprobe "${_module}"
  done
}

# Installs kube-prometheus [1] in the local cluster.
#
# [1]: https://github.com/prometheus-operator/kube-prometheus
function _install_kube_prometheus() {
  local _workdir=$( mktemp -d )
  local _oldpwd="${OLDPWD}"
  local _local_branch="install-$(date +%Y-%m-%d)"

  _msg_info "Installing kube-prometheus operator ..."

  cd "${_workdir}" && \
    git clone "${KUBE_PROMETHEUS_REPO}" && \
    cd kube-prometheus && \
    git checkout -b "${_local_branch}" "origin/${KUBE_PROMETHEUS_BRANCH}"

  minikube kubectl -- apply --server-side -f manifests/setup
  minikube kubectl -- wait \
          --for condition=Established \
          --all CustomResourceDefinition \
          --namespace=monitoring

  minikube kubectl -- apply -f manifests/
  _msg_info "Waiting for monitoring pods to become ready ..."
  sleep 15
  minikube kubectl -- wait \
          --for condition=Ready \
          --all Pod \
          --namespace monitoring \
          --timeout 10m

  cd "${_oldpwd}"
  rm -rf "${_workdir}"
}

# Installs OpenEBS Operator
#
# See https://openebs.io/docs/user-guides/installation
function _install_openebs_operator() {
  _msg_info "Installing OpenEBS operator ..."
  helm repo add openebs "${OPENEBS_HELM_REPO}"
  helm repo update
  helm install openebs \
       --namespace openebs \
       --create-namespace \
       --set engines.local.zfs.enabled=false \
       --set engines.replicated.mayastor.enabled=false \
       --set alloy.enabled=false \
       --set loki.enabled=false \
       openebs/openebs

  _msg_info "Waiting for OpenEBS pods to become ready ..."
  sleep 15
  minikube kubectl -- wait \
          --for condition=Ready \
          --all Pod \
          --namespace openebs \
          --timeout 10m

  _msg_info "Creating LVM PV and VG for OpenEBS driver ..."
  local _disk_path="/mnt/disks"
  local _disk0_path="${_disk_path}/disk0"
  local _vg_name="vg0"
  minikube ssh -- sudo mkdir "${_disk_path}"
  minikube ssh -- sudo truncate -s 100G "${_disk0_path}"
  local _lo_dev=$( minikube ssh -- sudo losetup --find --show "${_disk0_path}" | sed -e 's|\r||g' )
  minikube ssh -- sudo pvcreate "${_lo_dev}"
  minikube ssh -- sudo vgcreate vg0 "${_lo_dev}"

  _msg_info "Installing LVM-backed Storage Class ..."
  minikube kubectl -- apply -f "${_TEST_MANIFESTS_DIR}/storageclass.yaml"
}

# Installs cert-manager [1] in the local cluster.
#
# [1]: https://github.com/cert-manager/cert-manager
function _install_cert_manager() {
  _msg_info "Installing cert-manager ..."
  helm repo add jetstack https://charts.jetstack.io
  helm repo update

  helm install \
       cert-manager jetstack/cert-manager \
       --namespace cert-manager \
       --create-namespace \
       --version v1.16.1 \
       --set crds.enabled=true

  _msg_info "Waiting for cert-manager pods to become ready ..."
  sleep 15
  minikube kubectl -- wait \
          --for condition=Ready \
          --all Pod \
          --namespace cert-manager \
          --timeout 10m
}

function _main() {
  _minikube_start
  _install_kube_prometheus
  _install_openebs_operator
  _install_cert_manager
}

_main $*
