#!/usr/bin/env bash

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
OPENEBS_OPERATOR=${OPENEBS_OPERATOR:-https://openebs.github.io/charts/openebs-operator.yaml}
OPENEBS_CSI_DRIVER=${OPENEBS_CSI_DRIVER:-https://openebs.github.io/charts/lvm-operator.yaml}

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

  kubectl apply --server-side -f manifests/setup
  kubectl wait \
          --for condition=Established \
          --all CustomResourceDefinition \
          --namespace=monitoring

  kubectl apply -f manifests/
  _msg_info "Waiting for monitoring pods to become ready ..."
  sleep 10
  kubectl wait \
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
  kubectl apply -f "${OPENEBS_OPERATOR}"

  _msg_info "Waiting for OpenEBS pods to become ready ..."
  sleep 10
  kubectl wait \
          --for condition=Ready \
          --all Pod \
          --namespace openebs \
          --timeout 10m
}

# Installs the OpenEBS LVM CSI driver
#
# See https://github.com/openebs/lvm-localpv
function _install_openebs_lvm_driver() {
  _msg_info "Installing OpenEBS LVM CSI driver ..."

  kubectl apply -f "${OPENEBS_CSI_DRIVER}"
  _msg_info "Waiting for OpenEBS LVM CSI driver pods to become ready ..."
  sleep 10

  # The OpenEBS LVM CSI driver installs daemonsets in the kube-system (sigh), so
  # we wait for them there.
  kubectl wait \
          --for condition=Ready \
          --all Pod \
          --namespace kube-system \
          --selector role=openebs-lvm \
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
  kubectl apply -f "${_TEST_MANIFESTS_DIR}/storageclass/openebs-lvm.yaml"
}

function _main() {
  _minikube_start
  _install_kube_prometheus
  _install_openebs_operator
  _install_openebs_lvm_driver
}

_main $*
