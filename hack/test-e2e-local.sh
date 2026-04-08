#!/usr/bin/env bash
# SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

#
# A script to run the e2e tests

set -eu -o pipefail

_SCRIPT_NAME="${0##*/}"
_SCRIPT_DIR=$( dirname `readlink -f -- "${0}"` )
_TEST_MANIFESTS_DIR="${_SCRIPT_DIR}/../test/manifests"

# Import common utils
source "${_SCRIPT_DIR}/common.sh"

# Tests the PVC autoscaler by consuming space from a PVC and expecting the PVC
# autoscaler to resize the volume until we fully exhaust the space.
function _test_consume_space_and_resize() {
  local _pvc_yaml="${_TEST_MANIFESTS_DIR}/pvc-1.yaml"
  local _pvca_yaml="${_TEST_MANIFESTS_DIR}/pvca-1.yaml"
  local _pod_yaml="${_TEST_MANIFESTS_DIR}/pod-1.yaml"
  local _pod_name=$( yq '.metadata.name' "${_pod_yaml}" )
  local _pvca_name=$( yq '.metadata.name' "${_pvca_yaml}" )
  local _pod_path=$( yq '.spec.containers[0].volumeMounts[0].mountPath' "${_pod_yaml}" )
  local _pvc_name=$( yq '.metadata.name' "${_pvc_yaml}" )
  local _namespace=$( yq '.metadata.namespace // "default"' "${_pod_yaml}" )

  _msg_info "starting test: consume space and resize"
  _msg_info "creating test pvc, pvca and pod ..."
  kubectl create -f "${_pvc_yaml}"
  kubectl create -f "${_pvca_yaml}"
  kubectl create -f "${_pod_yaml}"

  _msg_info "waiting for test pod to be ready ..."
  kubectl wait "pod/${_pod_name}" \
          --for condition=Ready \
          --namespace "${_namespace}" \
          --timeout 10m

  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_bytes_low_1Gi
  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # The test pod initially comes with a PVC of size 1Gi.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 1Gi

  # Consume 90% of the PVC capacity.
  _msg_info "consuming 900MB of the space ..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_bytes_high_1Gi

  # Once we consume the space we expect to see these events for the PVC object.
  _wait_for_event Warning FreeSpaceThresholdReached "pvc/${_pvc_name}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_name}"
  _wait_for_event Normal Resizing "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_name}"

  # We should be at 2Gi now
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_bytes_low_2Gi

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  _msg_info "consuming another 900MB of space ..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_bytes_high_2Gi

  # We should see a second occurrence of these events
  _wait_for_event_to_occur_n_times Normal Resizing "pvc/${_pvc_name}" 2
  _wait_for_event_to_occur_n_times Normal FileSystemResizeSuccessful "pvc/${_pvc_name}" 2

  # We should be at 3Gi now
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_bytes_low_3Gi

  _msg_info "consuming all available disk space ..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_bytes_high_3Gi

  # And finally we should be at the max capacity, which is to 3Gi in the test
  # manifests.
  _wait_for_event Warning MaxCapacityReached "pvc/${_pvc_name}"

  # We should remain at 3Gi
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi

  _msg_info "waiting for PVC Autoscaler resource to have false Resizing condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=Resizing=false \
          --namespace "${_namespace}" \
          --timeout 10m

  _cleanup "${_pod_name}" "${_pvc_name}" "${_pvca_name}" "${_namespace}"
}

# Tests the PVC autoscaler by consuming inodes from a volume and then expects
# the PVC autoscaler to resize the volume.
function _test_consume_inodes_and_resize() {
  local _pvc_yaml="${_TEST_MANIFESTS_DIR}/pvc-2.yaml"
  local _pvca_yaml="${_TEST_MANIFESTS_DIR}/pvca-2.yaml"
  local _pod_yaml="${_TEST_MANIFESTS_DIR}/pod-2.yaml"
  local _pod_name=$( yq '.metadata.name' "${_pod_yaml}" )
  local _pvca_name=$( yq '.metadata.name' "${_pvca_yaml}" )
  local _pod_path=$( yq '.spec.containers[0].volumeMounts[0].mountPath' "${_pod_yaml}" )
  local _pvc_name=$( yq '.metadata.name' "${_pvc_yaml}" )
  local _namespace=$( yq '.metadata.namespace // "default"' "${_pod_yaml}" )

  _msg_info "starting test: consume inodes and resize"
  _msg_info "creating test pvc, pvca and pod ..."
  kubectl create -f "${_pvc_yaml}"
  kubectl create -f "${_pvca_yaml}"
  kubectl create -f "${_pod_yaml}"

  _msg_info "waiting for test pod to be ready ..."
  kubectl wait "pod/${_pod_name}" \
          --for condition=Ready \
          --namespace "${_namespace}" \
          --timeout 10m

  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_inode_low_1Gi
  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # The test pod initially comes with a PVC of size 1Gi.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 1Gi

  # The test pod initially comes a PVC of size 1Gi, which gives us ~ 65K of
  # available inodes.
  _msg_info "consuming 60K inodes ..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_inode_high_1Gi

  # We should see these events
  _wait_for_event Warning FreeInodesThresholdReached "pvc/${_pvc_name}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_name}"
  _wait_for_event Normal Resizing "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_name}"

  # We should be at 2Gi now
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_inode_low_2Gi

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # After the first increase of the volume we should have a total of ~ 130K inodes.
  _msg_info "consuming another 60K inodes ..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_inode_high_2Gi

  # We should see a second occurrence of these events
  _wait_for_event_to_occur_n_times Normal Resizing "pvc/${_pvc_name}" 2
  _wait_for_event_to_occur_n_times Normal FileSystemResizeSuccessful "pvc/${_pvc_name}" 2

  # We should be at 3Gi now
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_inode_low_3Gi

  # Once the volume resizes for a second time we should have a total of ~196K inodes.
  _msg_info "consuming all available inodes ..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_inode_high_3Gi

  # And finally we should be at the max limit, so no more resizing happens
  _wait_for_event Warning MaxCapacityReached "pvc/${_pvc_name}"

  # We should remain at 3Gi
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi

  _msg_info "waiting for PVC Autoscaler resource to have false Resizing condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=Resizing=false \
          --namespace "${_namespace}" \
          --timeout 10m

  _cleanup "${_pod_name}" "${_pvc_name}" "${_pvca_name}" "${_namespace}"
}

# Tests the cooldown functionality by verifying that a resize is blocked
# during the cooldown period and succeeds after it expires.
function _test_cooldown() {
  local _pvc_yaml="${_TEST_MANIFESTS_DIR}/pvc-3.yaml"
  local _pvca_yaml="${_TEST_MANIFESTS_DIR}/pvca-3.yaml"
  local _pod_yaml="${_TEST_MANIFESTS_DIR}/pod-3.yaml"
  local _pod_name=$( yq '.metadata.name' "${_pod_yaml}" )
  local _pvca_name=$( yq '.metadata.name' "${_pvca_yaml}" )
  local _pvc_name=$( yq '.metadata.name' "${_pvc_yaml}" )
  local _namespace=$( yq '.metadata.namespace // "default"' "${_pod_yaml}" )

  _msg_info "starting cooldown test: 3m cooldown duration"
  _msg_info "creating test pvc, pvca and pod ..."
  kubectl create -f "${_pvc_yaml}"
  kubectl create -f "${_pvca_yaml}"
  kubectl create -f "${_pod_yaml}"

  _msg_info "waiting for test pod to be ready ..."
  kubectl wait "pod/${_pod_name}" \
          --for condition=Ready \
          --namespace "${_namespace}" \
          --timeout 10m

  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_cooldown_low_1Gi
  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # The test pod initially comes with a PVC of size 1Gi.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 1Gi

  # Consume 90% of the PVC capacity to trigger first resize.
  _msg_info "triggering first resize (not blocked by cooldown)..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_cooldown_high_1Gi

  # Once we consume the space we expect to see these events for the PVC object.
  _wait_for_event Warning FreeSpaceThresholdReached "pvc/${_pvc_name}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_name}"

  # We should be at 2Gi now
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  _msg_info "first resize completed, cooldown timer started (3 minutes)"

  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_cooldown_low_2Gi

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  _msg_info "triggering second resize (should be blocked by cooldown)..."
  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_cooldown_high_2Gi

  _msg_info "verifying resize is blocked by cooldown..."
  _wait_for_pvca_cooldown "${_pvca_name}" "${_namespace}" 10 30

  # PVC should still be at 2Gi
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  _msg_info "confirmed: resize blocked by cooldown"

  # Wait for cooldown to expire and resize to complete
  _msg_info "waiting for cooldown to expire (3 minutes) and resize to 3Gi..."
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi 30 7 # Wait up to 3.5 minutes (0.5 buffer) for resize to complete after cooldown expires

  ${_SCRIPT_DIR}/set-volume-metrics-stage.sh pod_cooldown_low_3Gi
  _msg_info "cooldown test completed successfully"

  _cleanup "${_pod_name}" "${_pvc_name}" "${_pvca_name}" "${_namespace}"
}

# Main entrypoint
function _main() {  
  _msg_info "Waiting for pvc-autoscaler pods to become ready ..."
  kubectl wait \
          --for condition=Ready \
          --all Pod \
          --namespace pvc-autoscaler-system \
          --timeout 10m

  local _has_failed="false"
  if ! _test_consume_space_and_resize; then
    _msg_error "test consume space and resize has failed" 0
    _has_failed="true"
  fi

  if ! _test_consume_inodes_and_resize; then
    _msg_error "test consume inodes and resize has failed" 0
    _has_failed="true"
  fi

  if ! _test_cooldown; then
    _msg_error "test cooldown has failed" 0
    _has_failed="true"
  fi

  if [ "${_has_failed}" == "true" ]; then
    _msg_error "Failed" 1
  fi

  _msg_info "Success"
}

_main $*
