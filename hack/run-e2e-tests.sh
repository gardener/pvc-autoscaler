#!/usr/bin/env bash
#
# A script to run the e2e tests

set -e

_SCRIPT_NAME="${0##*/}"
_SCRIPT_DIR=$( dirname `readlink -f -- "${0}"` )
_TEST_MANIFESTS_DIR="${_SCRIPT_DIR}/../test/manifests"

# Import common utils
source "${_SCRIPT_DIR}/common.sh"

# Tests the PVC autoscaler by consuming space from a PVC and expecting the PVC
# autoscaler to resize the volume until we fully exhaust the space.
function _test_consume_space_and_resize() {
  local _pod_name="test-pod-1"
  local _pod_path="/app"
  local _pvc_name="test-pvc-1"
  local _namespace="default"

  _msg_info "starting test: consume space and resize"
  _msg_info "creating test pvc and pod ..."
  kubectl create -f "${_TEST_MANIFESTS_DIR}/pvc-1.yaml"
  kubectl create -f "${_TEST_MANIFESTS_DIR}/pod-1.yaml"

  _msg_info "waiting for test pod to be ready ..."
  kubectl wait "pod/${_pod_name}" \
          --for condition=Ready \
          --namespace "${_namespace}" \
          --timeout 10m

  # The test pod initially comes with a PVC of size 1Gi.  Consume 90% of it.
  _msg_info "consuming 900MB of the space ..."
  env POD="${_pod_name}" \
      NAMESPACE="${_namespace}" \
      NUM_FILES=9 \
      FILE_SIZE=100M \
      POD_PATH="${_pod_path}" \
      ${_SCRIPT_DIR}/consume-pod-space.sh

  # Once we consume the space we expect to see these events for the PVC object.
  _wait_for_event Warning FreeSpaceThresholdReached "pvc/${_pvc_name}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_name}"
  _wait_for_event Normal Resizing "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeRequired "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_name}"

  _msg_info "consuming another 900MB of space ..."
  env POD="${_pod_name}" \
      NAMESPACE="${_namespace}" \
      NUM_FILES=9 \
      FILE_SIZE=100M \
      POD_PATH="${_pod_path}" \
      ${_SCRIPT_DIR}/consume-pod-space.sh

  # We should see a second occurrence of these events
  _wait_for_event_to_occur_n_times Normal Resizing "pvc/${_pvc_name}" 2
  _wait_for_event_to_occur_n_times Normal FileSystemResizeRequired "pvc/${_pvc_name}" 2
  _wait_for_event_to_occur_n_times Normal FileSystemResizeSuccessful "pvc/${_pvc_name}" 2

  _msg_info "consuming all available disk space ..."
  env POD="${_pod_name}" \
      NAMESPACE="${_namespace}" \
      NUM_FILES=100 \
      FILE_SIZE=100M \
      POD_PATH="${_pod_path}" \
      ${_SCRIPT_DIR}/consume-pod-space.sh >& /dev/null || \
    _msg_info "available disk space consumed"

  # And finally we should be at the max capacity, which is to 3Gi in the test
  # manifests.
  _wait_for_event Warning MaxCapacityReached "pvc/${_pvc_name}"
}

function _main() {
  local _has_failed="false"
  if ! _test_consume_space_and_resize; then
    _msg_error "test consume space and resize has failed" 0
    _has_failed="true"
  fi

  if [ "${_has_failed}" == "true" ]; then
    _msg_error "Failed" 1
  fi

  _msg_info "done"
}

_main $*
