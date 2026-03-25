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

# Creates a resource from a YAML file, overriding storageClassName with _STORAGE_CLASS.
# Works for both PVC and StatefulSet volumeClaimTemplates by updating all occurrences.
function _kubectl_create_with_storage_class() {
  local _yaml="${1}"
  yq '(.. | select(has("storageClassName"))).storageClassName = "'"${_STORAGE_CLASS}"'"' "${_yaml}" \
    | kubectl create -f -
}


# Simulates resource consumption on a PVC by writing files (minikube) or switching the KinD
# fake metrics stage (KinD).
#
# On KinD: real consumption does not work because local-path-provisioner backs PVCs with
#          directories on the node's root filesystem, so kubelet reports metrics for the full node
#          disk rather than the PVC capacity. A fake metrics server is used instead; this function
#          maps the metric type + current volume size to the appropriate pre-canned stage and
#          delegates to set-volume-metrics-stage.sh (no real files are written).
# On minikube: writes real files via consume-pod-space.sh so that kubelet picks up actual usage.
#
# The optional 7th argument _stage_family overrides the KinD stage name family
# (default: "pod_<metric>"). Use "pod_cooldown" for the cooldown test or "sts_bytes" for
# the StatefulSet test to match their dedicated stage files.
function _consume() {
  local _metric="${1}"
  local _num_files="${2}"
  local _volume_size="${3}"
  local _pod="${4}"
  local _namespace="${5}"
  local _pod_path="${6}"
  local _stage_family="${7:-pod_${_metric}}"

  if [[ "${_FAKE_METRICS}" == "true" ]]; then
    local _stage="${_stage_family}_high_${_volume_size}"
    _msg_info "setting KinD fake metrics stage to ${_stage} ..."
    "${_SCRIPT_DIR}/set-volume-metrics-stage.sh" "${_stage}"
    return
  fi

  case "${_metric}" in
    bytes)
      _msg_info "consuming ~$(( _num_files * 100 ))MB of disk space in pod ${_pod} ..."
      env POD="${_pod}" NAMESPACE="${_namespace}" POD_PATH="${_pod_path}" \
        FILE_SIZE=100M NUM_FILES="${_num_files}" \
        "${_SCRIPT_DIR}/consume-pod-space.sh"
      ;;
    inode)
      _msg_info "consuming ~${_num_files} inodes in pod ${_pod} ..."
      env POD="${_pod}" NAMESPACE="${_namespace}" POD_PATH="${_pod_path}" \
        FILE_SIZE=1c NUM_FILES="${_num_files}" \
        "${_SCRIPT_DIR}/consume-pod-space.sh"
      ;;
  esac
}

# Resets the KinD fake metrics stage to reflect low usage after a volume resize.
# This is a no-op on minikube (real usage is already reflected by the filesystem).
#
# On KinD: because metrics report node-level disk stats (not PVC capacity), the fake metrics
#          server must be switched to the pod_<metric>_low_<new_volume_size> stage after each
#          resize so that it reports low usage on the newly-resized volume, allowing the autoscaler
#          to reach RecommendationAvailable before the next consumption round.
# On minikube: no action needed — the actual filesystem reflects the current usage.
#
# The optional 3rd argument _stage_family overrides the KinD stage name family
# (default: "pod_<metric>"). Use "pod_cooldown" for the cooldown test or "sts_bytes" for
# the StatefulSet test.
function _set_fake_metrics_after_resize() {
  local _metric="${1}"
  local _new_volume_size="${2}"
  local _stage_family="${3:-pod_${_metric}}"

  if [[ "${_FAKE_METRICS}" == "true" ]]; then
    local _stage="${_stage_family}_low_${_new_volume_size}"
    _msg_info "setting KinD fake metrics stage to ${_stage} ..."
    "${_SCRIPT_DIR}/set-volume-metrics-stage.sh" "${_stage}"
  fi
}


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
  _kubectl_create_with_storage_class "${_pvc_yaml}"
  kubectl create -f "${_pvca_yaml}"
  kubectl create -f "${_pod_yaml}"

  _msg_info "waiting for test pod to be ready ..."
  kubectl wait "pod/${_pod_name}" \
          --for condition=Ready \
          --namespace "${_namespace}" \
          --timeout 10m

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # The test pod initially comes with a PVC of size 1Gi.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 1Gi

  # Consume ~900MB (9 files) which is around ~88% of the 1Gi PVC to trigger autoscaling.
  _consume bytes 9 1Gi "${_pod_name}" "${_namespace}" "${_pod_path}"

  # Once we consume the space we expect to see these events for the PVC object.
  _wait_for_event Warning UsedSpaceThresholdReached "pvc/${_pvc_name}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_name}"
  _wait_for_event Normal Resizing "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_name}"

  # We should be at 2Gi now.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  _set_fake_metrics_after_resize bytes 2Gi

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # Consume another ~900MB of the 2Gi PVC to trigger a second autoscaling.
  _consume bytes 9 2Gi "${_pod_name}" "${_namespace}" "${_pod_path}"

  # We should see a second occurrence of these events.
  _wait_for_event_to_occur_n_times Normal Resizing "pvc/${_pvc_name}" 2
  _wait_for_event_to_occur_n_times Normal FileSystemResizeSuccessful "pvc/${_pvc_name}" 2

  # We should be at 3Gi now.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi
  _set_fake_metrics_after_resize bytes 3Gi

  # Consume ~900MB of the 3Gi PVC to exhaust the max capacity limit.
  _consume bytes 9 3Gi "${_pod_name}" "${_namespace}" "${_pod_path}"

  # And finally we should be at the max capacity, which is 3Gi in the test manifests.
  _wait_for_event Warning MaxCapacityReached "pvc/${_pvc_name}"

  # We should remain at 3Gi.
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
  _kubectl_create_with_storage_class "${_pvc_yaml}"
  kubectl create -f "${_pvca_yaml}"
  kubectl create -f "${_pod_yaml}"

  _msg_info "waiting for test pod to be ready ..."
  kubectl wait "pod/${_pod_name}" \
          --for condition=Ready \
          --namespace "${_namespace}" \
          --timeout 10m

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # The test pod initially comes with a PVC of size 1Gi, which gives us ~65K available inodes.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 1Gi

  # Consume ~60K inodes (~92%) of the 1Gi PVC to trigger autoscaling.
  _consume inode 60000 1Gi "${_pod_name}" "${_namespace}" "${_pod_path}"

  # We should see these events
  _wait_for_event Warning UsedInodesThresholdReached "pvc/${_pvc_name}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_name}"
  _wait_for_event Normal Resizing "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_name}"

  # We should be at 2Gi now, giving us ~130K total inodes.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  _set_fake_metrics_after_resize inode 2Gi

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # Consume another ~60K inodes on the 2Gi PVC to trigger a second autoscaling.
  _consume inode 60000 2Gi "${_pod_name}" "${_namespace}" "${_pod_path}"

  # We should see a second occurrence of these events.
  _wait_for_event_to_occur_n_times Normal Resizing "pvc/${_pvc_name}" 2
  _wait_for_event_to_occur_n_times Normal FileSystemResizeSuccessful "pvc/${_pvc_name}" 2

  # We should be at 3Gi now, giving us ~196K total inodes.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi
  _set_fake_metrics_after_resize inode 3Gi

  # Consume another ~60K inodes on the 3Gi PVC to exhaust the max capacity limit.
  _consume inode 60000 3Gi "${_pod_name}" "${_namespace}" "${_pod_path}"

  # And finally we should be at the max limit, so no more resizing happens.
  _wait_for_event Warning MaxCapacityReached "pvc/${_pvc_name}"

  # We should remain at 3Gi.
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
  local _pod_path=$( yq '.spec.containers[0].volumeMounts[0].mountPath' "${_pod_yaml}" )
  local _namespace=$( yq '.metadata.namespace // "default"' "${_pod_yaml}" )

  _msg_info "starting cooldown test: 3m cooldown duration"
  _msg_info "creating test pvc, pvca and pod ..."
  _kubectl_create_with_storage_class "${_pvc_yaml}"
  kubectl create -f "${_pvca_yaml}"
  kubectl create -f "${_pod_yaml}"

  _msg_info "waiting for test pod to be ready ..."
  kubectl wait "pod/${_pod_name}" \
          --for condition=Ready \
          --namespace "${_namespace}" \
          --timeout 10m

  _set_fake_metrics_after_resize bytes 1Gi pod_cooldown
  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # The test pod initially comes with a PVC of size 1Gi.
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 1Gi

  # Consume 90% of the PVC capacity to trigger first resize.
  _msg_info "triggering first resize (not blocked by cooldown)..."
  _consume bytes 9 1Gi "${_pod_name}" "${_namespace}" "${_pod_path}" pod_cooldown

  # Once we consume the space we expect to see these events for the PVC object.
  _wait_for_event Warning UsedSpaceThresholdReached "pvc/${_pvc_name}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_name}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_name}"

  # We should be at 2Gi now
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  _msg_info "first resize completed, cooldown timer started (3 minutes)"

  _set_fake_metrics_after_resize bytes 2Gi pod_cooldown

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  _msg_info "triggering second resize (should be blocked by cooldown)..."
  _consume bytes 9 2Gi "${_pod_name}" "${_namespace}" "${_pod_path}" pod_cooldown

  _msg_info "verifying resize is blocked by cooldown..."
  _wait_for_pvca_cooldown "${_pvca_name}" "${_namespace}" 10 30

  # PVC should still be at 2Gi
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 2Gi
  _msg_info "confirmed: resize blocked by cooldown"

  # Wait for cooldown to expire and resize to complete
  _msg_info "waiting for cooldown to expire (3 minutes) and resize to 3Gi..."
  _ensure_pvc_capacity "${_pvc_name}" "${_namespace}" 3Gi 30 7 # Wait up to 3.5 minutes (0.5 buffer) for resize to complete after cooldown expires

  _set_fake_metrics_after_resize bytes 3Gi pod_cooldown
  _msg_info "cooldown test completed successfully"

  _cleanup "${_pod_name}" "${_pvc_name}" "${_pvca_name}" "${_namespace}"
}

# Tests the PVC autoscaler when targeting a StatefulSet. The PVCA selects a
# StatefulSet via its scale subresource, the controller resolves the matching
# pods and the PVCs they reference, and resizes all of them when the threshold
# is reached.
function _test_consume_space_and_resize_statefulset() {
  local _sts_yaml="${_TEST_MANIFESTS_DIR}/sts-4.yaml"
  local _pvca_yaml="${_TEST_MANIFESTS_DIR}/pvca-4.yaml"
  local _sts_name=$( yq '.metadata.name' "${_sts_yaml}" )
  local _pvca_name=$( yq '.metadata.name' "${_pvca_yaml}" )
  local _namespace=$( yq '.metadata.namespace // "default"' "${_sts_yaml}" )
  local _pod_path=$( yq '.spec.template.spec.containers[0].volumeMounts[0].mountPath' "${_sts_yaml}" )
  local _pvc_0="data-${_sts_name}-0"
  local _pvc_1="data-${_sts_name}-1"

  _msg_info "starting test: target StatefulSet, consume space and resize"
  _msg_info "creating test statefulset and pvca ..."
  _kubectl_create_with_storage_class "${_sts_yaml}"
  kubectl create -f "${_pvca_yaml}"

  _msg_info "waiting for statefulset pods to be ready ..."
  kubectl rollout status "statefulset/${_sts_name}" \
          --namespace "${_namespace}" \
          --timeout 10m

  _set_fake_metrics_after_resize bytes 1Gi sts_bytes
  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  # The StatefulSet's volumeClaimTemplate provisions PVCs at 1Gi.
  _ensure_pvc_capacity "${_pvc_0}" "${_namespace}" 1Gi
  _ensure_pvc_capacity "${_pvc_1}" "${_namespace}" 1Gi

  # Drive both PVCs above the threshold simultaneously.
  _msg_info "consuming 90% of the space on both PVCs ..."
  _consume bytes 9 1Gi "${_sts_name}-0" "${_namespace}" "${_pod_path}" sts_bytes
  _consume bytes 9 1Gi "${_sts_name}-1" "${_namespace}" "${_pod_path}" sts_bytes

  # Both PVCs should be picked up and resized by the same PVCA.
  _wait_for_event Warning UsedSpaceThresholdReached "pvc/${_pvc_0}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_0}"
  _wait_for_event Normal Resizing "pvc/${_pvc_0}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_0}"

  _wait_for_event Warning UsedSpaceThresholdReached "pvc/${_pvc_1}"
  _wait_for_event Normal ResizingStorage "pvc/${_pvc_1}"
  _wait_for_event Normal Resizing "pvc/${_pvc_1}"
  _wait_for_event Normal FileSystemResizeSuccessful "pvc/${_pvc_1}"

  # Both PVCs should be at 2Gi now.
  _ensure_pvc_capacity "${_pvc_0}" "${_namespace}" 2Gi
  _ensure_pvc_capacity "${_pvc_1}" "${_namespace}" 2Gi

  _set_fake_metrics_after_resize bytes 2Gi sts_bytes

  _msg_info "waiting for PVC Autoscaler resource to have RecommendationAvailable condition ..."
  kubectl wait "pvca/${_pvca_name}" \
          --for condition=RecommendationAvailable \
          --namespace "${_namespace}" \
          --timeout 10m

  _cleanup_statefulset "${_sts_name}" "${_pvca_name}" "${_namespace}" "${_pvc_0}" "${_pvc_1}"
}

# Cleanup helper for the StatefulSet-based test. StatefulSets do not delete
# their PVCs, so we drop them explicitly.
function _cleanup_statefulset() {
  local _sts_name="${1}"
  local _pvca_name="${2}"
  local _namespace="${3}"
  shift 3

  kubectl --namespace "${_namespace}" delete pvca "${_pvca_name}"
  kubectl --namespace "${_namespace}" delete statefulset "${_sts_name}"
  for _pvc_name in "$@"; do
    kubectl --namespace "${_namespace}" delete pvc "${_pvc_name}"
  done
}

# Main entrypoint
function _main() {
  kubectl get deployment pvc-autoscaler-minimal-fake-metrics-server -n pvc-autoscaler-system &>/dev/null && _FAKE_METRICS="true" || _FAKE_METRICS="false"

  if [[ "${_FAKE_METRICS}" == "true" ]]; then
    _STORAGE_CLASS="standard"
  else
    _STORAGE_CLASS="openebs-lvm"
  fi

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

  if ! _test_consume_space_and_resize_statefulset; then
    _msg_error "test consume space and resize for statefulset has failed" 0
    _has_failed="true"
  fi

  if [ "${_has_failed}" == "true" ]; then
    _msg_error "Failed" 1
  fi

  _msg_info "Success"
}

_main $*
