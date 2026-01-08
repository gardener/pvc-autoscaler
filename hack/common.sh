# SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

# Common shell utilities

_SCRIPT_NAME="${0##*/}"

# Refer to the ANSI escape codes table for more details.
# https://en.wikipedia.org/wiki/ANSI_escape_code
_RED='\033[0;31m'
_GREEN='\033[0;32m'
_NO_COLOR='\033[0m'

# Display an INFO message
#
# $1: Message to display
function _msg_info() {
  local _msg="${1}"

  echo -e "[$( date +%Y-%m-%d-%T.%3N)] ${_SCRIPT_NAME} ${_GREEN}INFO${_NO_COLOR}: ${_msg}"
}

# Display an ERROR message
#
# $1: Message to display
# $2: Exit code
function _msg_error() {
  local _msg="${1}"
  local _rc=${2}

  echo -e "[$( date +%Y-%m-%d-%T.%3N)] ${_SCRIPT_NAME} ${_RED}ERROR${_NO_COLOR}: ${_msg}"

  if [[ ${_rc} -ne 0 ]]; then
    exit ${_rc}
  fi
}

# Wait until a given event occurs
#
# $1: event type (e.g. Normal or Warning)
# $2: event reason
# $3: object (e.g. pod/sample-pod)
# $4: poll interval (defaults to 10)
# $5: max attempts (defaults to 60)
function _wait_for_event() {
  local _type="${1}"
  local _reason="${2}"
  local _object="${3}"
  local _namespace=${4:-"pvc-autoscaler-system"}
  local _poll_sec=${5:-10}
  local _max_attempts=${6:-60}
  

  for i in $( seq 1 "${_max_attempts}" ); do
    _msg_info "[${i}/${_max_attempts}] waiting for '${_reason}' (${_type}) event(s) ..."
    local _events=$( kubectl events \
                             -n "${_namespace}" \
                             --for "${_object}" \
                             --types "${_type}" \
                             -o yaml | \
                       yq ".items.[] | select(.reason == \"${_reason}\") | .message" )
    if [ -n "${_events}" ]; then
      _msg_info "received '${_reason}' event(s)"
      echo "---"
      echo "${_events}"
      echo "---"
      return
    fi
    sleep "${_poll_sec}"
  done

  _msg_error "did not receive any '${_reason}' event(s)" 1
}

# Waits for a given event to occur N times
#
# $1: event type (e.g. Normal or Warning)
# $2: event reason
# $3: object (e.g. pod/sample-pod)
# $4: expected number of times event has occurred
# $5: poll interval (defaults to 10)
# $6: max attempts (defaults to 60)
function _wait_for_event_to_occur_n_times() {
  local _type="${1}"
  local _reason="${2}"
  local _object="${3}"
  local _n="${4}"
  local _namespace=${5:-"pvc-autoscaler-system"}
  local _poll_sec=${6:-10}
  local _max_attempts=${7:-60}

  for i in $( seq 1 "${_max_attempts}" ); do
    _msg_info "[${i}/${_max_attempts}] waiting for '${_reason}' (${_type}) event to occur ${_n} time(s) ..."
    local _events=$( kubectl events \
                             -n "${_namespace}" \
                             --for "${_object}" \
                             --types "${_type}" \
                             -o yaml | \
                       yq ".items.[] | select(.reason == \"${_reason}\") | .count" )

    # Do we have the correct match?
    for count in ${_events}; do
      if [ ${count} -eq ${_n} ]; then
        _msg_info "received ${_n} time(s) '${_reason}' event(s)"
        return
      fi
    done
    sleep "${_poll_sec}"
  done

  _msg_error "did not receive any '${_reason}' event(s)" 1
}

# Returns the size of a given PVC
#
# $1: pvc name
# $2: namespace (defaults to "default")
function _pvc_capacity() {
  local _pvc_name="${1}"
  local _namespace=${2:-default}

  kubectl --namespace "${_namespace}" get pvc "${_pvc_name}" -o yaml | \
    yq '.status.capacity.storage'
}

# Ensure that the size of the given PVC matches the given value
#
# $1: pvc name
# $2: namespace
# $3: capacity
function _ensure_pvc_capacity() {
  local _pvc_name="${1}"
  local _namespace="${2}"
  local _want_capacity="${3}"
  local _got_capacity=$( _pvc_capacity "${_pvc_name}" "${_namespace}" )

  # retry 1 time on fail
  for attempt in 1 2; do
    local _got_capacity=$( _pvc_capacity "${_pvc_name}" "${_namespace}" )
    
    if [ "${_want_capacity}" = "${_got_capacity}" ]; then
      return 0
    fi
    
    if [ ${attempt} -eq 1 ]; then
      sleep 30
    else
      _msg_error "pvc ${_namespace}/${_pvc_name} capacity is ${_got_capacity} (want ${_want_capacity})" 1
    fi
  done
}

# Cleanup unused resources
#
# $1: pod name
# $2: pvc name
# $3: pvca name
# $4: namespace
function _cleanup() {
  local _pod_name="${1}"
  local _pvc_name="${2}"
  local _pvca_name="${3}"
  local _namespace="${4}"

  kubectl --namespace "${_namespace}" delete pod "${_pod_name}"
  kubectl --namespace "${_namespace}" delete pvc "${_pvc_name}"
  kubectl --namespace "${_namespace}" delete pvca "${_pvca_name}"
}

export_artifacts() {
  cluster_name="${1}"
  echo "> Exporting logs of kind cluster '$cluster_name'"
  kind export logs "${ARTIFACTS:-}/$cluster_name" --name "$cluster_name" || true

  echo "> Exporting events of kind cluster '$cluster_name' > '$ARTIFACTS/$cluster_name'"
  export_events_for_cluster "$ARTIFACTS/$cluster_name"

  export_resource_yamls_for pods pvcas
}

export_resource_yamls_for() {
  mkdir -p $ARTIFACTS
  # Loop over the resource types
  for resource_type in "$@"; do
    echo "> Exporting Resource '$resource_type' yaml > $ARTIFACTS/$resource_type.yaml"
    echo -e "---\n# cluster name: '${cluster_name:-}'" >> "$ARTIFACTS/$resource_type.yaml"
    kubectl get "$resource_type" -A -o yaml >> "$ARTIFACTS/$resource_type.yaml" || true
  done
}

export_events_for_cluster() {
  local dir="$1/events"
  mkdir -p "$dir"

  while IFS= read -r namespace; do
    kubectl -n "$namespace" get event --sort-by=lastTimestamp >"$dir/$namespace.log" 2>&1 || true
  done < <(kubectl get ns -oname | cut -d/ -f2)
}

clamp_mss_to_pmtu() {
  # https://github.com/kubernetes/test-infra/issues/23741
  if [[ "$OSTYPE" != "darwin"* ]]; then
    iptables -t mangle -A POSTROUTING -p tcp --tcp-flags SYN,RST SYN -j TCPMSS --clamp-mss-to-pmtu
  fi
}