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

  echo -e "[$( date +%Y-%m-%d-%H:%M:%S)] ${_SCRIPT_NAME} ${_GREEN}INFO${_NO_COLOR}: ${_msg}"
}

# Display an ERROR message
#
# $1: Message to display
# $2: Exit code
function _msg_error() {
  local _msg="${1}"
  local _rc=${2}

  echo -e "[$( date +%Y-%m-%d-%H:%M:%S)] ${_SCRIPT_NAME} ${_RED}ERROR${_NO_COLOR}: ${_msg}"

  if [[ ${_rc} -ne 0 ]]; then
    exit ${_rc}
  fi
}
