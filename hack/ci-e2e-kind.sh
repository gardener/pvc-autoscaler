#!/usr/bin/env bash

# SPDX-FileCopyrightText: SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

set -o nounset
set -o pipefail
set -o errexit

REPO_ROOT="$(readlink -f $(dirname ${0})/..)"
source "${REPO_ROOT}"/hack/common.sh

clamp_mss_to_pmtu

make kind-up

trap "
  ( export_artifacts "pvc-autoscaler" )
  ( make pvc-autoscaler-down )
  ( make kind-down )
" EXIT

make pvc-autoscaler-up-e2e
make test-e2e-local
