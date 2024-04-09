#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

#
# A helper script to consume space from a volume in a pod.
#
# Consume 10G of space:
#
# $ env FILE_SIZE=1G NUM_FILES=10 ./consume-pod-space.sh
#
# Consume inodes by creating many small files:
#
# $ env FILE_SIZE=1B NUM_FILES=10000 ./consume-pod-space.sh

set -e

POD=${POD:-test-pod}
NAMESPACE=${NAMESPACE:-default}
POD_PATH=${POD_PATH:-/app}
NUM_FILES=${NUM_FILES:-5}
FILE_SIZE=${FILE_SIZE:-100M}

# The script to run within the pod
_script=$(
  cat <<'EOF'
for i in $( seq 1 ${NUM_FILES} ); do
  _file=$( mktemp --dry-run --tmpdir="${POD_PATH}" )
  dd if=/dev/zero of="${_file}" bs="${FILE_SIZE}" count=1 > /dev/null 2>&1
done
EOF
)

# Run our script
kubectl \
  --namespace "${NAMESPACE}" \
  exec -it "${POD}" -- bash -c "NUM_FILES=${NUM_FILES}; POD_PATH=${POD_PATH}; FILE_SIZE=${FILE_SIZE}; ${_script}"
