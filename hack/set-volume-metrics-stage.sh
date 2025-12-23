#!/bin/bash

# Minimal script to change fake metrics stages
# Usage: ./hack/set-volume-metrics-stage.sh <stage name>

set -euo pipefail

namespace=${namespace:-pvc-autoscaler-system}
deployment_name="pvc-autoscaler-minimal-fake-metrics-server"

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <stage name>"
    exit 1
fi

stage="$1"
echo "Setting stage to: $stage"

# Update pod template annotation to trigger restart and change stage
kubectl patch deployment "$deployment_name" -n "$namespace" -p "
{
  \"spec\": {
    \"template\": {
      \"metadata\": {
        \"annotations\": {
          \"fake-metrics.pvc-autoscaler.io/stage\": \"$stage\"
        }
      }
    }
  }
}"
kubectl rollout status deployment "$deployment_name" -n "$namespace" --timeout=180s

echo "Stage updated to: $stage"
echo "Validating stage change..."
sleep 5

pod=$(kubectl get pods -n "$namespace" -l app=minimal-fake-metrics-server -o jsonpath='{.items[0].metadata.name}')
kubectl port-forward -n "$namespace" pod/"$pod" 9091:8080 &
PF_PID=$!
sleep 2

if curl -s http://localhost:9091/metrics | grep -q "$stage"; then
    echo "Stage validation successful: $stage metrics are being served"
else
    echo "Stage validation failed: $stage metrics not found"
fi

kill $PF_PID 2>/dev/null || true