# Configuration

This document describes how to configure the `pvc-autoscaler` controller itself.
To configure autoscaling for a workload, create a `PersistentVolumeClaimAutoscaler` resource as described in the [README](../../README.md#usage).

## Command-line flags

| Flag | Default | Description |
|:-----|:--------|:------------|
| `--prometheus-address` | `http://localhost:9090` | Address of the Prometheus API used as metrics source. |
| `--metrics-available-bytes-query` | `kubelet_volume_stats_available_bytes` | Query for the available bytes of each PVC. See [Metrics source queries](#metrics-source-queries). |
| `--metrics-capacity-bytes-query` | `kubelet_volume_stats_capacity_bytes` | Query for the capacity in bytes of each PVC. |
| `--metrics-available-inodes-query` | `kubelet_volume_stats_inodes_free` | Query for the free inodes of each PVC. |
| `--metrics-capacity-inodes-query` | `kubelet_volume_stats_inodes` | Query for the total inodes of each PVC. |
| `--interval` | `5m` | Interval of the periodic check that fetches metrics, calculates recommendations, and resizes PVCs. |
| `--autoscaler-name` | `""` | Only reconcile `PersistentVolumeClaimAutoscaler` resources whose `.spec.autoscalerName` matches this value. An empty value reconciles resources without `.spec.autoscalerName`. |
| `--leader-elect` | `false` | Enable leader election, so that only one replica is active. |
| `--leader-election-id` | `2b09b108.gardener.cloud` | Name of the resource that holds the leader lock. |
| `--metrics-bind-address` | `:8080` | Address the controller's own metrics endpoint binds to. |
| `--metrics-secure` | `false` | Serve the metrics endpoint over HTTPS. |
| `--health-probe-bind-address` | `:8081` | Address the `/healthz` and `/readyz` endpoints bind to. |
| `--enable-http2` | `false` | Enable HTTP/2 for the metrics and webhook servers. |

Logging is configured with the controller-runtime zap flags (`--zap-devel`, `--zap-encoder`, `--zap-log-level`, `--zap-stacktrace-level`, `--zap-time-encoding`). Development mode is enabled by default.

The [dist/install.yaml](../../dist/install.yaml) bundle and the [default](../../config/overlays/default) overlay override some defaults, for example `--interval=30s` and `--prometheus-address=http://prometheus-k8s.monitoring.svc.cluster.local:9090`.

## Environment variables

| Variable | Description |
|:---------|:------------|
| `ENABLE_WEBHOOKS` | Set to `false` to skip registering the `PersistentVolumeClaimAutoscaler` validating webhook and the Pod mutating webhook. Offline-resize recovery of the `PreferInPlace` resize strategy relies on the Pod webhook. |

## Metrics source queries

Each query must return an instant vector with at most one sample per PVC.
Every sample must carry the `namespace` and `persistentvolumeclaim` labels, otherwise the periodic check fails with an error such as `metric does not provide namespace label`.
Samples are matched to PVCs by these two labels only.

Override the queries when the metrics source stores series from more than one cluster, for example a shared Thanos, Cortex, or Grafana Mimir.
Without a selector, PVCs with the same namespace and name in different clusters are merged into one entry.

``` yaml
args:
- --prometheus-address=http://mimir-query-frontend.mimir.svc.cluster.local:8080/prometheus
- --metrics-available-bytes-query=max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_available_bytes{cluster="my-cluster"})
- --metrics-capacity-bytes-query=max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_capacity_bytes{cluster="my-cluster"})
- --metrics-available-inodes-query=max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_inodes_free{cluster="my-cluster"})
- --metrics-capacity-inodes-query=max by (namespace, persistentvolumeclaim) (kubelet_volume_stats_inodes{cluster="my-cluster"})
```

The `max by` aggregation keeps one sample per PVC when the same kubelet is scraped more than once, for example by a highly available Prometheus pair.
A path prefix in `--prometheus-address`, such as `/prometheus` above, is kept when building API URLs.
Custom HTTP headers (for example `X-Scope-OrgID`), authentication, and TLS client settings are not configurable.

## Health probes

`/healthz` fails when the periodic check has not completed a run for 5 times `--interval`. A run that ends with an error still counts as completed.
`/readyz` succeeds as soon as the manager is running.
