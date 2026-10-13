<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Monitoring

Karakoram ships a node-side MongoDB exporter, a built-in dashboard and an authenticated Prometheus endpoint. **PMM server/client integration and query analytics are not shipped**; see [monitoring ADR 0014](../adr/0014-built-in-monitoring.md).

## Built-in dashboard

Enable cluster `spec.monitoring.enabled` (the CLI create convenience path enables it by default) and open the cluster's **Monitoring** tab. Agents query MongoDB with `clusterMonitor` and collect process/memory/filesystem observations. The control plane defaults to scraping every **30 seconds** and retaining **24 hours** of persisted UI samples. This is short-term visibility, not an HA monitoring platform.

```sh
karactl clusters metrics demo --range 1h --step 30s -o json
karactl clusters members demo
```

Ranges/steps are Go-duration forms such as `1h`, `30m`, `30s`; the metrics API permits a positive range at most 24 hours and a positive step no greater than the range. The default range is one hour, step thirty seconds. JSON `series` has member/name/unit/labels and timestamped points. Counter series are converted to reset-aware rates; a graph missing data is not evidence of a healthy zero.

Clusters with the same name in different namespaces have independent metric history and member usage. Select the cluster's namespace for JSON queries (`?namespace=<name>` in the API, or `karactl --namespace <name> clusters metrics ...`); omitted namespace selects `default`.

Member status/roles/lag and agent connectivity remain separate from charts. Monitoring failure cannot prove the database is unavailable, and a connected exporter cannot prove quorum. Normal database health uses authenticated provider observation; platform exec readiness only probes local TCP availability.

Disable collection using an API-shaped merge patch (`spec.monitoring.enabled: false`). Historical samples persist until retention prunes them; do not expect disabling monitoring to delete all history instantly. Control-plane unavailability interrupts scraping/aggregation even while databases continue.

## Prometheus scraping

The public endpoint is **`GET /metrics` at the installation root**, not `/api/v1/metrics`. It exports Prometheus text with authoritative `namespace`, `cluster` and `member` labels. Obtain a Karakoram API token for an account permitted to `read` `monitoring`; the server filters individual `<namespace>/<cluster>` objects by the same policy. It is not a Zerops integration token.

Without a `namespace` query parameter the endpoint includes all authorized namespaces. Set `?namespace=<name>` to restrict the scrape to one namespace.

```yaml
scrape_configs:
  - job_name: karakoram
    scheme: https
    metrics_path: /metrics
    scrape_interval: 30s
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/karakoram-token
    static_configs:
      - targets: ["YOUR-SUBDOMAIN:443"]
```

Store only the token value in the owner-readable credentials file, mounted from your secret manager; do not put it into a shared scrape YAML. Normal server-certificate validation remains enabled. For a privately signed HTTPS origin configure the appropriate Prometheus CA trust, not `insecure_skip_verify`.

`karactl metrics` fetches the same root endpoint. The endpoint presents the control plane's latest **successful** scrape snapshots; failed scrapes can leave a prior sample visible until another success or target removal. Do not treat its presence alone as a live heartbeat. Correlate database `up`, agent/status connectivity, observation timestamps and missing samples. External Prometheus is recommended for durable alerting/retention and independent availability checks.

## Metric vocabulary

Every public metric includes `namespace`, `cluster` and `member`. Include the namespace in queries and alert grouping to distinguish identically named clusters. Additional `operation`/`direction` labels are noted below; `set`/`role` are member-status properties, not guaranteed metric labels. Availability-dependent metrics may be absent (mongos has no replica-set oplog or WiredTiger cache).

| Prometheus metric | Type/unit | JSON series / meaning |
| --- | --- | --- |
| `karakoram_mongodb_up` | Gauge, 0/1 | `up`: local monitor authenticated `serverStatus` success |
| `karakoram_mongodb_uptime_seconds` | Gauge, seconds | `uptime`: MongoDB process uptime |
| `karakoram_opcounters_total` | Counter, operations | `opcounters.total`: aggregate rate, operations/second in JSON |
| `karakoram_mongodb_opcounters_total` | Counter, operations; `operation` | `opcounters.byOperation`: insert/query/update/delete/getmore/command rates |
| `karakoram_connections_current` | Gauge, count | `connections.current`: open connections |
| `karakoram_connections_available` | Gauge, count | `connections.available`: available connections |
| `karakoram_repl_lag_seconds` | Gauge, seconds | `repl.lagSeconds`: difference from latest visible set optime |
| `karakoram_repl_member_state` | Gauge, state code | `repl.memberState`: MongoDB `myState` numeric value |
| `karakoram_oplog_window_seconds` | Gauge, seconds | `oplog.windowSeconds`: local oplog first-to-last time span, **not PBM PITR coverage** |
| `karakoram_cache_used_bytes` | Gauge, bytes | `cache.usedBytes`: WiredTiger cache use |
| `karakoram_cache_max_bytes` | Gauge, bytes | `cache.maxBytes`: configured WiredTiger cache maximum |
| `karakoram_process_cpu_seconds_total` | Counter, CPU seconds | `cpu.percent`: process CPU-time rate ×100, clamped 0–100 in JSON |
| `karakoram_ram_used_bytes` | Gauge, bytes | `ram.usedBytes`: mongod/mongos RSS, not total cgroup/PBM memory |
| `karakoram_ram_total_bytes` | Gauge, bytes | `ram.totalBytes`: available memory bound using proc/cgroup observations |
| `karakoram_disk_used_bytes` | Gauge, bytes | `disk.usedBytes`: filesystem use for DATA_DIR |
| `karakoram_disk_total_bytes` | Gauge, bytes | `disk.totalBytes`: DATA_DIR filesystem capacity |
| `karakoram_pbm_agent_up` | Gauge, 0/1 | `pbm.up`: supervised PBM process running, **not PBM registration/backup health** |
| `karakoram_mongodb_documents_total` | Counter, documents; `operation` | `documents.total`: inserted/updated/deleted/returned rates |
| `karakoram_mongodb_network_bytes_total` | Counter, bytes; `direction` | `network.bytes`: bytesIn/bytesOut rates |

Use `rate(karakoram_opcounters_total[5m])` for operation throughput in Prometheus; do not apply `rate` to gauges. `rate(karakoram_process_cpu_seconds_total[5m]) * 100` is raw process CPU percent and can differ from the clamped UI series on multi-core work. Oplog duration and PBM PITR windows describe different recovery guarantees.

## Troubleshooting

- **403 scrape:** check per-cluster `monitoring, read, <namespace>/<cluster>` policy and account/token status.
- **Empty charts:** check enabled collection, connected agents, time range, monitor credentials/CA and enough samples for a counter rate.
- **Namespace unavailable:** check that namespace's project token and relay connectivity. A namespace routing failure degrades only its clusters, emits a namespaced `NamespaceUnavailable` event, and retries with backoff; collection and reconciliation in other namespaces continue.
- **PBM up but no backups:** the metric indicates a supervised process only; inspect PBM registration and backup/error records.
- **Memory near bound:** RSS excludes other processes; inspect Zerops allocation/runtime logs before concluding the complete container fits.

See [operations](../operations.md), [RBAC](rbac-and-auth.md) and [backups/PITR](backups-and-restore.md).
