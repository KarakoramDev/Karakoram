<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Monitoring and events

Use built-in charts for short-term inspection, authenticated [Prometheus](https://prometheus.io/) scraping for external retention/alerting, and durable events/status for operational progress.

## Built-in dashboard

Enable `spec.monitoring.enabled` and open **Cluster → Monitoring**. The CLI create convenience path enables monitoring by default. The control plane scrapes agents every **30 seconds** and retains **24 hours** of persisted samples by default. Control-plane downtime interrupts collection/aggregation; this is not independent availability monitoring.

Collection uses at most eight concurrent member requests, each bounded by the
scrape interval (at most 30 seconds). A slow member does not serialize every
other target behind its timeout. Retention pruning runs independently of new
ingestion, including when discovery or metric insertion fails. Storage failures
are logged and retried; fresh in-memory usage/federation observations remain
available even when their historical samples cannot be persisted. Pruning can
still fail on a completely full or read-only filesystem: restore writable
headroom rather than deleting the database or WAL.

Global retention lookups use a timestamp index. Each sweep deletes at most
4,096 expired rows in transactions of at most 256 rows, yielding the writer
between batches and using a two-second admission/execution context. Saturated
or deadline-limited sweeps wake one coalescing maintenance worker, which starts
another bounded sweep after yielding for 25 ms; it does not wait another
30-second scrape interval. The worker stops and is joined on collector shutdown.
This removes a fixed 4,096-rows-per-interval ceiling, but retention remains
best-effort under writer contention, slow disks, or sustained ingestion beyond
available delete throughput; 24 hours is not a hard storage-size guarantee.
Slow writer admission in retention and token-usage maintenance is logged after
two seconds with the operation name; it does not expose SQL parameters or
credentials. A one-second token-usage admission timeout normally completes
before that warning threshold.
HTTP requests exceeding two seconds also emit a warning with request ID,
method, matched route pattern and elapsed duration; raw URLs and query values
are not logged. These completion diagnostics cannot replace a failure-time
goroutine dump when a request never returns.

### Upgrading an existing metrics database

Migration `011_metrics_retention.sql` creates `(timestamp,id)` in one [SQLite](https://sqlite.org/)
transaction with its migration marker. Building it scans and sorts existing
metrics: startup time and temporary/WAL disk headroom depend on retained rows
and storage speed, not just the current cluster count. The control plane opens
its listeners only after all migrations finish; `/healthz` and the API are
unavailable during the build. Do not bypass this by deleting state or reporting
healthy before migration completes.

Before a large-table upgrade, preserve a consistent database/WAL backup and
master key, measure the upgrade against a restored copy on comparable storage
in an isolated environment with no access to member/platform endpoints, and
allow sufficient free disk and startup time. [Zerops](https://zerops.io/)'s supplied readiness
`failureTimeout` is **120 seconds**: increase it before deploying if the measured
migration plus normal startup can exceed that window. The supplied [Nomad](https://developer.hashicorp.com/nomad) job
uses `health_check = "task_states"`; a running allocation is **not** API readiness.
Wait explicitly for `/healthz` and `/api/v1/version` after deployment rather than
treating allocation/deployment status as proof that migration completed.

A failed or interrupted index transaction does not leave a committed migration
marker; reopening retries it. Preserve authoritative state and repair disk/write
access rather than repeatedly killing an index build. An older binary whose
schema includes migrations through `010_lifecycle_recovery.sql` can reopen this
additive index and ignores the unknown `011` marker; its existing metric SQL
remains compatible. This statement is specific to `011`, not a guarantee that
every other schema or durable-state change supports downgrade. Keep a single
active control-plane writer; rolling back the binary also restores that binary's
older, unbounded retention implementation.

```sh
karactl -n default clusters metrics demo --range 1h --step 30s -o json
karactl -n default clusters members demo
```

The UI offers 15m/1h/6h/24h ranges with 15s/30s/2m/10m steps. The `range` URL parameter preserves selection; default is 1h. Buckets average sampled values: fractional state codes can represent mixed states, not a new state. Counter series become reset-aware rates; missing data is not healthy zero.

Namespaces have independent histories even for same-named clusters. Select the namespace explicitly; omitted namespace selects `default`. Agent connectivity, member status and engine health remain separate signals: exporter reachability does not prove quorum or successful application work.

Disable collection with a merge patch setting `spec.monitoring.enabled: false`; history remains until retention prunes it.

## Metrics API and SDK

`GET /api/v1/clusters/{cluster}/metrics?namespace=<name>&range=1h&step=30s` returns JSON `series` with member/name/unit/labels and timestamped points. Range/step use [Go](https://go.dev/) durations: positive range ≤24h; positive step ≤range. Defaults are 1h and 30s. The [Go client walkthrough](api.md#clientssdk) supplies authentication/client setup.

Historical JSON is distinct from installation-root `/metrics` Prometheus text. A generated SDK client for root endpoints needs the origin, not `/api/v1`, as its base URL. Empty results are not evidence of health.

## Prometheus scraping

Scrape **`GET /metrics` at the installation root**, not `/api/v1/metrics`. Metrics carry authoritative `namespace`, `cluster` and `member` labels; include namespace in queries/alert grouping. `set`/`role` are not guaranteed metric labels.

Use a **Karakoram API token**, not a provider infrastructure credential. Its owner needs `monitoring, read, <namespace>/<cluster>`; individual objects are filtered by policy. Without `namespace`, all authorized namespaces are included; `?namespace=<name>` restricts the scrape. A signed-in browser's access cookie also works, but unattended scrapers should use bearer tokens, not expiring browser cookies.

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
      - targets: ["karakoram.example.com:443"]
```

Mount an owner-readable token-only credentials file from a secret manager; do not embed tokens in shared YAML. Keep server-certificate verification enabled; configure CA trust for a private issuer rather than `insecure_skip_verify`. `karactl metrics` reads this endpoint too.

The endpoint serves current scrape snapshots, not an indefinitely renewable
cache. A failed member scrape omits that exporter's series and exposes
`karakoram_member_scrape_success=0`. Failed target discovery immediately
invalidates cached observations for all previously known targets. Federation
also checks sample age on every request: after **two collector intervals**
(**60 seconds** at the default 30-second cadence), or for missing/future sample
timestamps, exporter series are absent and scrape success is zero. Repeated
Prometheus requests cannot renew that lease while collection is stalled.
Previously persisted historical series are unchanged. This metric lease is
separate from the cluster API's 15-second readiness lease; exporter reachability
does not prove application availability or quorum. Use external retention and
independent application checks for alerting.

## Control-plane state metrics

The same `/metrics` response includes durable cluster state even when
`spec.monitoring.enabled` is false or the member collector is unavailable.
Cluster series require `monitoring:read` on each namespace-qualified cluster.
Installation-wide backup, notification and certificate series additionally
require `settings:read` on `*`; a cluster monitoring grant does not grant that
visibility. An explicit `?namespace=` selection excludes installation-wide
series. State inventory read errors fail the scrape rather than reporting
healthy zeroes. These observations reflect stored state, not independent live
database probes.

Every cluster series has `namespace` and `cluster`; extra labels are listed
below. All are gauges except the notification failure counter.

| Metric | Extra labels | Meaning |
| --- | --- | --- |
| `karakoram_cluster_info` | `provider`, `version`, `phase` | Value 1 with observed engine version and stored phase. |
| `karakoram_cluster_ready` | — | 1 only for Ready; suspended, creating and deleting clusters also report 0. |
| `karakoram_cluster_degraded` | — | 1 for Degraded or Error. |
| `karakoram_cluster_condition` | `type`, `status`, `reason` | Value 1 identifies a current condition, not its health polarity. |
| `karakoram_cluster_members_co_located` | — | 1 when multiple members share a known observed placement group; 0 does not guarantee anti-affinity. |
| `karakoram_member_agent_outdated` | `member` | 1 for a member reported outdated; unknown agent builds are not asserted outdated. |
| `karakoram_backup_last_success_timestamp_seconds` | `schedule` | Latest successful completion; one series per enabled schedule plus `schedule=""` for all backups, including manual ones. |
| `karakoram_backup_last_failure_timestamp_seconds` | — | Latest failed backup completion. |
| `karakoram_certificate_expiry_timestamp_seconds` | `certificate` | Certificate expiry; installation certificates use empty cluster identity. |
| `karakoram_backup_drill_enabled` | — | Whether scheduled drills are enabled. |
| `karakoram_backup_drill_last_success_timestamp_seconds` | — | Latest successful drill completion. |
| `karakoram_backup_drill_last_failure_timestamp_seconds` | — | Latest failed drill completion. |
| `karakoram_backup_drill_last_restore_duration_seconds` | — | Restore duration from the latest successful report, not the longest or latest failed report. |
| `karakoram_deployment_retries_exhausted` | — | 1 while a deployment is terminally fenced pending a desired-state change or explicit retry. |

Installation metrics have no cluster labels:
`karakoram_control_plane_backup_enabled`,
`karakoram_control_plane_backup_last_success_timestamp_seconds`,
`karakoram_control_plane_backup_last_failure_timestamp_seconds`, and
`karakoram_notification_deliveries_failed_total{channel}`. Notification counts
include deliveries that exhausted retries, not transient failed attempts; use
`increase()` or `rate()` to handle counter resets. Channel credentials and
delivery destinations are never metric labels.

Timestamp gauges are Unix seconds; zero means no recorded terminal result, not
the current time or healthy state. Pending/in-progress records cannot renew a
success timestamp. Backup completion freshness differs from the backup's data
recovery point. Results summarize retained durable inventory: deleting backup
records or rotating drill-report retention can remove old evidence. Conditions
are ternary and condition-specific: `True` can mean a fault, so inspect `type`
and `reason` rather than treating every true condition as healthy.

## Grafana dashboards and Prometheus alerts

Import the supplied JSON dashboards from
[`deploy/grafana/dashboards`](../deploy/grafana/dashboards):
`overview.json`, `mongodb.json`, `rabbitmq.json`, `elasticsearch.json`,
`postgres.json` and `supabase.json`. Select a Prometheus datasource through the
`datasource` variable; no fixed datasource UID or credentials are embedded.
For file provisioning, configure a [Grafana](https://grafana.com/) dashboard provider for the mounted
dashboard directory and provision the datasource separately. Namespace,
cluster and member selectors are populated from member scrape series: select
**All** to include state-only clusters that do not have member monitoring.

The overview covers readiness, conditions, backup/drill freshness, certificate
lifetime, placement, agent drift, deployment failures and installation signals.
Engine dashboards use native metric semantics: reset-aware `rate()`/`increase()`
for counters, `max` rather than sums for [Elasticsearch](https://www.elastic.co/elasticsearch)'s repeated cluster-wide
values, and separate [PostgreSQL](https://www.postgresql.org/) primary-observed replication lag from
replica-local replay backlog. Missing series are unknown, not healthy zeroes.

Load [`deploy/prometheus/karakoram-rules.yaml`](../deploy/prometheus/karakoram-rules.yaml)
through Prometheus `rule_files`, reload Prometheus, and route its `severity`
labels with [Alertmanager](https://prometheus.io/docs/alerting/latest/alertmanager/). Give the scrape token both cluster monitoring and
installation settings read permissions if installation alerts are wanted.
Alert annotations link to the relevant operational runbooks.

Review thresholds before enabling paging: backup freshness assumes daily
schedules (26 hours), drill freshness assumes weekly schedules (eight days),
certificate warnings start at 14 days and become critical at three days, and
agent drift warns after 24 hours. Enabled schedules with no successful result
are stale. Disable policies intentionally rather than silencing failing runs.
Cluster-not-ready alerts can include planned suspension or provisioning.
Monitor Prometheus's own scrape `up` signal too: missing state metrics after a
failed scrape must not be interpreted as recovery.

For a degraded cluster, inspect conditions and recent operations before
restarting members. For backup/drill alerts, inspect completed reports,
verification checks, cleanup inventory and storage access; backup process
availability alone does not establish recoverability. For deployment retry
exhaustion, correct the provider error before explicitly retrying. Certificate
alerts require reviewing certificate identity and renewal conditions, not
disabling TLS verification.


## Engine metric vocabularies

Metric names, units, availability and engine-specific health interpretation live in:

- [MongoDB operations](mongodb/operations.md)
- [RabbitMQ operations](rabbitmq/operations.md)
- [Elasticsearch operations](elasticsearch/operations.md)
- [Neo4j Community operations and metrics](neo4j/operations.md#monitoring): authenticated Query API/JVM observations and filesystem store size, not an Enterprise Prometheus endpoint; all ten `neo4j_*` series are gauges.
- [Weaviate operations and exact metric vocabulary](weaviate/operations.md#metrics): availability, object/shard/collection counts, query/request/batch counters, vector-index capacity/tombstones, LSM bytes and async queue length; native 2112 is private and wildcard-bound.

Use `rate()` for counters, not gauges; distinguish process availability from recovery coverage and application correctness.

### PostgreSQL

PostgreSQL members expose the following `karakoram_postgres_` metrics. Database
series have a `database` label; [ProxySQL](https://proxysql.com/) pool series have `host` and `hostgroup`
labels. Counters reset when their underlying PostgreSQL statistics or ProxySQL
process resets. An unavailable SQL or [Patroni](https://patroni.readthedocs.io/) endpoint omits its series rather
than reporting a healthy zero.

| Suffix | Type | Meaning |
| --- | --- | --- |
| `transactions_committed_total`, `transactions_rolled_back_total` | Counter | Database transactions committed or rolled back. |
| `blocks_read_total`, `blocks_hit_total` | Counter | Database disk blocks read or found in the buffer cache. |
| `tuples_returned_total`, `tuples_fetched_total`, `tuples_inserted_total`, `tuples_updated_total`, `tuples_deleted_total` | Counter | Database tuple activity from `pg_stat_database`. |
| `connections`, `max_connections` | Gauge | Current database providers and the server connection limit. |
| `database_size_bytes` | Gauge | Total size of each database. |
| `replication_lag_bytes`, `replay_backlog_bytes` | Gauge | Primary-observed transport-plus-replay lag and replica-local received-but-not-replayed WAL respectively. Replica backlog excludes WAL that has not arrived. |
| `archiver_failed_total` | Counter | Failed WAL archive attempts. |
| `last_archived_age_seconds` | Gauge | Age of the last successful WAL archive; zero before the first archive. Correlate with archive status and backup configuration. |
| `patroni_primary`, `patroni_timeline` | Gauge | Patroni's local primary role (1/0) and PostgreSQL timeline on HA members. |
| `proxysql_connections_used`, `proxysql_connections_free` | Gauge | Used and idle provider pool connections. |
| `proxysql_queries_total` | Counter | Queries sent through each provider pool. |

PostgreSQL readiness means that local SQL accepts connections; proxy readiness
means that its frontend listener is open and its runtime writer hostgroup has an
ONLINE provider at observation time. Neither is a quorum guarantee, and a ready
proxy does not make the next connection infallible: clients keep reconnect and
retry logic after member failures. ProxySQL defaults to writer-only routing.
Optional read/write split rules are deliberately simple and do not guarantee
causal reads or classify every SQL construct. Native replication, LISTEN/NOTIFY
and other session-sensitive protocols should connect directly to PostgreSQL.

## Events and event streams

```sh
karactl -n default events list --cluster demo
karactl -n default events watch --cluster demo
karactl -n default operations cluster demo
```

Persisted events and operations support incident review; streams provide timely notification, not authoritative completion. Preserve replay position and filters when reconnecting. Event categories such as `Warning` differ from lifecycle reasons such as `OperationFailed`; use the [API pagination/event contract](api.md#pagination-and-events) for filtering, SSE and proxy behavior. Audit scope/coverage is defined in [Security](security.md#audit-attribution-and-durability).

To query [Data MCP](data-mcp.md) audit events, use
`karactl -n default events list --cluster demo --resource cluster-data` or
`GET /api/v1/events?namespace=default&cluster=demo&resource=cluster-data`.
The same resource filter is supported by event streams. Reading these events
requires `events:read` or `audit:read` on the cluster; `cluster-data:read` alone
does not grant audit visibility. Explicit unauthorized resource filters return
403, while unfiltered listings omit unauthorized events.

## Troubleshooting

| Symptom | Check/action |
| --- | --- |
| 403 scrape | Account/token status and per-cluster `monitoring:read` permission. |
| Empty charts | Enabled collection, time range, enough counter samples, agent connectivity and engine monitor credentials/CA. |
| Stale samples | Latest successful observation, failed scrapes and independent availability checks. |
| One namespace unavailable | Project token and [relay health](namespaces.md#relay-health); unrelated namespaces continue. |
| Remote scrape/config pull fails | Public-origin reachability and namespace-qualified routing; never disable CA/hostname checks. |
| Memory near limit | Inspect complete runtime allocation/process usage; a process RSS series is not total container memory. |
