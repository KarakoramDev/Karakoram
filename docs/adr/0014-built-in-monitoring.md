# 0014 — Built-in agent metrics, Prometheus output and UI charts

Status: Accepted

## Context

Per-member visibility requires [MongoDB](https://www.mongodb.com) application telemetry in addition to [Zerops](https://zerops.io) platform metrics. [OpenEverest](https://openeverest.io)'s [PSMDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) monitoring integration uses an independently operated [PMM](https://docs.percona.com/percona-monitoring-and-management/) server and member-side PMM clients, not a standalone exporter endpoint.

## Decision

Collect MongoDB metrics in the node agent through its local driver using the purpose-specific `clusterMonitor` account. Expose [Prometheus](https://prometheus.io) text through authenticated private agent `/v1/metrics`. The control plane scrapes/aggregates member samples, retains bounded UI time series and serves:

- `/api/v1/clusters/{c}/metrics?range=` for the UI dashboard;
- `/metrics` as a Prometheus-compatible endpoint for external monitoring, protected by Bearer/API-token authentication and monitoring RBAC.

Show member role/health, replication lag, uptime and resource/disk observations alongside charted metrics. Include authoritative namespace/cluster/member labels and honor each cluster's monitoring setting. Namespace-qualified history/cache keys isolate identically named clusters; remote scrapes use their namespace relay (ADR 0018). Cluster JSON routes default to `default`; root `/metrics` includes all authorized namespaces unless filtered by `namespace`. Apply per-object `<namespace>/<cluster>` authorization to aggregation. Unavailable or stale data is marked as such, not reported as a healthy zero. Monitoring failures do not redefine database readiness or substitute for direct engine health checks.

Defer PMM integration; it is not required or shipped in the initial supported monitoring path. The agent exporter, public scrape endpoint and UI charts are required, not deferred along with PMM.

## Consequences

A self-contained installation satisfies dashboard/exporter needs without another server or PMM client distribution. Operators can integrate their own Prometheus-compatible tooling. Built-in short-term samples are not an HA, long-retention monitoring platform; control-plane downtime interrupts aggregation while databases keep running. Rich PMM query analytics and its wider workflows are not claimed. Metric cardinality, retention and stale-sample handling need explicit bounds.

## Alternatives considered

- **PMM server plus clients:** richer analysis and descriptive OpenEverest parity, but adds service credentials, deployment cost and another operational dependency.
- **External exporter only:** usable Prometheus integration, but leaves the required self-contained UI dashboard disconnected.
- **Platform metrics alone:** cannot supply replica-set/application-level health.
