# Karakoram dashboards

Import any JSON file in `dashboards/` with Grafana's dashboard import screen,
then select a Prometheus datasource. The dashboards require Grafana's JSON
schema version 39 and use the `datasource` variable rather than a fixed UID.
For file provisioning, mount `dashboards/` into Grafana and point a dashboard
provider's `options.path` at that directory. Provision a Prometheus datasource
separately; no datasource credentials are included here.

Prometheus must scrape the control plane's authenticated root `/metrics`
endpoint over verified TLS with a bearer token authorized for the intended
clusters (`monitoring:read`) and installation signals (`settings:read`). Enable
cluster member monitoring for engine metrics. State metrics do not require
member monitoring. Namespace, cluster and member selectors come from
`karakoram_member_scrape_success`; use **All** to include state-only clusters.
Missing series and gaps mean **unknown**, not a healthy zero. Some engine
metrics are role-specific. A timestamp of zero means no recorded result.

Load `../prometheus/karakoram-rules.yaml` through Prometheus `rule_files`, then
reload Prometheus. Route its `severity` labels in Alertmanager. Alert annotations
link to operational runbooks. Review thresholds before enabling notifications:
backup freshness assumes daily schedules (26 hours), drill freshness assumes
weekly schedules (eight days), and outdated agents warn after 24 hours. Enabled
schedules that have never succeeded are stale. Failure counters use reset-aware
rates/increases; Elasticsearch cluster-wide values use `max`, not member sums.
PostgreSQL primary-observed replication lag includes transport and replay;
replica replay backlog excludes transport lag. RabbitMQ running-member counts
measure distribution links, not quorum or independent placement guarantees.
