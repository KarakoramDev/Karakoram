<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Monitoring and events

Use built-in charts for short-term inspection, authenticated Prometheus scraping for external retention/alerting, and durable events/status for operational progress.

## Built-in dashboard

Enable `spec.monitoring.enabled` and open **Cluster → Monitoring**. The CLI create convenience path enables monitoring by default. The control plane scrapes agents every **30 seconds** and retains **24 hours** of persisted samples by default. Control-plane downtime interrupts collection/aggregation; this is not independent availability monitoring.

```sh
karactl -n default clusters metrics demo --range 1h --step 30s -o json
karactl -n default clusters members demo
```

The UI offers 15m/1h/6h/24h ranges with 15s/30s/2m/10m steps. The `range` URL parameter preserves selection; default is 1h. Buckets average sampled values: fractional state codes can represent mixed states, not a new state. Counter series become reset-aware rates; missing data is not healthy zero.

Namespaces have independent histories even for same-named clusters. Select the namespace explicitly; omitted namespace selects `default`. Agent connectivity, member status and engine health remain separate signals: exporter reachability does not prove quorum or successful application work.

Disable collection with a merge patch setting `spec.monitoring.enabled: false`; history remains until retention prunes it.

## Metrics API and SDK

`GET /api/v1/clusters/{cluster}/metrics?namespace=<name>&range=1h&step=30s` returns JSON `series` with member/name/unit/labels and timestamped points. Range/step use Go durations: positive range ≤24h; positive step ≤range. Defaults are 1h and 30s. The [Go client walkthrough](api.md#clientssdk) supplies authentication/client setup.

Historical JSON is distinct from installation-root `/metrics` Prometheus text. A generated SDK client for root endpoints needs the origin, not `/api/v1`, as its base URL. Empty results are not evidence of health.

## Prometheus scraping

Scrape **`GET /metrics` at the installation root**, not `/api/v1/metrics`. Metrics carry authoritative `namespace`, `cluster` and `member` labels; include namespace in queries/alert grouping. `set`/`role` are not guaranteed metric labels.

Use a **Karakoram API token**, not the Zerops integration token. Its owner needs `monitoring, read, <namespace>/<cluster>`; individual objects are filtered by policy. Without `namespace`, all authorized namespaces are included; `?namespace=<name>` restricts the scrape. A signed-in browser's access cookie also works, but unattended scrapers should use bearer tokens, not expiring browser cookies.

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

The endpoint serves the latest **successful** scrape snapshot. Failed scrapes can leave an older sample visible until success or target removal. Correlate freshness, status/agent connectivity and independent application checks; sample presence alone is not a heartbeat. Use external retention and alerting.

## Engine metric vocabularies

Metric names, units, availability and engine-specific health interpretation live in:

- [MongoDB operations](mongodb/operations.md)
- [RabbitMQ operations](rabbitmq/operations.md)
- [Elasticsearch operations](elasticsearch/operations.md)

Use `rate()` for counters, not gauges; distinguish process availability from recovery coverage and application correctness.

## Events and event streams

```sh
karactl -n default events list --cluster demo
karactl -n default events watch --cluster demo
karactl -n default operations cluster demo
```

Persisted events and operations support incident review; streams provide timely notification, not authoritative completion. Preserve replay position and filters when reconnecting. Event categories such as `Warning` differ from lifecycle reasons such as `OperationFailed`; use the [API pagination/event contract](api.md#pagination-and-events) for filtering, SSE and proxy behavior. Audit scope/coverage is defined in [Security](security.md#audit-attribution-and-durability).

## Troubleshooting

| Symptom | Check/action |
| --- | --- |
| 403 scrape | Account/token status and per-cluster `monitoring:read` permission. |
| Empty charts | Enabled collection, time range, enough counter samples, agent connectivity and engine monitor credentials/CA. |
| Stale samples | Latest successful observation, failed scrapes and independent availability checks. |
| One namespace unavailable | Project token and [relay health](namespaces.md#relay-health); unrelated namespaces continue. |
| Remote scrape/config pull fails | Public-origin reachability and namespace-qualified routing; never disable CA/hostname checks. |
| Memory near limit | Inspect complete runtime allocation/process usage; a process RSS series is not total container memory. |
