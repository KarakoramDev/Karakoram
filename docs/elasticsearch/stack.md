<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Kibana, Logstash and APM

Configure the optional components of an [Elasticsearch cluster](README.md).

## Endpoints

Use returned endpoints, not guessed platform addresses. The endpoint names are `elasticsearch`, `kibana`, `apm`, `logstash-beats` and `logstash-http`. Public entries retain the same name and carry `public: true`; there are no `kibana-public` or `apm-public` names. Select the public entry explicitly. See [listeners, network access and credentials](access-and-security.md#connect-and-reveal-credentials).

## Kibana

Open the returned Kibana URL. Initial administration can use `elastic`; use a regular native user with appropriate roles for routine work. Karakoram's UI login is not a Kibana identity. The automatically installed service-account token authenticates Kibana's backend, not browser users.

Set custom settings under `spec.engineOptions.kibana.settings`. If using `server.basePath`, the reverse proxy must strip the prefix. `server.rewriteBasePath` is managed as `false`: omit it from new requests; previously stored values are ignored. Shared, cluster-lifetime session/saved-object/reporting encryption keys support multiple replicas. [Public routing](access-and-security.md) is a separate opt-in.

A route can exist before application initialization finishes. Wait for [component readiness](operations.md); a suspended component must be resumed rather than treated as starting.

## Logstash pipelines and queues

Enable `spec.components.extra.logstash.replicas` and size RAM/runtime disk for throughput and buffering. With no user pipelines, Karakoram supplies dual-stack Beats and HTTP inputs and a CA-verified Elasticsearch data-stream output. Inputs bind `::`; input exposure and ports are in [Access and security](access-and-security.md). Custom listeners do not automatically provision additional routing or secure inputs.

`engineOptions.logstash.pipelines` contains `{id, config}` entries, not raw `pipelines.yml` documents.

| Limit | Value |
|---|---|
| Pipelines | At most 10 |
| ID | Unique; `^[a-z][a-z0-9_-]{0,31}$` (1–32 characters) |
| Config | Non-whitespace content; at most 64 KiB per pipeline |

Pipeline substitutions are supplied through the process environment:

| Variable | Value |
|---|---|
| `${KARAKORAM_ES_HOSTS}` | Space-separated HTTPS data/combined-node URLs for URI-list settings |
| `${KARAKORAM_ES_HOST}` | First Elasticsearch HTTPS URL |
| `${KARAKORAM_ES_USER}` | Scoped writer username |
| `${KARAKORAM_ES_PASSWORD}` | Writer password |
| `${KARAKORAM_ES_CA}` | Rendered Elasticsearch CA path |

For a custom JSON HTTP intake, save this merge patch as `logstash.json`:

```json
{
  "spec": {
    "components": {"extra": {"logstash": {"replicas": 1}}},
    "engineOptions": {
      "logstash": {
        "persistentQueue": true,
        "pipelines": [{
          "id": "jsonlogs",
          "config": "input { http { host => \"::\" port => 8080 codec => json } }\noutput { elasticsearch { hosts => [\"${KARAKORAM_ES_HOSTS}\"] user => \"${KARAKORAM_ES_USER}\" password => \"${KARAKORAM_ES_PASSWORD}\" ssl_enabled => true ssl_certificate_authorities => [\"${KARAKORAM_ES_CA}\"] ecs_compatibility => v8 data_stream => true data_stream_type => \"logs\" data_stream_dataset => \"application\" data_stream_namespace => \"default\" } }"
        }]
      }
    }
  }
}
```

```sh
karactl clusters patch search -f logstash.json --wait
```

The default writer covers `logstash-*`, `ecs-logstash-*`, `logs-*` and `metrics-*`. Other destinations need a separately authorized writer/role. Never embed `elastic` or real secrets in pipeline source, settings or committed files; see [Elastic output options](https://www.elastic.co/docs/reference/logstash/plugins/plugins-outputs-elasticsearch).

Confirmed `ConfigurationError`/`PluginLoadingError` failures appear in member status and `ComponentsReady=False`, with parser positions when available but without raw source/credentials. They fail the current generation's convergence operation on observation instead of its normal two-hour deadline; transient startup/monitoring failures retain that deadline. The warning is deduplicated per applied configuration/error. Failed convergence retains the cluster and desired state and does not block independent snapshots, restores or queued operations. Correct the pipeline and submit a new patch: changed configuration is republished even after failed initial deployment or rolling rejoin. Use protected component logs for full diagnostics. Readiness does not establish event delivery.

**Warning:** `persistentQueue: true` persists only to each container's runtime disk. It survives process restart, but not redeployment, version upgrade, container replacement or service deletion. Queues are neither replicated nor snapshot-protected. Budget disk for every pipeline and retain upstream replay when event loss is unacceptable; see [queue semantics](https://www.elastic.co/docs/reference/logstash/persistent-queues).

## APM intake

Enable `spec.components.extra.apm.replicas`. This is standalone APM Server, not Fleet orchestration. `engineOptions.apm.apiKeys` defaults to true; false disables intake API-key authentication while retaining secret-token authentication. Enable public intake only through the [explicit public-route policy](access-and-security.md).

Configure APM agents with the returned server URL and the revealed intake token. Intake authentication is separate from the APM-to-Elasticsearch writer. Send bearer tokens only over trusted HTTPS: private HTTP intake is unencrypted.

Elasticsearch's built-in `apm-data` plugin supplies APM templates, ILM policies and ingest pipelines on supported versions (8.15+). Karakoram does not run a separate asset setup job; asset presence is not proof of event delivery.

For manual intake, obtain `APM_URL` from an HTTPS endpoint and `APM_SECRET_TOKEN` from protected credentials without recording either secret in logs. This Bash recipe needs `jq`, `curl` and GNU `date`; it sends NDJSON metadata before a transaction with a current Unix-microsecond timestamp:

```bash
(
  set +x
  set -euo pipefail
  umask 077
  work=$(mktemp -d)
  trap 'rm -rf -- "$work"' EXIT
  printf '%s' "$APM_SECRET_TOKEN" \
    | jq -Rs '"header = " + ("Authorization: Bearer " + . | @json)' \
    > "$work/curl.conf"
  TIMESTAMP_US=$(date +%s%6N)
  printf '%s\n' \
    '{"metadata":{"service":{"name":"intake-example","agent":{"name":"manual","version":"1.0.0"}}}}' \
    "{\"transaction\":{\"id\":\"0123456789abcdef\",\"trace_id\":\"0123456789abcdef0123456789abcdef\",\"name\":\"GET /example\",\"type\":\"request\",\"timestamp\":$TIMESTAMP_US,\"duration\":1.2,\"span_count\":{\"started\":0},\"sampled\":true}}" \
    | curl --config "$work/curl.conf" --fail-with-body -i "$APM_URL/intake/v2/events" \
        -H 'Content-Type: application/x-ndjson' --data-binary @-
)
```

Expected success is `202 Accepted`; verify indexed data afterward ([intake API](https://www.elastic.co/docs/solutions/observability/apm/elastic-apm-events-intake-api)). For API-key intake, use an APM-authorized key with `Authorization: ApiKey` and its encoded value, keeping it in protected curl configuration as above. A general index key is not automatically valid for intake: use [APM authorization](https://www.elastic.co/docs/solutions/observability/apm/apm-server/apm-agent-authorization). Karakoram supports application descriptors such as `{"application":"apm","privileges":["event:write","config_agent:read"],"resources":["*"]}` through its [native access APIs](access-and-security.md).
