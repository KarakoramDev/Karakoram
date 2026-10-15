<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Elasticsearch and the Elastic Stack

Karakoram runs Elasticsearch on Zerops Ubuntu runtimes with one persistent Local Storage volume per node. It does **not** provision the Zerops managed `elasticsearch` service. Optional Kibana, Logstash and standalone APM Server belong to the same cluster, use the same exact version and receive backend credentials and Elasticsearch CA trust automatically.

Before creating a cluster, [install and authenticate](../install.md), select its [namespace](namespaces.md), and review [platform limitations](../limitations.md). A namespace selects an existing Zerops project; a management relay does not make private database endpoints reachable from another project's applications. Multiple nodes/volumes do not guarantee independent physical hosts.

**Licensing:** downloaded default distributions are Elastic-licensed, not Apache-licensed because Karakoram is Apache-2.0. The default workflows use Basic-tier features; document/field-level security needs a licence tier that includes those features and is not available on Basic. Operators reselling hosted Elasticsearch/Kibana must review ELv2's managed-service restriction and obtain appropriate rights from Elastic. See [licensing](#licensing) before offering clusters to third parties.

## Verification status

Live Zerops checks have exercised a three-node Elasticsearch 9.5.4 Stack with Kibana, Logstash and APM, verified HTTPS and access controls, monitoring, rolling restart, member replacement, same-cluster snapshot restore, a single-node snapshot clone, rolling patch upgrade to 9.5.5, 3→4→3 scaling with cleared exclusions, retained-volume/backup suspension and resume, and final cluster/backup-record deletion. A dedicated-master smoke also formed three master-only and two data nodes with exact proof data. Docker integration tests cover rendered TLS/file-realm/keystore configuration and real security/snapshot APIs. These are functional checks on disposable data, not production load, failure-domain isolation or durability guarantees.

## Topologies and presets

| Topology | Elasticsearch nodes | Counts |
|---|---|---|
| `cluster` | Combined master/data/ingest/remote-cluster-client/transform nodes | `components.engine.replicas`: 1–9 |
| `dedicatedMasters` | Dedicated master-only nodes plus data/ingest/remote-cluster-client/transform nodes | `components.extra.master.replicas`: 3 or 5; `components.engine.replicas`: 1–20 |

Topology type is immutable. These layouts do not enable the `ml` role or separate hot/warm/cold pools. Applications connect to data/combined nodes, not dedicated masters.

Choose one combined node for development or usually 3/5 for production voting. Even counts are allowed with warnings, not recommended as an availability improvement. Two combined nodes require both voters; loss of either stops writes. One data node has no redundant shard copy, even with three dedicated masters. Replicas, disk headroom and actual failure domains determine resilience, not the preset name.

| Preset | Topology | Initial components |
|---|---|---|
| `dev` | `cluster` | 1 combined node + Kibana |
| `prod` | `cluster` | 3 combined nodes + Kibana |
| `observability` | `cluster` | 3 combined nodes + Kibana + Logstash + APM |
| `large` | `dedicatedMasters` | 3 masters + 3 data nodes + Kibana |

Optional components use one service with multiple containers: Kibana 0–3, Logstash 0–5 and APM 0–3 replicas. Zero disables the component. They cannot be attached to a different cluster or versioned independently. Their runtime disks are ephemeral.

## Sizing and heap

Public API RAM/disk fields are GiB; CPU fields are cores. The starting provider defaults are modest, not production throughput guarantees:

Stateless extension components may report and request zero `minDisk`/`maxDisk`; provider validation still enforces the data-disk requirements of Elasticsearch nodes. Request objects reject undeclared fields, including nested role privileges and field-security objects. Role writes omit the response-only `reserved` flag.

| Component | Starting RAM | Validation floor |
|---|---|---|
| Combined/data Elasticsearch node | Fixed 2 GiB | 1.5 GiB |
| Dedicated master | Fixed 1.5 GiB | 1 GiB |
| Kibana | 1.5–2 GiB | 1 GiB |
| Logstash | Fixed 1.5 GiB | 1 GiB |
| APM Server | Fixed 0.5 GiB | 0.25 GiB |

Elasticsearch starts with shared CPU bounds 1–2 and persistent disk bounds 5–20 GiB. Stateless component disks start at 1–5 GiB. Size disks for shard replicas, recovery/rebalance and watermarks; expanding node count does not fix a cluster whose remaining disks cannot accept relocated shards. Local Storage can grow but not shrink.

Karakoram derives explicit equal `-Xms`/`-Xmx` from configured **minimum** RAM, not the current autoscaled allocation:

- Elasticsearch: 40% at up to 4 GiB, otherwise 50%; round down to a 64-MiB multiple; clamp to 512–31744 MiB. At 2 GiB this gives 768 MiB.
- Kibana: Node.js old-space cap is 60% of minimum RAM, at least 512 MiB.
- Logstash: heap is 50% of minimum RAM, at least 384 MiB.
- APM Server is a Go process and has no JVM heap setting.

Heap is not total process RAM. Leave room for filesystem cache, native/direct memory, threads and queues. Elastic recommends keeping Elasticsearch heap below the compressed-oops threshold: 26 GiB is conservative on most systems, and some permit about 30 GiB. Karakoram's higher cap is not proof compressed pointers remain enabled; check startup logs and size deliberately ([JVM guidance](https://www.elastic.co/docs/reference/elasticsearch/jvm-settings)). Elastic's Kibana/Logstash sizing guidance exceeds these small defaults for many real workloads ([Kibana sizing](https://www.elastic.co/docs/deploy-manage/production-guidance/kibana-traffic-scaling-considerations), [Logstash sizing](https://www.elastic.co/docs/reference/logstash/jvm-settings)).

A 2026-10-13 probe at fixed 2-GB RAM found automatic Elasticsearch 9.5.5 heap sizing was OOM-killed; explicit 768-MB heap passed production bootstrap checks. The measured runtime had `vm.max_map_count=262144`, below Elastic's recommended `1048576`, and could not change the shared-host setting. File limits were raised from soft 1024 to hard 65535; 8-MiB memlock precludes memory locking. Do not assume all deployments have those limits or suppress bootstrap failures. See [runtime rationale](../adr/0025-elasticsearch-engine-runtime-model.md).

The agent renders product configuration below `DATA_DIR/karakoram/config` and runtime data/logs below `DATA_DIR/karakoram`, not `DATA_DIR/config`. `ES_PATH_CONF`, `KBN_PATH_CONF` and Logstash `--path.settings` point to that rendered configuration directory. Deployment readiness explicitly invokes `karakoram agent readiness --engine=elasticsearch --data-dir=<dir> <component>` (`elasticsearch`, `kibana`, `logstash` or `apm-server`): Zerops readiness exec was measured to run without the service environment, so both engine selection and the rendered configuration directory are passed explicitly. Component readiness does not by itself verify cluster formation or end-to-end delivery.

Kibana deployment readiness requires local `/api/status` to return HTTP 200,
not merely an open port. Startup HTTP 503 responses keep the deployment pending
until Kibana's application initialization finishes; redirects are not accepted.
`server.rewriteBasePath` is managed as `false` so this local status endpoint
remains unprefixed. If you set `server.basePath`, your reverse proxy must strip
that prefix before forwarding requests to Kibana. Previously stored
`server.rewriteBasePath` values are ignored during reconciliation and member
configuration; new create/update requests must omit this managed setting.

## Create

In **Clusters → Create cluster**, select **Elasticsearch**, its advertised version, topology/preset and component counts/resources. Keep private exposure and `requireTLS`. Cluster names must match `^[a-z][a-z0-9]{1,11}$`: 2–12 lowercase letters/digits, beginning with a letter. Select the namespace in the UI, or use `-n NAME` with the CLI.

Use the current catalog rather than an arbitrary archive version:

```sh
karactl providers get elasticsearch -o json
```

The initial catalog offers `9.5.5` (default), `9.5.4`, `9.4.8` and `8.19.23`. Enabled Stack products always follow the selected exact version.

There is no `--preset` CLI flag. Copy a catalog `presets[].spec` into a create
body's `spec`, keep `provider: elasticsearch`, then customize the components and
`engineOptions`. The [CLI file example](cli.md#elasticsearch-create-from-a-file)
includes a native `snapshot` schedule and its required namespace-local storage.

For CLI or API creation, save this API-shaped request as `elasticsearch.json`. It creates three combined nodes and private Kibana with nightly snapshots; Logstash/APM are disabled. [Register `archive`](backups-and-restore.md#register-s3-storage) in the selected namespace first, or remove the `backup` block if no backups are wanted:

```json
{
  "name": "search",
  "spec": {
    "provider": "elasticsearch",
    "version": "9.5.5",
    "topology": {"type": "cluster"},
    "components": {
      "engine": {
        "replicas": 3,
        "resources": {
          "cpuMode": "SHARED", "minCpu": 1, "maxCpu": 2,
          "minRam": 2, "maxRam": 2, "minDisk": 5, "maxDisk": 20
        }
      },
      "extra": {
        "kibana": {
          "replicas": 1,
          "resources": {
            "cpuMode": "SHARED", "minCpu": 1, "maxCpu": 2,
            "minRam": 1.5, "maxRam": 2, "minDisk": 1, "maxDisk": 5
          }
        },
        "logstash": {"replicas": 0},
        "apm": {"replicas": 0}
      }
    },
    "tls": {"mode": "requireTLS", "ca": "selfSigned"},
    "backup": {
      "storage": "archive",
      "schedules": [{
        "name": "nightly", "cron": "0 2 * * *", "type": "snapshot",
        "retainCount": 7, "enabled": true
      }]
    },
    "engineOptions": {
      "kibana": {"public": false, "settings": {}},
      "apm": {"public": false, "apiKeys": true},
      "logstash": {"pipelines": [], "persistentQueue": false},
      "elasticsearch": {"settings": {}}
    }
  }
}
```

```sh
karactl -n default clusters create search -f elasticsearch.json --wait
karactl -n default clusters status search
karactl -n default clusters members search
```

The same body goes to `POST /api/v1/clusters?namespace=default`. With a Karakoram bearer token supplied securely as `KARAKORAM_TOKEN`:

```sh
curl --fail-with-body "$KARAKORAM_SERVER/api/v1/clusters?namespace=default" \
  -H "Authorization: Bearer $KARAKORAM_TOKEN" \
  -H 'Content-Type: application/json' --data-binary @elasticsearch.json
```

`KARAKORAM_SERVER` here is the HTTPS installation origin. Request acceptance is not deployment completion; inspect status/operations and `observedGeneration`. Creation imports node runtimes/volumes, bootstraps once, observes cluster identity and membership, establishes security, deploys enabled components and verifies repositories. A listening HTTP port alone does not prove the cluster is ready.

For dedicated masters, create with `spec.topology.type: dedicatedMasters` and add `spec.components.extra.master`, for example 3 replicas with fixed 1.5-GiB RAM and its own CPU/disk resource bounds. `components.engine` then describes data-only nodes. Do not include the `master` component in a combined-node topology.

Custom Elasticsearch settings belong under `spec.engineOptions.elasticsearch.settings`, not `engineConfig`. Kibana settings belong under `engineOptions.kibana.settings`. Karakoram reserves node roles, discovery, network/path/ports, security/TLS, S3 clients and managed Kibana connection/encryption/public-URL settings; user overrides are rejected. Logstash pipelines are `{id, config}` entries, not raw `pipelines.yml` documents. API/CLI patch files contain `{spec: {...}}`; arrays replace the complete previous array ([API conventions](api.md#declarative-updates-and-asynchronous-work)).

## Connect and reveal credentials

Get endpoints from cluster connection details, not from guessed platform IPs:

```sh
karactl clusters connection search --reveal
```

`GET /api/v1/clusters/{cluster}/connection?namespace=NAME` requires separate `cluster-credentials:read` permission and returns sensitive credentials with `Cache-Control: no-store`. The credential entries are `elastic` (human administrative password) and `apm-secret-token` (APM intake token, useful when APM is enabled); `caCertificate` is the cluster's public CA PEM. Credential entries also carry the cluster connection URI; obtain component URLs from endpoints instead. Ordinary status contains password-free endpoints; do not record credential reveal in shared logs.

| Endpoint name | Use |
|---|---|
| `elasticsearch` | Private HTTPS on data/combined nodes, port 9200; optional public TCP HTTPS entry |
| `kibana` | Private HTTP 5601; optional public HTTPS subdomain entry |
| `apm` | Private HTTP 8200; optional public HTTPS subdomain intake entry |
| `logstash-beats` | Private Beats input, port 5044 |
| `logstash-http` | Private HTTP input, port 8080 |

Public endpoints retain these names and carry `public: true`; there are no `kibana-public` or `apm-public` names. Select the public entry explicitly when accessing a subdomain or public Elasticsearch route.

Run **private-endpoint commands from an application or diagnostic workload in
the same Zerops project as the selected namespace**. A local workstation or
management relay does not resolve those private hostnames or provide an
application-network tunnel. Install/authenticate `karactl`, `jq` and `curl` on
that workload first.

The following Bash function fetches the structured connection response and
extracts an Elasticsearch URL, the `elastic` password and the multiline CA PEM
into protected temporary files. `false` selects a private URL; `true` selects a
public URL. It fails if the selected endpoint or credentials are absent:

```bash
es_health() (
  set +x                         # Never trace secrets, even in a parent shell.
  set -euo pipefail
  umask 077
  work=$(mktemp -d)
  trap 'rm -rf -- "$work"' EXIT

  karactl -n default clusters connection search --reveal -o json \
    | jq -e --argjson public "$1" '
        {
          url: ([.connection.endpoints[]
                 | select(.name == "elasticsearch" and .public == $public)
                 | .url][0]),
          password: ([.credentials[]
                      | select(.username == "elastic") | .password][0]),
          caCertificate: .caCertificate
        }
        | if (.url // "") != "" and (.password // "") != ""
             and (.caCertificate // "") != ""
          then . else error("Endpoint, password or CA unavailable") end
      ' > "$work/connection.json"

  ES_URL=$(jq -er '.url' "$work/connection.json")
  jq -er '.caCertificate' "$work/connection.json" > "$work/cluster-ca.pem"
  jq -r '"user = " + ("elastic:" + .password | @json)' \
    "$work/connection.json" > "$work/curl.conf"
  curl --config "$work/curl.conf" --fail-with-body \
    --cacert "$work/cluster-ca.pem" "$ES_URL/_cluster/health?pretty"
)

# Private variant: execute on a workload in the namespace's Zerops project.
es_health false
```

For the **public variant**, run the same function from an allowed client after
configuring [public TCP exposure](../adr/0017-public-tcp-exposure.md), then select
the explicitly public Elasticsearch endpoint:

```bash
es_health true
```

Keep shell tracing disabled, and do not run reveal commands in CI/shared
terminal recordings or log their JSON. The function does not put the password
in curl's process arguments or export it to child environments; its files are
owner-only and removed on exit. Protect the host and temporary storage too.
`jq -r` decodes the CA's JSON escapes into real PEM newlines; do not flatten it.
As an alternative CA export, inside the protected function use
`karactl -n default clusters ca search > "$work/cluster-ca.pem"` in place of the
CA extraction line; see [CLI connection and CA export](cli.md#requests-output-and-waits).

Never use `-k` or disable certificate verification. Password clients do not need the transport certificate or a client private key. Use a narrowly scoped application user/API key for workloads; `elastic` is an administrative superuser, not a default ingestion identity. Give clients multiple returned data-node endpoints for failover. Public clients should use provisioned public addresses, and should not enable sniffing that discovers unreachable private hosts.

`spec.exposure` uses the existing [public TCP policy](../adr/0017-public-tcp-exposure.md): explicit public enablement, IPv6 or dedicated IPv4 and a CIDR allowlist. It routes to data/combined node HTTP 9200 with Elasticsearch TLS end to end. Dedicated masters, transport 9300 and management ports remain private. Routing status is not proof of external reachability; check allowed/denied clients and CA validation.

## Kibana

Open the returned Kibana endpoint and initially log in as **`elastic`** with the revealed password, or use a regular native user with appropriate roles. Karakoram's own UI login is not a Kibana identity. The generated Kibana service-account token authenticates the server to Elasticsearch; it is not a browser login and need not be copied manually.

Set `spec.engineOptions.kibana.public: true` for the Zerops subdomain HTTPS route. Karakoram supplies the public base URL, secure cookies and stable encryption keys shared across replicas. The balancer terminates TLS and forwards HTTP on the private project network. Zerops documents a **50-MB upload cap** and says the shared subdomain balancer is **not recommended for production**; custom-domain management is not supplied by this feature ([Zerops public access](https://docs.zerops.io/references/networking/public-access)). The same limits apply to optional public APM intake.

**Public Kibana and APM routes are reachable from the internet, without source-IP restrictions.** The Elasticsearch `spec.exposure.allowlist` applies only to direct database access on port 9200, not these HTTPS subdomains. Kibana is protected by Elasticsearch/Kibana authentication; APM intake is protected by its secret token or API keys. Leave these routes private unless that exposure is intentional.

Creating a public route, or changing a component from private to public, requires a request-only acknowledgement alongside its `public` flag: `engineOptions.kibana.allowAllAddresses: true` or `engineOptions.apm.allowAllAddresses: true`. For example, a Kibana merge patch is `{"spec":{"engineOptions":{"kibana":{"public":true,"allowAllAddresses":true}}}}`. The acknowledgement is consumed, not stored or inherited by later PATCH requests. An unchanged public route does not need another acknowledgement; disabling and re-enabling it requires fresh confirmation. The wizard and topology editor ask for confirmation and Review lists every enabled public route, even when database access stays private.

These routes use Zerops's **shared subdomain access**, not custom-domain HTTP locations. The [public REST schema](https://api.app-prg1.zerops.io/api/rest/public/swagger/) exposes enable/disable-subdomain-access actions without a routing-policy body. Source-IP access policies are available on [custom-domain HTTP locations](https://docs.zerops.io/references/networking/l7-balancer-config#access-policy-configuration), a different route type; they cannot be attached to the generated subdomain routes.

A provisioned Kibana route can exist while the application is still starting.
Wait for component readiness and retry after initialization; a route alone is
not proof Kibana is usable. Suspended clusters show Kibana as suspended, not
starting; resume the cluster before waiting for readiness.

## Logstash pipelines and queues

Enable `spec.components.extra.logstash.replicas` and choose RAM/disk for throughput and buffering. With no user pipelines, Karakoram supplies dual-stack Beats 5044 and HTTP 8080 inputs (`host => "::"`) and a CA-verified Elasticsearch data-stream output. Inputs are private; enabling Kibana/APM public access does not expose Logstash or secure its inputs automatically.

Pipeline environment variables are injected into the Logstash process:

| Variable | Value |
|---|---|
| `${KARAKORAM_ES_HOSTS}` | Space-separated HTTPS data/combined-node URLs; usable in URI-list output settings |
| `${KARAKORAM_ES_HOST}` | First Elasticsearch HTTPS URL |
| `${KARAKORAM_ES_USER}` | Scoped Logstash writer username |
| `${KARAKORAM_ES_PASSWORD}` | Writer password |
| `${KARAKORAM_ES_CA}` | Path to the rendered Elasticsearch CA file |

Example merge patch (`logstash.json`) with a custom JSON HTTP intake pipeline:

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

The default writer covers `logstash-*`, `ecs-logstash-*`, `logs-*` and `metrics-*`. A pipeline targeting other names needs a separately authorized writer/role; do not put the `elastic` password in pipeline source. Keep real secrets out of settings and committed files. The existing declared inputs are 5044/8080; a pipeline listener on another port does not by itself provision routing for that port. See [Elastic's output options](https://www.elastic.co/docs/reference/logstash/plugins/plugins-outputs-elasticsearch).

Logstash readiness requires **every configured pipeline** to be loaded and
running, not just a live JVM or an open monitoring port. Pipeline configuration
errors from node reload statistics or current-configuration startup logs appear
in member status and the `ComponentsReady=False` condition, including the error
class and parser line/column when available. Raw source and credentials are not
copied into status. A confirmed `ConfigurationError` or `PluginLoadingError`
fails the create/update convergence operation on the next observation rather
than waiting for the ordinary two-hour operation deadline; transient startup or
unavailable monitoring APIs retain that normal deadline. Failed operations do
not delete the cluster or discard desired state. Correct the pipeline and apply
the configuration again; the new generation can converge normally. Inspect the
protected component logs for full diagnostics. Readiness still does not prove
events reached their destination.

`persistentQueue: true` writes queues to **container disk**, not a Local Storage volume. Events survive a Logstash process restart, but queues are lost on redeployment, version upgrade, container replacement or service deletion. Each replica has its own queue, without replication or snapshot protection. Size runtime disk for every pipeline's queue and retain upstream replay where loss is unacceptable ([queue semantics](https://www.elastic.co/docs/reference/logstash/persistent-queues)).

## APM intake

Enable `spec.components.extra.apm.replicas`; set `engineOptions.apm.public: true` only when applications need public HTTPS intake. `engineOptions.apm.apiKeys` defaults to true; set false to disable intake API-key authentication while retaining secret-token authentication. Standalone APM Server is used, not Fleet Server orchestration, and listens dual-stack on `[::]:8200`.

Configure an Elastic APM agent with the returned APM server URL and the revealed secret token. Intake authentication is separate from the scoped APM-to-Elasticsearch writer password. Send the token only over a trusted HTTPS route: private HTTP intake is not encrypted and does not make a bearer token safe across untrusted networks.

Elasticsearch's built-in `apm-data` plugin installs APM index templates, ILM policies and ingest pipelines on Elasticsearch 8.15+, including the supported 8.19/9.x releases. These assets were observed live on 9.5.5; Karakoram does not run a separate APM template/ILM/pipeline setup job. Asset presence is not proof that APM events reach storage.

A manual intake example uses `APM_URL` from the HTTPS endpoint and `APM_SECRET_TOKEN` from protected credentials. It sends NDJSON metadata before a transaction and uses a current Unix-microsecond timestamp:

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

The expected successful intake response is `202 Accepted`; inspect indexed data to establish delivery, not just listener health ([intake API](https://www.elastic.co/docs/solutions/observability/apm/elastic-apm-events-intake-api)). Use an APM-agent-authorized API key instead with `Authorization: ApiKey $APM_API_KEY` when configured. A general Elasticsearch index API key is not automatically an APM intake key: use Elastic's APM API-key workflow and ingestion application privileges. Karakoram's role and API-key descriptor APIs accept `applications` entries with `application`, `privileges` and `resources` ([APM authorization](https://www.elastic.co/docs/solutions/observability/apm/apm-server/apm-agent-authorization)).

## Users, roles and API keys

Karakoram accounts/RBAC authorize the control plane; native Elasticsearch identities authorize the database and Kibana. These APIs require `database-users` permissions on the selected cluster, with namespace selection as on other cluster routes.

| Resource | Karakoram routes |
|---|---|
| Users | `GET/POST /api/v1/clusters/{cluster}/elasticsearch/users`; `GET/PUT/DELETE .../users/{username}` |
| Roles | `GET/POST /api/v1/clusters/{cluster}/elasticsearch/roles`; `GET/PUT/DELETE .../roles/{role}` |
| API keys | `GET/POST /api/v1/clusters/{cluster}/elasticsearch/api-keys`; `DELETE .../api-keys/{id}` invalidates |

Create a custom role before the user referencing it, or select an existing built-in role. Every assigned role must already exist in Elasticsearch; unknown role names are rejected before the user is written. Example `role.json`:

```json
{
  "name": "orders-reader",
  "cluster": [],
  "indices": [{
    "names": ["orders-*"],
    "privileges": ["read", "view_index_metadata"],
    "allowRestrictedIndices": false
  }],
  "applications": []
}
```

A user create body is `{username, password, roles, fullName, email, enabled}`; supply a password of at least eight characters securely on create. Passwords are write-only. Reads cannot recover application passwords. User roles are string names, not MongoDB role/database tuples. Role bodies include `cluster`, `indices` and `applications` privilege arrays; use an empty array where no grant is needed.

Invalid native privilege errors identify the cluster, index or application grant field separately; index privilege diagnostics can list cross-cluster privileges without implying a cluster-grant error.

Index privileges can also include a document-level `query` (an opaque Elasticsearch query JSON string) and `fieldSecurity` with optional `grant` and `except` field-pattern arrays. **Both require a licence tier that includes document/field-level security; Basic does not.** Requests that require unavailable licensed features return an actionable 4xx response explaining the licensing prerequisite, rather than creating an unrestricted role. Reads and role edits preserve these restrictions, including explicitly empty grant arrays. The UI accepts a JSON array for index privileges when these restrictions are needed, in addition to the simple one-entry-per-line format. API-key descriptors support the same fields.

API-key licence errors identify the restricted grant under `roleDescriptors.<name>.indices[i].query` or `.fieldSecurity`, rather than a top-level query field.

Kibana browser access needs application privileges as well as privileges on the
underlying data indices. For example, this complete role body grants read-only
Kibana access across all spaces plus read access to `orders-*`:

```json
{
  "name": "orders-kibana-reader",
  "cluster": [],
  "indices": [{
    "names": ["orders-*"],
    "privileges": ["read", "view_index_metadata"]
  }],
  "applications": [{
    "application": "kibana-.kibana",
    "privileges": ["read"],
    "resources": ["*"]
  }]
}
```

For the default Kibana application in the supported Elasticsearch/Kibana 8/9
Stack, `kibana-.kibana` is the application name; `read` is a Kibana base
privilege, not an Elasticsearch index privilege in this array. Kibana registers
its application privileges at startup. `resources: ["*"]` covers every space;
use `["space:default"]` to restrict the grant to the Default space, or explicitly
select other spaces. Use the **Elastic Access** application-grant editor or a
role file to author/edit these fields; the grant does not authorize reading
data without the separate `indices` privileges. Elastic recommends Kibana's
own role UI/API for general Kibana role management; see
[Kibana application privilege mapping](https://www.elastic.co/docs/extend/kibana/key-concepts/security/rbac-and-saved-objects)
and [Kibana privileges](https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/kibana-privileges).

Elasticsearch accepts arbitrary application names and privilege strings, even if no application has registered them. Acceptance does not prove Kibana authorization. The role editor gives a non-blocking warning for applications other than `kibana-.kibana` or privileges outside the known `all`, `read`, `feature_<name>.all` and `feature_<name>.read` forms; custom application grants remain valid native inputs. This is a form check, not a live inventory of privileges registered by Kibana.

The Elasticsearch CLI uses these commands and flags:

```sh
karactl elasticsearch roles create search orders-reader -f role.json
karactl elasticsearch roles list search
karactl elasticsearch roles get search orders-reader
printf '%s' "$APP_PASSWORD" | karactl elasticsearch users create search ordersreader \
  --roles orders-reader --password-stdin
karactl elasticsearch users list search
karactl elasticsearch users get search ordersreader
karactl elasticsearch roles update search orders-reader -f role.json
karactl elasticsearch users update search ordersreader --roles orders-reader
karactl elasticsearch users delete search ordersreader --yes
karactl elasticsearch roles delete search orders-reader --yes
```

Native user/role deletion and API-key invalidation require confirmation. In a
script or other non-TTY session, pass `--yes` after reviewing the target or pipe
`y`; empty stdin never submits the destructive request.

Supply `APP_PASSWORD` securely and keep shell tracing disabled; use `--password-stdin` for password changes rather than an argument containing the secret. For API creation, submit protected JSON with the user body above. `elastic`, `kibana_system`, other built-in users, reserved roles and `karakoram*` identities are protected: ordinary native-access CRUD cannot modify them. The provider owns control, monitor, Kibana and writer credentials; use the supported managed rotation operation instead. Existing built-in roles may still be assigned to regular users.

API-key create bodies use `name`, optional `expiration` and a required, nonempty `roleDescriptors` map. Missing or empty descriptors are rejected: keys must never inherit the control-plane superuser identity's permissions. Example for a read-only workload:

```json
{
  "name": "orders-query",
  "expiration": "30d",
  "roleDescriptors": {
    "orders-read": {
      "cluster": [],
      "indices": [{"names": ["orders-*"], "privileges": ["read"]}],
      "applications": []
    }
  }
}
```

For CLI creation, `descriptors.json` contains only the `roleDescriptors` map from the body above (the `orders-read` object and its privileges), not the surrounding `name`/`expiration` request. Supply those fields through flags:

```sh
karactl elasticsearch api-keys create search --name orders-query --expiration 30d \
  -f descriptors.json
karactl elasticsearch api-keys list search
karactl elasticsearch api-keys invalidate search KEY_ID --yes
```

Creation returns `id`, `apiKey` and `encoded`; store them immediately and securely. Lists do not reveal key secrets. API-key creation and native user/role POST/PUT operations accept `Idempotency-Key`: retry an unchanged request with the same key to replay the original successful response, including the one-time API-key secret, for 24 hours without creating another object. The UI sends a submission key and changes it when the request is edited. Without that header, an uncertain key-creation outcome requires listing and invalidating any unwanted key before creating another. See [safe API retries](api.md#safe-retries-with-idempotency-key). Prefer explicit expiry and narrow role descriptors. Use `Authorization: ApiKey $ES_API_KEY` with the encoded key for Elasticsearch HTTP. Expiration, invalidation and role descriptors follow [Elastic API-key semantics](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-create-api-key); keys are not Karakoram management bearer tokens.

API-key invalidation is safe to retry, including when a reload or interrupted
client leaves its outcome uncertain; invalidating an already invalidated key
succeeds. Refresh the key inventory to confirm the final state. The UI retains
its pending marker across reload in the same browser tab and offers a retry if
the key remains active after the request window. This is not a durable
control-plane operation or a guarantee across server restarts. A not-found (404)
invalidation response clears the pending marker and remains an error; an absent
key after that rejection is not reported as a successful invalidation.

Managed rotation uses the existing system-user operation with identity `elastic`, `karakoram-monitor`, `kibana-token`, `logstash`, `apm` or `apm-secret-token`; these are exactly the identities the system-user list shows. Affected component configuration is republished/restarted. TLS leaf rotation uses `karactl clusters rotate-tls search --wait --yes` and Elasticsearch's watched PEM reload under the same CA. Neither operation rotates the CA or Kibana's saved-object encryption keys.

## Snapshots and restore

Elasticsearch supports backup type **`snapshot`**, not PBM logical/physical backups or PITR. Register an existing S3 BackupStorage in **Settings → Backup storages** with endpoint, signing region, bucket, prefix and protected keys; see [storage registration](backups-and-restore.md#register-s3-storage). Snapshot traffic goes directly from nodes to storage.

Karakoram creates a named S3 client and verified repository for each referenced storage, with a cluster-UID-unique base path and one writer. Adding/removing a client/storage or changing static endpoint settings requires rolling Elasticsearch maintenance; changing keystore credentials is reloadable without restart. Removing a storage from the spec retires its repository registrations and client only after no backup inventory, queued/running restore, or unfinished initial-restore source references it. Repository unregistration does not delete snapshots. An already deleted, unreferenced storage does not block configuration pulls or this retirement rollout. Do not register another writable cluster over the same repository prefix or delete repository files directly.

On 2026-10-13, Elasticsearch 9.5.5 `_verify` and `_analyze` passed against Zerops Object Storage with `blob_count=100`, `max_blob_size=1mb` and `register_operation_count=20`. This is limited compatibility evidence, not a production durability guarantee. Elasticsearch 9.3+ needs S3 conditional writes; incompatible storage must be fixed, not enabled with an unsafe workaround. See [security/snapshot rationale](../adr/0028-elasticsearch-security-and-snapshots.md).

```sh
karactl backups create search beforechange --type snapshot --storage archive
karactl backups get search beforechange
karactl backups list search
karactl operations cluster search
```

Wait for the Backup and its Operation to succeed; partial/failed native snapshots are not recovery bases. Snapshots include indices/data streams, global state and feature states (including security/Kibana). They do not include node files, private keys, keystores or repository registrations. Preserve encrypted control-plane state, its original encryption key and storage configuration alongside snapshots.

`Backup.location` records `s3://<bucket>/<base_path>?repository=<repo>#<snapshot>`. This identifies a repository and native snapshot, **not** a per-snapshot object prefix: snapshots share blobs. For a retained backup whose source cluster has been deleted, direct deletion is possible only when it is the sole snapshot in that retired repository path and no cluster writes there. If other snapshots remain, register the repository in another Elasticsearch cluster and delete through Elasticsearch's snapshot API.

Use the existing cluster scheduler, not Elastic SLM, for scheduled backups/count retention. For example, patch this fragment under `spec`:

```yaml
backup:
  storage: archive
  schedules:
    - name: nightly
      cron: "0 2 * * *"
      type: snapshot
      storage: archive
      retainCount: 7
      enabled: true
```

Schedules use five-field UTC cron; retainCount counts successful snapshots for that schedule. Arrays replace previous schedules. Control-plane downtime pauses scheduling/retention without a promised missed-run catch-up. ILM manages live index retention and is not a backup.

### In-place restore

**Quiesce external writers and plan downtime. Restore overwrites the destination.** Take a recovery snapshot of current state if needed.

```sh
karactl restores create search recovery --backup beforechange --yes
karactl restores get search recovery
karactl operations cluster search
```

Karakoram stops all Kibana/Logstash/APM services **at the platform level**, deletes destination data streams (including hidden streams) and every non-system index (including hidden and closed indices), restores global state, aliases and feature states, waits for recovery, re-ensures managed credentials through the unaffected file realm, then restarts the services with current configuration. System indices are handled by feature-state restoration rather than blanket deletion. Restored application users/roles/API keys and cluster settings may replace current values. Recheck application authentication/data and component delivery before reopening writers. Do not submit a duplicate restore because a client wait expired.

### New cluster from backup

At creation, `spec.dataSource` requires the source cluster to **still exist** and use Elasticsearch, and the named snapshot to have succeeded. Create a complete destination request with provider `elasticsearch`, a compatible version and this immutable fragment:

```yaml
dataSource:
  cluster: search
  backup: beforechange
```

```sh
karactl clusters create searchcopy -f clone.json --wait
karactl clusters status searchcopy
karactl operations cluster searchcopy
```

The destination registers the source repository **read-only**, restores after formation, establishes its own managed credentials and uses its own unique writable prefix afterward. Target version must not be older than the snapshot and may be at most one major ahead; snapshot/index compatibility still applies ([Elastic compatibility](https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore)). Restore is not a downgrade path or continuous replication link. Select source namespace in `dataSource.namespace` for cross-namespace recovery and ensure required source export/target permissions and direct S3 reachability.

Keep the source snapshot and its storage available until the initial restore finishes. After a successful initial restore, unrelated destination updates do not re-resolve the source cluster or snapshot: deleting those source records does not prevent scaling or monitoring changes. The `dataSource` reference remains immutable historical provenance; it cannot be changed by an update.

To recover a retained backup of a **deleted** source cluster, create a destination without `spec.dataSource`, wait for formation, then submit an explicit restore operation using the retained backup's original source cluster/namespace. For example, with a retained backup `beforechange` from deleted cluster `search`:

```sh
karactl clusters create searchcopy -f destination.json --wait
karactl restores create searchcopy recovery --source-cluster search --backup beforechange --yes
karactl restores get searchcopy recovery
```

`destination.json` contains the destination spec without a data source. Add `--source-namespace NAME` when the retained backup belongs to another namespace; preserve its backup record, storage configuration and protected control-plane state.

## Scale, upgrade, restart and replace

Change node/component counts or resource bounds through the UI or a spec merge patch. Example scale-up file:

The UI's Apply review derives consequences from the exact outgoing merge patch,
not normalized form defaults. A replicas-only edit therefore reviews scaling
without claiming unrelated TLS, exposure or monitoring changes.

```yaml
spec:
  components:
    engine:
      replicas: 5
```

```sh
karactl clusters patch search -f scale.yaml --wait
karactl clusters members search
```

Added nodes receive new, never-reused indices and join through hot-reloaded discovery seeds. Scale-down drains nodes serially via allocation exclusions and voting exclusions where needed. Removing members remain visible as **draining**, not healthy, until Elasticsearch confirms their absence; Overview distinguishes desired, observed and draining counts. Remaining data nodes must support the maximum index replica count plus a primary. A blocked drain shows a `ShardDrainBlocked` condition/callout and emits a Warning event on transition into the blocked reason, not on every reconciliation. **Restore or raise `components.engine.replicas` to cover all currently observed data nodes to cancel a scale-down before its shard drain completes.** This clears the managed allocation exclusion and blocked condition and emits a Normal `ShardDrainCancelled` event; raising the count further then provisions extra capacity. A node already voting-excluded or being deleted must finish removal, and replacement removals are not cancelled this way. Alternatively, deliberately lower the affected index replica policy only if the resulting loss of redundancy is acceptable, then let reconciliation retry. Restore unavailable primaries before retrying; never force removal.

Green health does not prove that a node can drain: an allocated primary can remain pinned to the removing node by an index allocation filter while every shard is healthy. With green or yellow health, a drain is reported blocked after the removing node retains the same nonzero shard count through three consecutive observations without relocation or initialization. Shard-count progress or resumed relocation resets that window. Repeated idle/relocating flaps do not emit the same Warning again for that drain. Karakoram uses a remaining shard's allocation explanation when available to identify the affected index, destination eligibility and allocation-filter rejection; unavailable diagnostics do not suppress the blocked condition.

For a pinned index, remove or relax the restrictive `index.routing.allocation.require.*`, `include.*` or `exclude.*` filter through Elasticsearch's index settings API so the shard can move. Alternatively restore `components.engine.replicas` to cover all observed data nodes to cancel the unfinished scale-down. Extra capacity or fewer index replicas alone cannot release a primary pinned to a specific excluded node. The `ShardDrainBlocked` Warning and Overview callout remain actionable even while Elasticsearch reports green.

Dedicated masters do not hold shards and are absent from Elasticsearch's allocation inventory. Their scale-down proceeds to voting exclusion without waiting for a data-node allocation row; primary-shard safety checks still apply before removal.

Managed allocation-exclusion ownership is checkpointed before sending the exclusion to Elasticsearch. Cancellation therefore removes its own exclusion even if the native write was interrupted before the drain stage was recorded, while preserving exclusions supplied by the operator.

Removal never advances to voting exclusion or runtime/volume deletion while any primary shard is unassigned (red health), including for a node that disappeared mid-drain and for retried platform deletions. Auto-expanded replica settings are evaluated for the topology after removal. Restarts, replacements, backups, restores and credential/TLS rotations submitted before the cluster first forms fail immediately instead of waiting behind provisioning.

RAM/heap or restart-relevant Elasticsearch settings use rolling maintenance. CPU/disk-only resource changes use nondisruptive resource updates, subject to platform constraints. Optional component replica changes adjust container count; zero deletes that component service and its ephemeral disk/queue contents.

Supported upgrade paths in the advertised catalog are:

- Same major: any newer advertised version, never a downgrade.
- `8.19.x` → `9.1+`: currently the offered `9.4.8`, `9.5.4` or `9.5.5`, with a backup requirement. `9.0` is not offered as an 8.19 target.

Take a successful fresh snapshot and resolve Upgrade Assistant/deprecation/index-compatibility issues before a major change. Apply a version-only spec patch with `karactl clusters patch search -f upgrade.json --wait`. Status and the upgrade Operation report the current component/member/step. Maintenance orders Elasticsearch nodes first (elected master last among Elasticsearch nodes), then Kibana, Logstash and APM. Kibana upgrades with all its instances stopped and has a separate interruption even when Elasticsearch remains available; mixed Kibana versions are not supported. An in-place downgrade is not rollback; use a compatible pre-upgrade snapshot in a newly rebuilt earlier-version cluster ([Elastic upgrade guidance](https://www.elastic.co/docs/deploy-manage/upgrade)).

```sh
karactl clusters restart search --wait
karactl clusters replace-member search MEMBER_NAME --wait --yes
karactl operations cluster search
```

Restart and replace-member operations are rejected until the cluster has formed and is active; they are not bootstrap recovery commands. Select `MEMBER_NAME` from observed members. Rolling disruptions require all planned master-eligible nodes present and green health, except when only unplaceable replicas explain yellow and no shards are initializing/relocating. Red or unexplained yellow blocks progress. Rolling maintenance temporarily restricts allocation to primaries, flushes, changes one node, waits for rejoin, restores allocation and waits for stable health. Data-only nodes precede non-elected masters; the elected master is last.

If a platform deployment fails after taking the current node offline, its retry
may recover that same absent target. It still requires every other planned
master and the committed voting quorum; it never advances to a different node
while the failed target is absent.

Replacement adds/joins a new node before draining/deleting the old one and needs temporary capacity. Single-node replacement is rejected; recover through snapshot restore. One-node restarts/upgrades have downtime, and two combined nodes cannot maintain quorum with one stopped. Persistent volumes are separate from runtime disks; deleting a cluster with `keepVolumes` and/or `keepBackups` is an explicit retention choice, not a backup guarantee.

With only one data node, restarts/upgrades of that node interrupt Elasticsearch
service even if dedicated masters preserve quorum; there is no replica copy on
another data node to serve requests. Kibana restarts separately from the
Elasticsearch nodes, so plan its own browser-access interruption too.

Deleting with `keepVolumes: true` during a snapshot restore abandons that restore rather than resuming its destructive steps. On resume, Karakoram records a `RestoreAbandoned` warning, clears the restore-owned destructive setting after Elasticsearch rejoins, restores component process intent and skips an interrupted initial-restore checkpoint. **Retained data may be partially restored**; inspect it before allowing workloads and request a fresh explicit snapshot restore if needed. Automatic initial restore is not retried.

Suspended member/agent observations are historical: the API marks members
`suspended`, reports agents offline and does not present cached live health.
Retained allocations or past green samples do not mean services are running.

With `keepBackups: false`, cleanup first deletes native snapshots in **owned writable** repositories while Elasticsearch remains reachable, before removing infrastructure. Corresponding Backup inventory records are removed only after native snapshot absence is confirmed. It never deletes snapshots from read-only source repositories. If Elasticsearch is unreachable, cleanup records a `SnapshotsRetained` warning, continues infrastructure deletion and leaves snapshots and their inventory records retained; this choice is not a promise that object storage was emptied. `keepBackups: true` preserves backups and their records explicitly.

## Monitoring

Inspect cluster phase/conditions, observed version/membership, elected master, component readiness, heap/disk and shard allocation. Yellow means all primaries assigned but replicas missing; it can be expected on one data node, but is not generally healthy redundancy. Red means at least one primary is unavailable. An open port or an `up` sample is not proof of data delivery or durable recovery. See [monitoring](monitoring.md) for collection/access conventions.

Elasticsearch metrics use the `karakoram_elasticsearch_` prefix:

| Suffix | Meaning |
|---|---|
| `up` | Local Elasticsearch availability |
| `cluster_status` | 0 green, 1 yellow, 2 red |
| `cluster_nodes` | Observed node count |
| `active_shards`, `unassigned_shards`, `relocating_shards` | Shard allocation/recovery |
| `jvm_heap_used_bytes`, `jvm_heap_max_bytes` | Heap utilization/capacity |
| `docs`, `store_bytes` | Document count and indexed storage |
| `indexing_total`, `search_query_total` | Workload counters; use rates |
| `gc_collection_seconds_total` | Accumulated GC time |
| `thread_pool_rejected_total` | Rejected work counter |
| `fs_available_bytes` | Available data-filesystem space |

Component metrics are `karakoram_kibana_up`, `karakoram_kibana_status_level`; `karakoram_logstash_up`, `karakoram_logstash_events_in_total`, `karakoram_logstash_events_out_total`, `karakoram_logstash_jvm_heap_used_bytes`; and `karakoram_apm_up`, `karakoram_apm_requests_total`, `karakoram_apm_events_accepted_total`. Generic process CPU/RSS/disk metrics remain available. Use external alerting/history for independent availability monitoring; the control plane's own collector is not an HA monitoring service.

`karakoram_kibana_status_level` uses 0 available, 1 degraded and 2 unavailable.
Monitoring query steps may average categorical samples: fractional historical
cluster/Kibana values describe mixed sampled states, not an additional native
health state.

## Limitations

- Local Storage is single-machine; no host anti-affinity or Elasticsearch shard-awareness policy is configured, so independent volumes do not establish failure domains.
- Host kernel/process limits remain Zerops constraints; the measured map-count value meets bootstrap minimum but not Elastic's recommendation.
- Small RAM/heap defaults need workload/recovery sizing; they are not Elastic production-capacity certification.
- Topology type is immutable; no arbitrary role pools, ML provisioning, version skew or downgrade is supported.
- Single-node maintenance interrupts service; two combined nodes lack failure-tolerant quorum; insufficient shard capacity blocks drain.
- Public Kibana/APM subdomains have a 50-MB upload cap and shared-balancer production caveat; custom-domain management is out of scope.
- Logstash queues on container disks are lost across redeploy/replacement, even with `persistentQueue` enabled.
- Snapshot restore is destructive, includes security/global state and needs compatible index versions; Elasticsearch PITR is not offered.
- S3 compatibility requires real repository semantics; unsafe conditional-write bypasses are not supported.
- One S3 client is configured per referenced storage; adding a new storage requires a rolling restart before repository use.

Read [known platform limitations](../limitations.md) and the Elasticsearch decisions [0025](../adr/0025-elasticsearch-engine-runtime-model.md), [0026](../adr/0026-elasticsearch-cluster-formation-and-topology-changes.md), [0027](../adr/0027-elastic-stack-components.md) and [0028](../adr/0028-elasticsearch-security-and-snapshots.md). MongoDB-specific procedures elsewhere in the guide, especially PBM/PITR, do not apply to Elasticsearch.

## Licensing

Karakoram contains no Elastic code, does not redistribute binaries and downloads default distributions at deploy time on the user's infrastructure. Elasticsearch/Kibana default distributions are ELv2; AGPLv3/SSPL are source options, not licenses for those downloaded binaries. Logstash/APM offer OSS builds, but Karakoram uses default builds for consistent Stack features. Basic-tier security, TLS, API keys, service accounts, ordinary snapshots and monitoring are used; paid-feature controls are not bypassed.

ELv2's managed-service restriction still applies to operators reselling substantial direct Elasticsearch/Kibana functionality. Non-redistribution and customer infrastructure are not blanket exemptions. Obtain appropriate rights from Elastic and legal review of your commercial model. OpenSearch (Apache-2.0 core) is a possible later engine, not currently this provider. This is not legal advice; see [ADR 0029](../adr/0029-elastic-licensing.md), [Elastic licensing FAQ](https://www.elastic.co/pricing/faq/licensing) and [ELv2 examples](https://www.elastic.co/licensing/elastic-license/faq).
