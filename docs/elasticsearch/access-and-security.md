<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Access and security

Connect clients and manage [Elasticsearch](https://www.elastic.co/elasticsearch), [Kibana](https://www.elastic.co/kibana) and intake identities separately from Karakoram accounts.

## Connect and reveal credentials

Ordinary cluster status contains password-free endpoints. In Overview, expand **How to connect**; reveal credentials only for administration. `karactl clusters connection search --reveal` calls `GET /api/v1/clusters/{cluster}/connection?namespace=NAME`, requiring separate `cluster-credentials:read` permission. The response is sensitive and uses `Cache-Control: no-store`:

| Field | Meaning |
|---|---|
| Credential `elastic` | Human administrative superuser password, not an application identity |
| Credential `apm-secret-token` | APM intake token; not the provider writer password |
| `caCertificate` | Public cluster CA PEM |

Credential URIs describe the cluster; use returned [component endpoints](stack.md), not those URIs, for Kibana/APM. Do not reveal into CI/shared logs or terminal recordings.

**Admin-only health check:** this Bash recipe uses the revealed `elastic` superuser, authenticated [karactl](../cli.md), `jq` and `curl`. Run it from a trusted diagnostic workload in the namespace's [Zerops](https://zerops.io/) project for private access; for public access, change `PUBLIC=false` to `PUBLIC=true` and use an allowed client. See [namespace networking](../namespaces.md) for private reachability.

```bash
(
  set +x
  set -euo pipefail
  umask 077
  work=$(mktemp -d)
  trap 'rm -rf -- "$work"' EXIT
  PUBLIC=false
  karactl -n default clusters connection search --reveal -o json > "$work/connection.json"
  url=$(jq -er --argjson public "$PUBLIC" \
    '.connection.endpoints | map(select(.name == "elasticsearch" and .public == $public))[0].url // empty' \
    "$work/connection.json")
  jq -er '.caCertificate' "$work/connection.json" > "$work/ca.pem"
  jq -er '.credentials[] | select(.username == "elastic") | "user = " + ("elastic:" + .password | @json)' \
    "$work/connection.json" > "$work/curl.conf"
  curl --config "$work/curl.conf" --fail-with-body --cacert "$work/ca.pem" \
    "$url/_cluster/health?pretty"
)
```

Files are owner-only and removed on exit; protect temporary storage and the diagnostic host too. `jq -r` preserves actual PEM newlines; alternatively export with `karactl -n default clusters ca search`. Never use `-k`. Password clients need the CA, not transport certificates or private keys.

Provision scoped users/API keys from a trusted operator environment, then deliver only that app identity, returned endpoints and CA file through the application's persistent runtime secrets/configuration. Do not install a Karakoram management token in the application. Supply multiple data-node endpoints for failover. Public clients must use provisioned public addresses and disable sniffing that discovers unreachable private hosts.

**Workload-side verification:** mount the application's scoped key's `encoded` value (base64 `id:key`) at `/run/secrets/elasticsearch-api-key` and the CA at `/run/secrets/elasticsearch-ca.pem`. Set `ELASTICSEARCH_URL` to a returned data-node endpoint. This checks the `orders-*` read grant shown below without a Karakoram token or admin identity:

```bash
(
  set +x
  set -euo pipefail
  umask 077
  work=$(mktemp -d)
  trap 'rm -rf -- "$work"' EXIT
  { printf 'header = "Authorization: ApiKey '; tr -d '\r\n' < /run/secrets/elasticsearch-api-key; printf '"\n'; } > "$work/curl.conf"
  curl --config "$work/curl.conf" --fail-with-body \
    --cacert /run/secrets/elasticsearch-ca.pem \
    "$ELASTICSEARCH_URL/orders-*/_search?size=0"
)
```

The secret header is read from an owner-only curl configuration, never a process argument. Use an existing authorized index pattern; a successful request verifies that workload's CA, connectivity and read access, not cluster-wide health.

## Listeners and TLS

| Listener | Port/protocol | Reachability |
|---|---|---|
| Elasticsearch HTTP | 9200 HTTPS | Private nodes; optional public TCP to data/combined nodes |
| Elasticsearch transport | 9300 mutual TLS | Private nodes only |
| Kibana | 5601 HTTP | Private; optional public HTTPS subdomain |
| Logstash monitoring API | 9600 HTTP | Localhost only |
| Logstash Beats input | 5044 TCP | Private |
| Logstash HTTP input | 8080 HTTP | Private |
| APM intake | 8200 HTTP | Private, dual-stack `[::]`; optional public HTTPS subdomain |
| APM monitoring | 5066 HTTP | Localhost only |

Elasticsearch security is always enabled. HTTP and transport use per-node certificates under the cluster CA; transport requires client authentication with CA-based certificate verification. Components verify Elasticsearch HTTPS and CA trust automatically. Kibana/APM public routes terminate TLS at the balancer, forwarding HTTP on the private project network; [Logstash](https://www.elastic.co/logstash) inputs are not secured automatically.

CA sources are `selfSigned` or `userProvided`. The latter requires a certificate and matching signing private key, supplied through protected write-only TLS inputs; see [PKI input contract](../../api/openapi.yaml). CA source/material cannot change in place. Leaf rotation uses `karactl clusters rotate-tls search --wait --yes` and watched PEM reload without Elasticsearch restart; it does not rotate the CA or Kibana encryption keys.

## Public access

Elasticsearch `spec.exposure` explicitly enables public TCP, chooses IPv6 or dedicated IPv4, and applies the shared [public exposure allowlist and acknowledgement rules](../security.md#public-exposure-allowlists) with end-to-end Elasticsearch TLS. Dedicated masters and transport/management ports are not published. Routing status does not prove external reachability: test allowed and denied clients with CA validation.

**Warning:** Kibana/APM HTTPS subdomains are internet-reachable without source-IP restrictions. Elasticsearch's database allowlist does not protect them. Kibana requires Elasticsearch/Kibana authentication; APM requires intake authentication. These switches do not publish Elasticsearch or Logstash.

Set `engineOptions.kibana.public` or `engineOptions.apm.public` and include the corresponding request-only `allowAllAddresses: true` when creating/enabling that route. An unchanged public route needs no new acknowledgement; disabling/re-enabling does. The acknowledgement is consumed, not persisted or inherited by future PATCH requests. The wizard/topology editor confirms this exposure and Review lists every public route even with private database access.

Setting either public switch to `false` revokes that component's HTTPS route before configuration rolls or component-readiness waits, including an already-running roll. Revocation still waits for any accepted platform operation and uses the normal ownership checks and operation receipt; it does not mark the component ready or acknowledge the new generation. Enabling a route remains part of normal reconciliation.
On an existing cluster, both route enable and disable still require an open
[maintenance window](../operations.md#maintenance-windows) or generation
override: a backend may replace the component task to change ingress. Use the
override for urgent route withdrawal; readiness is not an additional prerequisite.

These are Zerops shared subdomains, not custom-domain HTTP locations; custom-domain source-IP policies cannot attach to them. Karakoram supplies Kibana's public base URL and secure cookies. Zerops documents a 50-MB upload cap and does not recommend the shared subdomain balancer for production. Custom-domain management is outside this feature; see [Zerops public access](https://docs.zerops.io/references/networking/public-access).

## Users, roles and API keys

Karakoram RBAC controls management; native identities control Elasticsearch/Kibana. **Elastic Access** manages users, roles, API keys and editable application grants. All routes below use `database-users` permissions (`read`, `create`, `update`, `delete`) on the selected cluster/namespace:

| Resource | Routes below `/api/v1/clusters/{cluster}/elasticsearch` |
|---|---|
| Users | `GET/POST /users`; `GET/PUT/DELETE /users/{username}` |
| Roles | `GET/POST /roles`; `GET/PUT/DELETE /roles/{role}` |
| API keys | `GET/POST /api-keys`; `DELETE /api-keys/{id}` invalidates |

Create a role before assigning it; unknown roles are rejected. User create bodies contain `{username,password,roles,fullName,email,enabled}`; passwords require eight characters and are write-only. Roles are string names. User reads cannot recover app passwords.

Save this role as `role.json` for read access to `orders-*` and Kibana's Default space:

```json
{
  "name": "orders-reader",
  "cluster": [],
  "indices": [{"names": ["orders-*"], "privileges": ["read", "view_index_metadata"], "allowRestrictedIndices": false}],
  "applications": [{"application": "kibana-.kibana", "privileges": ["read"], "resources": ["space:default"]}]
}
```

Kibana needs both application and index grants. Use resources `["*"]` for all spaces or explicit `space:<id>` entries. Kibana registers application privileges at startup; native acceptance of arbitrary names/privileges does not establish browser access. The editor warns for unknown forms, not against a live privilege inventory: known forms include `all`, `read`, `space_all`, `space_read`, and `feature_<name>.<privilege>` with `all`, `read`, `minimal_all`, `minimal_read` or subfeatures such as `url_create`. Custom application grants remain valid. Prefer [Kibana's own role UI/API](https://www.elastic.co/docs/deploy-manage/users-roles/cluster-or-deployment-auth/kibana-privileges) for general management.

Role bodies contain `cluster`, `indices` and `applications` arrays; empty arrays grant nothing. Writes omit response-only `reserved`; undeclared nested fields are rejected. Index entries support opaque JSON-string `query` and `fieldSecurity` with `grant`/`except` arrays. These restrictions require an appropriate paid licence, not Basic; unavailable features return an actionable 4xx rather than an unrestricted role. Reads/edits preserve restrictions, including empty grants. The UI accepts an index-privilege JSON array; API-key descriptors support identical restrictions. Errors identify the relevant cluster/index/application field, or `roleDescriptors.<name>.indices[i].query`/`.fieldSecurity` for keys; cross-cluster names in index diagnostics are not cluster-grant errors.

```sh
karactl elasticsearch roles create search orders-reader -f role.json
# APP_PASSWORD comes from protected secret input; keep tracing disabled.
printf '%s' "$APP_PASSWORD" | karactl elasticsearch users create search ordersreader \
  --roles orders-reader --password-stdin
karactl elasticsearch users get search ordersreader
karactl elasticsearch roles update search orders-reader -f role.json
karactl elasticsearch users update search ordersreader --roles orders-reader
```

| Commands | Flags/semantics |
|---|---|
| `users list`, `roles list`, `api-keys list` | `--limit` (100 default), `--cursor`; inherited namespace/output flags |
| `users create/update` | `--roles` replaces role names; `--full-name`, `--email`, `--disabled`/`--disabled=false`; omitted update flags retain values |
| Password changes | `--password-stdin`; required on create; never place secrets in arguments |
| `roles create/update` | Required `-f` JSON (`-` for stdin); command name authoritative and file name must match; omitted arrays empty; update replaces all privileges |
| `users/roles get` | `<cluster> <name>` |
| `users/roles delete` | `<cluster> <name> --yes` |

Reserved/built-in and `karakoram*` users are hidden/protected; built-in roles are readable/assignable but immutable, and `karakoram*` roles are hidden/protected. Do not mutate managed identities through native CRUD.

API-key creation requires a name and a **nonempty** `roleDescriptors` map; inherited superuser privileges are never accepted. Save only that map in `descriptors.json`, for example:

```json
{"orders-read":{"cluster":[],"indices":[{"names":["orders-*"],"privileges":["read"]}],"applications":[]}}
```

```sh
karactl elasticsearch api-keys create search --name orders-query --expiration 30d -f descriptors.json
karactl elasticsearch api-keys list search
karactl elasticsearch api-keys invalidate search KEY_ID --yes
```

The REST body wraps the map in `{name,expiration,roleDescriptors}`; CLI `--expiration` accepts an Elasticsearch duration. Creation reveals `id`, `apiKey`, `encoded` once: store securely, never log it. Only Karakoram-created keys are listed/invalidated; lists omit secrets. Use the encoded value with `Authorization: ApiKey` for Elasticsearch, not as a Karakoram management token. Prefer explicit expiry/narrow grants; [APM intake keys](stack.md) need their own privileges.

Native user/role writes and API-key creation support [safe HTTP retries](../api.md#safe-retries-with-idempotency-key), including replay of one-time secrets. Without keyed retries, resolve uncertain creation by listing/invalidating unwanted keys first. Invalidation of an already invalidated key succeeds; retry and refresh inventory. A 404 remains an error. These access mutations are not durable cluster Operations. Deletion/invalidation needs confirmation: non-TTY callers pass `--yes` or pipe `y`; empty stdin never submits.

## Managed credentials and secret locations

Every node has file-realm `karakoram` (superuser control/recovery) and `karakoram-monitor` (cluster/index monitoring). The control identity remains usable when restoring native security state; it is not an app identity or a revealed credential. Kibana uses an `elastic/kibana` service-account token; Logstash/APM have separate scoped provider writers.

Regular reconciliation reasserts the engine driver-owned Logstash/APM writer roles, native users, enabled state and sealed passwords even when desired secrets have not changed. Native edits/deletion of these managed objects are repaired; customer-owned users and roles are not modified. Kibana's service token is separately authenticated and replaced if revoked.

Node [S3](https://aws.amazon.com/s3/) keys reside in `elasticsearch.keystore`; rendered configuration, TLS keys, file-realm files and component secrets are owner-only (0600 files, 0700 directories). Kibana's token/encryption keys and APM's intake token/writer password are in component configuration; Logstash's writer credential is injected into its process environment. These are accessible to privileged runtime administrators: see [trust boundaries and encryption at rest](../security.md).

`karactl system-users list search` lists rotation targets: `elastic`, `karakoram-monitor`, `kibana-token`, `logstash`, `apm`, `apm-secret-token`. Use `karactl system-users rotate search IDENTITY`; rotation republishes/restarts affected components. The internal file-realm control user is not a rotation target. Credential rotation does not change the CA or cluster-lifetime Kibana encryption keys.
