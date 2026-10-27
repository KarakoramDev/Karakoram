<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Weaviate access and TLS

Use separate application database users and least-privilege native roles. Karakoram authentication/RBAC controls management requests; [Weaviate](https://weaviate.io/) API keys/RBAC control database requests. These are separate authorization systems.

## Connections and administrative credentials

```sh
karactl -n default clusters connection vectors
karactl -n default clusters ca vectors > vectors-ca.pem
karactl -n default clusters connection vectors --reveal
```

The ordinary connection summary is password-free. Explicit reveal requires `cluster-credentials:read`, is audited and returns `Cache-Control: no-store`. It exposes the protected `karakoram-admin` root API key and public CA, not an application identity. Keep revealed output out of logs, shell tracing and shared terminals. CA export also requires credentials-read permission.

REST uses port 8080 and client gRPC uses 50051. Configure your native client with the advertised member endpoint, API key and trusted cluster CA; retain hostname verification. REST authentication uses `Authorization: Bearer <API-key>`. Do not send administrative keys to applications or provider inference services.

Anonymous access is disabled. API-key authentication, native RBAC and dynamic database users are enabled; OIDC is not configured. Shared `karakoram-cluster` credentials authenticate internal data/replication calls and internal Raft removal, not application requests, gossip or Raft consensus.

## TLS and exposure

| `spec.tls.mode` | REST | Client gRPC | Public exposure |
| --- | --- | --- | --- |
| `requireTLS` (default) | HTTPS-only on 8080; no parallel plaintext REST listener | TLS on 50051 | Optional, subject to provider capability and CIDR allowlist |
| `disabled` | HTTP on 8080 | Plaintext | Rejected; private-only |

`preferTLS` is unsupported. CA source is `selfSigned` (default) or `userProvided`; the CA-source selection is immutable. Trust the returned public CA and use endpoint names/IPs covered by the member certificate. Generated leaf SANs cover member name, localhost, loopbacks and applicable published addresses.

Both native listeners load certificates **at startup**. Replacing PEM files is not a hot reload: `rotate-tls` issues leaves and restarts members in order, non-voters before voters. It retains the existing CA unless user-provided material has been changed; coordinate trust changes separately. A singleton is unavailable during restart. See [operations](operations.md#rotation).

For rolling recovery only, readiness/health observations can verify the **exact pinned active expired leaf** at a time within its validity interval, provided the same durable roll includes that member and a valid replacement leaf. TLS rotation can also use this pinned observation for its replacement preflight. CA chain, server-auth usage and member hostname verification still apply; a different expired leaf or a not-yet-valid leaf is rejected. Ordinary readiness, administrative native mutations and application connections retain normal current-time TLS verification. Status reports expired native certificates explicitly and does not mark affected members healthy, including while renewal is in progress.

Client TLS does not cover internal transports. Require the [private network isolation and ports](README.md#ports-and-network-isolation) before deployment. Keep gossip, data, Raft, metrics and agent management private. Port 2112 is wildcard-bound upstream; lack of a public mapping alone does not restrict private-network peers.

Namespace relay/control paths reach exact member targets on **8080, 7947 and 9443 only**: native REST, authenticated internal Raft removal and agent HTTPS. Client gRPC 50051 is not relayed; neither are gossip, Raft consensus/RPC or metrics.

## Native users and roles

The web UI's **Weaviate access** tab is capability-gated by `weaviateAccess`. The equivalent CLI families are `karactl weaviate users` and `karactl weaviate roles`; generic [MongoDB](https://www.mongodb.com/) `db-users`/`db-roles` do not apply.

```sh
karactl -n default weaviate users list vectors
karactl -n default weaviate users create vectors reader --role viewer
karactl -n default weaviate users get vectors reader
karactl -n default weaviate users update vectors reader --role viewer --inactive
karactl -n default weaviate users update vectors reader --role viewer --active
karactl -n default weaviate users rotate-key vectors reader --yes
karactl -n default weaviate users delete vectors reader --yes
karactl -n default weaviate roles list vectors
karactl -n default weaviate roles create vectors -f role.yaml
karactl -n default weaviate roles get vectors collectionreader
karactl -n default weaviate roles update vectors collectionreader -f role.yaml
karactl -n default weaviate roles delete vectors collectionreader --yes
```

`--role` is repeatable. User update **replaces the complete role list**; omission means no roles. `--active` and `--inactive` are mutually exclusive. API-shaped files can replace convenience bodies: create user `{name, roles}`, update `{roles, active?}`. Role create/update accepts `{name, permissions}`; permissions use native action/resource selectors. For example, `role.yaml`:

```yaml
name: collectionreader
permissions:
  - action: read_collections
    collections: {collection: Articles}
  - action: read_data
    data: {collection: Articles, tenant: "*", object: "*"}
```

The native access layer hides environment/system users. `karakoram-admin`, `karakoram-cluster` and environment-defined users are not mutable application identities. Built-in/protected roles `root`, `admin`, `viewer`, `read-only` and `operator_metadata_reader` cannot be created, replaced or deleted through native access. Assign only the privileges applications need; custom roles are not assumed read-only merely because their names sound restrictive.

### Management authorization

All grants below apply to the same namespace/cluster object:

| Action | Required Karakoram authority |
| --- | --- |
| List/get users or roles | `database-users/read` |
| Create user | `database-users/create`; assigning **any** role other than `viewer` or `read-only` also requires `clusters/update` |
| Update user | `database-users/update`; assigning **any** role other than `viewer` or `read-only` also requires `clusters/update`, **including roles already present** |
| Delete user | `database-users/delete` only, even if the user holds privileged roles; protected identities remain excluded |
| Rotate database-user key | `database-users/update`; if current roles include **any** role other than `viewer` or `read-only`, also `clusters/update` |
| Create/replace/delete role | **Both** `database-users/update` and `clusters/update` |

Protected identities/roles remain protected even with those grants. Native privileges do not grant Karakoram management access, and Karakoram cluster-read does not grant credential reveal.

## REST resource family and secrets

Paths below follow `/api/v1/clusters/{cluster}`; use `?namespace=NAME`:

| Suffix | Methods |
| --- | --- |
| `/weaviate/users` | GET list, POST create |
| `/weaviate/users/{user}` | GET, PUT roles/activation replacement, DELETE |
| `/weaviate/users/{user}/rotate-key` | POST |
| `/weaviate/roles` | GET list, POST create |
| `/weaviate/roles/{role}` | GET, PUT replacement, DELETE |

Creation returns `{user, apiKey}`; key rotation returns `{apiKey}`. Secret-bearing responses are no-store. Store generated keys securely: GET never retrieves them. Matching HTTP `Idempotency-Key` retries replay sealed secret results for 24 hours; the CLI does not send idempotency keys, so do not blindly repeat a creation/rotation after losing its response. See [API retries](../api.md#safe-retries-with-idempotency-key) and [CLI](../cli.md).

The UI shows a newly created/rotated key once in a dedicated dialog, outside the query/mutation cache. Save it before closing the dialog or leaving the tab; either clears the displayed key. Rotation immediately replaces the old key.

Dynamic application-user key rotation is separate from system-user rotation. The root key uses two rolling passes; shared internal credentials require a coordinated full restart. See [rotation](operations.md#rotation).
