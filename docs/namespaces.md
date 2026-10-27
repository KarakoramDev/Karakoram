<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Namespaces and relays

A namespace maps one backend target to its infrastructure credentials, management routing and RBAC scope. Each namespace has an immutable backend kind: `zerops` ([Zerops](https://zerops.io/) projects, see [Zerops backend](zerops.md)), `dokploy` ([Dokploy](https://dokploy.com/) environments on a [Docker Swarm](https://docs.docker.com/engine/swarm/), see [Dokploy backend](dokploy.md)), or `nomad` ([Nomad](https://developer.hashicorp.com/nomad) namespaces, see [Nomad backend](nomad.md)). Existing records and omitted create kinds default to `zerops`.

`default` is the control-plane target: `KARAKORAM_BACKEND` selects its kind (default `zerops`, token from `KARAKORAM_ZEROPS_TOKEN`; `dokploy` reads `KARAKORAM_DOKPLOY_*`; `nomad` reads `KARAKORAM_NOMAD_*`, including `KARAKORAM_NOMAD_NAMESPACE`, `KARAKORAM_NOMAD_ADDR` and `KARAKORAM_NOMAD_TOKEN`) and networking is direct. Additional namespaces use an outbound management relay when their backend requires one (`BackendInfo.Relay`); all three current backends require it. Relays are not application VPNs or replication paths; namespaces are not hostile-tenant isolation. See [architecture](architecture.md) and the [relay threat model](security.md#relay-threat-model).

## Names and selection

Names match `^[a-z][a-z0-9-]{0,30}[a-z0-9]$`: 2–32 lowercase letters/digits/hyphens, starting with a letter and ending with a letter/digit. `default` is reserved. A target belongs to only one namespace within its backend kind. Name/backend/target identity are immutable; description is editable. Cluster/storage names are namespace-local.

| Interface | Selection |
| --- | --- |
| CLI object/create/mutation | `--namespace/-n` → saved context → `default` |
| CLI collections | Without explicit `-n`, all authorized namespaces; with `-n`, filtered |
| UI | Switcher offers authorized namespaces and **All namespaces**; choose an explicit namespace for creation |

```sh
karactl config set-context production --namespace prod
karactl -n prod clusters list
```

Responses carry `namespace`; unfiltered tables identify it. CLI/UI namespace lists follow all authorized pages. Namespace metadata requires its own read grant; cluster access does not imply it. UI selection uses `?namespace=` and a non-secret local preference; shared links do not bypass RBAC.

An explicit nonexistent `?namespace=NAME` returns **404 `not_found` only if the caller holds a permission in that namespace**; otherwise collections remain empty and object requests return their normal **403**. The 404 identifies `details.resource: namespace` and `details.name`. Omit the parameter for all namespaces; empty/malformed values return **400**.

## Backend discovery

`GET /api/v1/backends` and `karactl backends list` require `namespaces/read` on global `*` and list registered backend kinds, display names, status, enabled capabilities, relay requirements and accepted non-secret config keys. Namespace responses include `backend`, `capabilities` and non-secret `config`; use `karactl namespaces get NAME -o yaml` to inspect them. Capabilities describe optional infrastructure features, not engine-provider features or availability guarantees.

In **Settings → Namespaces**, the table and detail identify the backend; the detail's **Capabilities** section marks each known feature **Supported** or **Not supported**.

Namespace creation accepts `backend` (default `zerops`) and a string-to-string `config` object. CLI flags are `--backend zerops|dokploy|nomad` and repeatable `--config key=value`; Zerops accepts no config keys, while [Dokploy](dokploy.md#target-settings) and [Nomad](nomad.md#target-settings) list their settings in their guides. Zerops's API URL is operator-only, set with `KARAKORAM_ZEROPS_API_URL` for all namespaces, including legacy records; namespace callers cannot override it. Credentials belong in the write-only token input, never config. Unknown backend kinds/config keys are rejected with **400**.

Cluster create, replace and patch requests check the namespace backend's advertised capabilities after engine defaults and validation. Specs requiring unavailable features (such as public TCP exposure or dedicated CPU) return **400 `invalid_request`** before any cluster change is saved; capability admission uses static backend metadata, not a platform API request.

## Add, validate and rotate

The example below registers an additional target on the **Zerops backend**. [Dokploy](dokploy.md#managing-dokploy-from-another-control-plane) and [Nomad](nomad.md#target-settings) require their own target settings and credentials; see their guides before registration.

1. Create a token for the additional project using the [required Zerops token scope](security.md). Allow capacity for one relay runtime and an available hostname, normally `karakoramrelay`.
2. Ensure `KARAKORAM_PUBLIC_URL` is an HTTPS origin reachable from that project. Proxies must forward WebSocket Upgrade at root `/relay/v1/connect`, without browser-login interception; admission uses inner mTLS.
3. Choose **Settings → Namespaces → Add namespace → Validate and add**, or:

```sh
# Store the project token in a private, untracked file; '-' reads stdin.
karactl namespaces create --name dev --backend zerops --project-id '<project-id>' \
  --token-file namespace-token.txt
karactl namespaces get dev -o yaml
# Once the relay is connected, select the namespace for cluster commands.
karactl -n dev clusters list
karactl namespaces validate dev
karactl namespaces update dev --description 'Development and test'
karactl namespaces rotate-token dev --token-file replacement-token.txt --yes
```

Tokens are write-only. Never put them in argument values, committed files or shell traces. Registration validates synchronously and returns **201**; deployment follows asynchronously. Missing required fields/invalid names return **400**; existing/reserved names **409**; invalid credentials/project scope, duplicate projects or occupied relay hostnames **422**.

Validation checks project identity/service-list access, not every mutation grant: insufficient read-capable tokens can pass and fail deployment. For Zerops, wait for `token.valid: true`, namespace/relay `Ready` and `relay.connected: true`. A backend that does not require a relay reports `relay.phase: NotRequired` and uses direct management connectivity.

Re-validation reports invalid stored credentials through `token.valid: false`/`lastError`, not necessarily an HTTP error. A manual validation request returns a stored-token read error to the caller without changing token health. Checks run about every five minutes. Explicit **401/403** or authentication errors invalidate tokens; **429/5xx**, timeouts and connection failures retain last-known validity with `Zerops API temporarily unavailable: <safe detail>; retrying`. First validation must succeed before validity is known. Transient create/rotation failures return **500 `internal_error`**, not 422, and do not save submitted credentials; rejected or unsupported backend requests return **400**.

Rotation validates the **same backend target**, seals the replacement and updates live infrastructure access without restarting the relay/session. Revoke the old token only after success. `default` API rotation returns **409**: update its service secret and restart instead.

## Relay health

Each relay is one private [Ubuntu](https://ubuntu.com/) runtime running `karakoram relay`, with private TCP 9443 and no public access. It connects outbound using WSS, inner mTLS 1.3 and multiplexed streams. Remote agents pull configuration through `https://<relay hostname>:9443` with end-to-end internal TLS.

The outer HTTPS certificate must match the host in `KARAKORAM_PUBLIC_URL`
and chain to the **relay runtime's system trust store**. `KARAKORAM_RELAY_CA`
pins the separate inner mTLS connection; it does not trust your HTTPS ingress.
With a private HTTPS issuer, install its public CA certificate in the
operator-provided runtime image (on Ubuntu, under
`/usr/local/share/ca-certificates/`, then run `update-ca-certificates`) before
deploying the relay. Updating a node's or CLI's trust store alone does not
update an existing allocation's private rootfs. Never disable verification.

Disconnected status distinguishes an authenticated session that dropped from
no session observed by this control plane and names the outbound endpoint.
Observation history is process-local: after a control-plane restart, absence
does not prove the relay never connected in its lifetime. Pre-connect DNS,
TCP, outer TLS and WebSocket failures cannot reach the hub; inspect the relay
runtime logs for the typed failure stage and reason. Logs intentionally omit
raw transport error text, HTTP response bodies and credential material.
Outer HTTPS unknown-authority failures require system trust installation;
inner mTLS unknown-authority failures instead require checking the pinned
`KARAKORAM_RELAY_CA` and hub enrollment. HTTP 401/403 means ingress
authentication/authorization rejection, not just a missing Upgrade route.
Diagnostic authorities are capped at 192 bytes; longer targets are abbreviated
so reconnect logs and persisted health messages remain bounded.

The hostname appends `relay` to the control-plane hostname; over 25 characters it uses the first sixteen characters, four hexadecimal SHA-256 characters and `relay`. `KARAKORAM_RELAY_HOSTNAME` affects new enrollments only; recorded hostnames/SANs remain unchanged.

The relay's destination policy authorizes exact logical member/port pairs, not
arbitrary IPs or dotted hostnames. [PostgreSQL](https://www.postgresql.org/) proxy readiness and retirement
add an instance selector while retaining the logical member's TLS identity.
The hub checks each selector against fresh live-container inventory from that
namespace's own backend and member before sending it over authenticated mTLS.
Only the agent port (9443) supports instance selection. The relay still rejects
loopback, link-local, unspecified and multicast destinations, including DNS
answers; inventory failure or an absent/stopped/unowned instance fails closed.
Every running proxy must acknowledge the applied drain version separately.

| Field | Meaning |
| --- | --- |
| `phase` | Namespace `Pending`, `Ready`, `Error`, `Deleting` |
| `token.source`, `valid` | `env` for default or `stored`; last validation result |
| `token.lastValidatedAt`, `lastError`, `rotatedAt` | Validation/rotation timestamps and diagnostic; no token/fingerprint |
| `relay.hostname`, `serviceId` | Private service identity |
| `relay.phase` | `NotRequired` for default or a non-relay backend; otherwise `Pending`, `Deploying`, `Ready`, `Error` |
| `relay.connected`, `connectedSince`, `lastSeenAt` | Authenticated session/activity |
| `relay.version`, `desiredVersion` | Reported/desired executable; digest mismatch triggers redeployment |
| `relay.certNotAfter` | Certificate expiry; renewal starts within thirty days |
| `relay.reconnects`, `lastError` | Process-local hub reconnect count and diagnostic, not a lifetime SLA |

`/healthz`, platform ACTIVE and `karakoram relay readiness` do not prove tunnel connectivity: relay readiness probes only its local listener. Transport bounds are 1 MiB per WebSocket message and 256 active data streams per session, excluding control, not a total-payload cap.

## Relay operations

```sh
karactl namespaces list
karactl namespaces get prod -o yaml
karactl namespaces validate prod
karactl -n prod clusters status orders
karactl -n prod events list
karactl -n prod orphaned-volumes list
```

Invalid credentials/unavailable routing degrade that namespace's management; other namespaces continue reconciling. Existing engine processes may continue, but observations, new configuration pulls and backup control/scheduling can fail or pause. Cached configuration can preserve startup; uncached bootstrap requires connectivity. Do not clear maintenance holds or repeat restores solely because management disconnected.

Relays reconnect automatically with backoff/jitter capped at thirty seconds. Inspect `RelayConnected`, `RelayDisconnected` and `RelayTargetRefused` events and the backend's deployment/runtime logs without exporting secrets. Certificate renewal replaces env secrets and restarts the relay; upgrades redeploy on executable-digest mismatch. Check connection and desired/reported versions after [upgrading](upgrade.md).

Observed owned relays that are stopped or failed are restarted even when their
deployed executable digest is current; a runtime awaiting its executable is
redeployed. Recovery has a persisted one-minute cooldown and does not restart a
connected healthy relay. An accepted backend operation remains a safety fence:
Karakoram never treats an elapsed timeout as permission for a duplicate mutation.
`lastError` identifies a pending operation and, after 15 minutes, asks for backend
inspection or cancellation. Let the backend expose its terminal outcome; never
erase the control-plane operation checkpoint to unblock deletion or deployment.
Cancelled namespace mutation requests stop waiting for an occupied lifecycle
lock without cancelling or duplicating the operation that already owns it.

## Backup storages

Register storage in the cluster's namespace (`karactl -n dev storages create -f FILE`); backup settings/schedules reference local storage names. Identical names need not mean identical buckets/credentials. New names use 2–63 lowercase letters/digits/hyphens, starting with a letter; trailing hyphens are allowed, and older stored names remain manageable. Verification runs from the control-plane network, not the relay; backup transfer paths are engine-specific. See [storage and cross-namespace recovery](backups.md).

## RBAC examples

Add to a complete policy retaining administrator access; see [policy format](access-control.md).

```csv
# Development authority.
p, user:alice, *, *, dev/**
# Production reads, excluding credential reveal.
p, user:alice, *, read, prod/**
p, user:alice, cluster-credentials, read, prod/**, deny
# Separate credential and namespace metadata grants.
p, user:bob, cluster-credentials, read, prod/orders
p, user:bob, namespaces, read, prod
# Namespace audit; not global installation activity.
p, user:auditor, audit, read, dev/**
# Source export; backup read and target mutation are still required.
p, user:bob, backups, export, prod/**
```

`dev/**` spans namespace/objects; `dev/*` does not span further separators. Namespace creation requires `namespaces/create` on global `*`. Fresh operator/viewer namespace grants and upgrade-policy preservation are documented in [Default roles](access-control.md#default-roles).

## Remove

```sh
karactl namespaces delete dev --yes
karactl namespaces get dev -o yaml
```

Deletion returns **202 without an Operation ID/body** and marks `Deleting`; poll `namespaces get` until removed (404 for an authorized caller), or follow lifecycle events. There is no `--wait` and no `operations wait` target.

**409** blocks removal while clusters (including deleting), storages, retained backup records or pending orphaned volumes remain. Finish cleanup first; retain valid credentials until the owned relay is removed. `default` cannot be deleted. Deletion removes credentials/state and revokes active/pending relay sessions, not the project or RBAC grants. Review stale grants before name reuse; replacement enrollment starts fresh.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Token invalid | Explicit access rejection | Validate scope/project; rotate the infrastructure token, not a Karakoram API token |
| Temporary Zerops API error | Rate limit/outage/connectivity | Keep token; inspect timestamps; allow automatic retry or validate after recovery |
| Nomad namespace validation returns 422 `invalid_namespace` with a TLS diagnostic | Server certificate verification or client certificate rejection | Follow the named TLS setting: check `KARAKORAM_NOMAD_TLS_SERVER_NAME` against certificate SANs, `KARAKORAM_NOMAD_CACERT` for trust, or `KARAKORAM_NOMAD_CLIENT_CERT` / `KARAKORAM_NOMAD_CLIENT_KEY` for mTLS; no namespace or token is saved |
| Relay Pending/Deploying/Error | Capacity, deployment or ownership conflict | Inspect `lastError`/platform logs; never adopt a foreign hostname or replace enrollment manually |
| Disconnected | Origin/certificate/WebSocket failure | Check outbound HTTPS/Upgrade and session fields; do not restart database members first |
| Target refused | Unresolved/unavailable target or policy update lag | Brief refusals during topology changes are expected; persistent refusals need generated-hostname checks; never broaden policy to arbitrary services/IPs |
| Agents cannot pull | Tunnel/listener/CA/SAN/token mismatch | Inspect assigned-member configuration; never disable TLS verification |
| Recovery 403 | Missing source/target authority or deny | Check [cross-namespace recovery permissions](backups.md) |
| Removal 409 | Namespace inventory remains | Inspect storages, retained backups and orphan inventory, not just clusters |
