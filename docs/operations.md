<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Day-2 operations

Operate Karakoram and its MongoDB clusters as separate systems: control-plane availability, database quorum, PBM recovery coverage and physical placement are different signals. Start with [installation](install.md), [architecture](architecture.md), [security](security.md) and [limitations](limitations.md).

## Health and logs

```sh
curl --fail --show-error https://YOUR-SUBDOMAIN/healthz
karactl health
karactl clusters status demo
karactl clusters members demo
karactl operations cluster demo
karactl events list --cluster demo
```

`/healthz` reports the control-plane store check, not database/PBM health. Inspect cluster phase/conditions, observedGeneration, member role/health/lag, observed binary/FCV, agent config/connectivity, disk capacity and backup/PITR status. A Zerops runtime marked ACTIVE is not sufficient. The platform member exec readiness command `karakoram agent readiness` checks **local TCP 127.0.0.1:27017 only**, without needing credentials/environment; it does not validate replication, users or PBM registration. The provider's authenticated replica-set health gate determines database readiness.

Two log paths serve different purposes:

1. **Zerops runtime/deployment logs:** use the service dashboard's process/application logs. They capture preparation, readiness, startup and forwarded child output. The backend implements the platform's signed project-log mechanism; signed log URLs/access tokens are credentials and must not be pasted into tickets. These logs also cover failures before the node-agent API is reachable.
2. **Node process tails through Karakoram:**

   ```sh
   karactl clusters logs demo demom0 --process mongod --lines 100
   karactl clusters logs demo demom0 --process pbm-agent --lines 100
   # For a router use its actual member name and --process mongos.
   ```

   Public API: `GET /api/v1/clusters/{cluster}/members/{member}/logs?process=mongod&lines=100`. The control plane proxies the private agent's authenticated `GET /v1/logs`. The public default is mongod; specify mongos for routers. The agent retains an **in-memory ring of at most 1000 lines per process**, lost on agent restart. Asking for more does not create historical archive/pagination; the current handler supplies a bounded tail rather than durable log cursors. Agent supervisor/control-plane diagnostics are in Zerops application logs, not a separate arbitrary-file read endpoint.

Redact passwords, credential-bearing URIs, environment contents, API tokens and PEM private keys before exporting logs. Capture cluster/member/operation/event identifiers and timestamps instead of full secret responses.

Platform import, deploy and cleanup failures include the Zerops error code and
a sanitized summary of metadata field/reason values in Events and cluster
status/conditions. The summary is limited to 1024 bytes; credential-bearing
fields, environment contents, signed URLs and raw response bodies are not
included. A message that previously said only “See metadata” now carries the
available safe diagnostic inline, rather than requiring a separate metadata
link. Check the named resource bound or missing service before changing the
specification. For `serviceStackNotFound`, inspect the recorded service/process
identity and whether the service was removed outside Karakoram; do not recreate
or delete unrelated services to clear the error. If details are unavailable or
truncated, inspect that process in the Zerops dashboard and share only redacted
error codes, service/process IDs and timestamps.

## Namespace and relay operations

Use [namespace lifecycle](user-guide/namespaces.md) for registration, validation,
rotation, guarded deletion and health fields. Inspect project/token health
separately from tunnel and database health:

```sh
karactl namespaces list
karactl namespaces get prod -o yaml
karactl namespaces validate prod
karactl -n prod clusters status orders
karactl -n prod events list
karactl -n prod orphaned-volumes list
```

Invalid project credentials or unavailable namespace routing degrade that
namespace's clusters; other projects continue reconciling. Check namespace
`phase`, `token.valid`/validation time/error and relay
`phase`/`connected`/`lastSeenAt`/`reconnects` before restarting database members.
`/healthz` and a relay marked ACTIVE do not establish authenticated tunnel
health. `karakoram relay readiness` probes the private listener only.

An infrastructure API outage is not an invalid-token diagnosis. Namespace token
checks retain last known validity on HTTP 429/5xx, timeouts and connection
failures, with `Zerops API temporarily unavailable: <safe detail>; retrying` in
`token.lastError`. They retry on the usual five-minute validation cadence.
Only explicit access rejection (401/403 or authentication error code) marks a
previously valid token invalid. Check timestamps and the diagnostic before
rotating credentials or repairing a relay. A fresh installation still needs a
successful first validation before its token is known valid.

During a remote namespace outage, MongoDB replication and already-running
member-to-S3 PBM work can continue. Management, new agent configuration pulls,
observations and backup scheduling/control can fail or pause. Cached member
configuration can preserve startup; an uncached bootstrap cannot. Do not clear
restore maintenance holds or submit another restore merely because management
lost connectivity.

The relay reconnects automatically using exponential backoff/jitter capped at
thirty seconds. Verify outbound HTTPS to the configured public origin and
WebSocket forwarding at `/relay/v1/connect`, then inspect the relay's Zerops
runtime/deployment logs without exporting secret environment. `RelayConnected`
and `RelayDisconnected` events identify session transitions; `RelayTargetRefused`
names a target the relay could not open and why. Token validation events report
health transitions. Never publish relay/agent 9443 or broaden target policy to
repair routing.

Rotate stored namespace tokens with `namespaces rotate-token NAME --token-file
FILE` (or stdin `-`); validation precedes replacement and the live relay is not
restarted. For `default`, update the service secret and restart. Client
certificate renewal uses env-secret replacement and a relay restart; executable
upgrades redeploy the same control-plane binary when digests differ. Verify
connection and desired/reported versions after [upgrading](upgrade.md).

The dedicated hub leaf also renews during runtime before expiry. Pending relay
admissions are fenced against enrollment transitions, and namespace deletion
revokes both its active session and pending admissions; deleting credentials
alone is not the session-revocation mechanism.

Before removing a namespace, finish deletion of clusters, storages, retained
backup records and orphaned volumes. Keep a valid token until the owned relay is
deleted. A foreign service occupying the relay hostname is never adopted or
deleted. Removal does not delete the project or its RBAC rules: remove obsolete
grants deliberately before reusing the namespace name.

## Reviewing audit activity

Global and cluster **Events**/**Operations** show the initiating actor in the UI and CLI. Local actions use the persisted account name; OIDC actors include that name and an `(oidc: issuerURL)` suffix. API-token actions include owner, public token name and ID. Background work uses `reconciler`, `scheduler`, `system:startup` or `system` (orphan-volume janitor) unless attributable to an initiating user/operation. Desired-state generations persist the requesting actor, preserving attribution when reconciliation runs later or after restart.

```sh
karactl -n dev events list --cluster demo
karactl -n dev operations cluster demo
# Installation audit access requires audit:read on '*'.
karactl events list --actor admin --resource accounts
karactl events list --resource auth -o json
# Namespace audit filters compose with the selected namespace.
karactl -n dev events list --actor alice --resource namespaces
```

Actor/resource filters are exact, case-sensitive AND matches, also intersected with `--namespace/-n`; omit a flag to leave that field unrestricted and preserve the filters when passing `--cursor`. Copy the full actor value, including OIDC issuer or token metadata, when filtering those actions. `events watch` supports the same actor/resource filters alongside namespace/cluster/type/operation/replay filters. In the global UI, audit-authorized users can apply Actor/Resource filters and inspect Resource/Target together with the namespace switcher; cluster-detail views stay cluster-scoped. Namespace-level audit records require `audit:read` on the namespace; global installation records require it on `*`. A grant such as `dev/**` does not expose another namespace or otherwise unauthorized clusters.

Successful mutations and credential reveals are audited without request/response bodies or secret values; login/OIDC success/failure and logout are also recorded. Audit and cluster events share **indefinite SQLite retention**, without automatic age/count pruning. Monitor state-database disk use and preserve consistent off-platform state backups. The event API is append-only, not a tamper-proof remote audit sink. A remote mutation and local event persistence cannot commit atomically: an absent event or failed request is not sufficient evidence that the remote action did not happen. Inspect operation/resource status before retrying.

See [security](security.md#audit-attribution-and-durability) and [API filter/transport semantics](user-guide/api.md#pagination-and-events).

## Node-agent semantics

The agent receives nonsecret identity/connection environment (`KARAKORAM_URL`, `KARAKORAM_CLUSTER`, `KARAKORAM_MEMBER`, `KARAKORAM_INTERNAL_CA`, `KARAKORAM_DATA_DIR`) and the per-member secret `KARAKORAM_AGENT_TOKEN`. It pins the control-plane internal CA and pulls only its assigned MemberConfig.

| Path under DATA_DIR | Behavior |
| --- | --- |
| `karakoram/config.json` | Atomic owner-only **0600** accepted configuration cache, containing locally necessary secrets |
| `karakoram/keyfile` | MongoDB internal-authentication keyfile, **0400** |
| `karakoram/mongodb.pem`, `ca.pem`, `mongod.conf`/`mongos.conf` | Protected rendered engine files; private material is owner-only |
| `karakoram/controls.json` | Atomic **0600** durable maintenance and explicitly stopped-process holds |
| `karakoram/pbm-receipts/` | Durable **0600** initiation receipts indexed by hashed request ID |
| `db/` | Persistent database files for data-bearing members |

On startup the agent applies cached config if present, then tries a fresh pull. With a usable cache it can run through control-plane unavailability; without one it waits/retries first configuration every five seconds. Normal re-pull is every **60 seconds**, and `/v1/reload` requests an immediate pull. Provider-activated per-member snapshots prevent all nodes adopting a disruptive config/credential change at once. Stateless router caches do not survive runtime replacement.

The supervisor owns child start/stop and ordinary restart backoff and forwards SIGTERM for clean shutdown. Do not launch a duplicate mongod or PBM supervisor. Maintenance holds suppress automatic restarts, including successful PBM exits during physical restore. Explicit process stops survive config pulls and agent restart; only an explicit maintenance-off or process start/restart releases the corresponding local hold. Provider desired restore holds persist in control-plane state as well. Never delete/edit `controls.json` to “fix” an offline restore without understanding who owns the dbpath.

PBM starts use the durable Operation ID as RequestID and pin the command member before initiation. A completed receipt replays the same response; reuse with a different request is rejected. An interrupted receipt with no persisted outcome fails with:

> PBM initiation outcome unknown after interrupted execution; reconcile before issuing another request

This is intentional safety, not a retry suggestion. Inspect PBM operation metadata/logs/storage and the persisted Karakoram Operation before any recovery action. Do not erase receipts, change the request ID, switch command nodes, or start a second restore to evade ambiguity. Keep receipts and controls with retained volumes across recovery. Private agent commands are for the controller's bounded protocol; there is no public arbitrary exec endpoint.

## Routine operating checks

- Check database/agent health, lag, disk headroom and current Zerops allocations; desired resource bounds are not instantaneous observed allocation.
- Check successful backups, S3 verification/quota and **observed continuous** PITR windows. Run periodic data recovery drills, not just backup creation checks.
- Review recent failed Operations/events and whether desired generations have converged.
- Inspect placement warnings without calling them HA certification.
- Preserve control-plane state/master key and monitor expiry of user-supplied TLS/OIDC/API credentials.
- Drain disruptive work before [Karakoram upgrades](upgrade.md). Existing member agents receive the current binary on a later provider-driven member deployment, not on control-plane restart alone.

## Orphaned platform volumes

If the platform refuses to delete a Local Storage volume, Karakoram first confirms that its member runtime was deleted and verifies volume ownership. Repeated mount-related deletion refusals are handed off to the orphan inventory after the retry threshold. Karakoram emits the Warning event **`VolumeOrphaned`** for the original cluster, allowing member removal or cluster deletion to finish while background deletion retries continue. This does not mean the volume is gone: it continues consuming disk and may incur charges until deletion succeeds.

Inspect the read-only inventory with **`GET /api/v1/orphaned-volumes`**, which returns `{items: [...]}`, or:

```sh
karactl orphaned-volumes list
karactl orphaned-volumes list -o json
```

The Web UI shows **Settings > Orphaned volumes** and a warning banner linking to that inventory. Each record identifies the volume hostname, Zerops service ID, original cluster/member, failure history, attempt count, latest delete Process ID and retry timestamps. Visibility requires read permission on the record's cluster; administrators see all records, including those whose clusters have been deleted.

The in-process janitor runs while the control plane is running and keeps retrying recorded volumes, starting after **two minutes**, doubling the retry delay up to **one hour**. Failed attempts emit **`OrphanedVolumeDeleteFailed`** (Warning); when Zerops accepts deletion, the record is removed and **`OrphanedVolumeDeleted`** (Normal) is emitted. These events retain the original cluster name. Before deleting, the janitor checks that the hostname still belongs to the recorded service ID. If the hostname is absent or belongs to a different service ID, it drops the record without deleting anything. Karakoram never deletes services it did not record.

For manual cleanup later, delete the volume in the **Zerops GUI** or run **`zcli service delete <hostname>`** once Zerops accepts deletion. If the refusal persists, report the recorded **service ID** (`serviceId`) and **failing delete Process ID** (`lastProcessId`) to Zerops support. When the hostname is gone, the janitor drops the record on its next attempt; no manual Karakoram state edit is needed.

**Manual cleanup is an external prerequisite.** Karakoram's orphaned-volume
API/CLI and Settings page only list diagnostic records; they have no manual
force-delete action. The automatic janitor submits ordinary Zerops deletion
requests with service-ID ownership checks, but cannot override a platform mount
or process refusal. An operator needs separate Zerops project permissions (and,
for the CLI route, an authenticated zcli) to resolve that refusal or remove the
volume. This separation prevents an inventory action from bypassing platform
data-safety checks. Use the original namespace's project, not necessarily the
project hosting Karakoram.

Re-creating a cluster or member with the same name skips the orphaned hostname and allocates a new member index. Allocation also skips runtime/volume hostnames already held by existing Zerops services rather than reusing or deleting them. See [resource naming](adr/0009-resource-naming.md) and the [platform limitation](limitations.md#control-plane-and-platform-operations).

## Control-plane state backup and recovery

State includes specifications/status/internal ownership, operations/checkpoints, events, accounts/sessions/API-token hashes, policy/settings, metric history and encrypted cluster/storage/PKI credentials. It is SQLite WAL at `KARAKORAM_DATA_DIR/karakoram.db`, normally `/mnt/karakoramdata/karakoram.db`.

A state copy without its original **`KARAKORAM_MASTER_KEY`** cannot recover encrypted secrets. Store the key separately in a restricted secret manager, along with project identity and a replaceable project-scoped Zerops token. Do not generate a new master key for an existing state database. There is no `admin backup-db`, managed backup schedule or master-key re-encryption CLI.

### Consistent backup

Use the [complete stopped-writer procedure](upgrade.md#consistent-pre-upgrade-state-backup):

1. Let disruptive operations settle, then stop the single control-plane runtime and wait for STOPPED.
2. In the Zerops Local Storage maintenance container, the mounted volume appears as `/data`. Archive the whole state directory into a file **outside** it, with restrictive permissions, while no writer is running. Include any remaining WAL/SHM sidecars and persisted identity material.
3. Download to encrypted off-platform storage, recording snapshot time, Karakoram revision/version and the corresponding master-key identity. Remove disposable copies when safely transferred.
4. Start the old server again or proceed with the intended upgrade.

Never copy only a live `.db`, assume a platform live volume tarball is transactionally consistent, or run two servers against the same volume. A native SQLite backup API/tool can make a consistent live snapshot if separately implemented correctly; the runtime is not guaranteed to include the sqlite3 executable. Control-plane state archives do not contain MongoDB member data; PBM must protect that separately.

### Restore a state snapshot

Fence/stop every old control-plane process. Preserve a protected copy of the failed/current state for diagnosis. Restore the complete consistent archive into an emptied state directory on a retained/replacement Local Storage volume, without merging stale WAL/SHM files. Restore ownership to `zerops` and restrictive permissions. Supply the **original** master key, correct project identity, public origin and a valid project-scoped integration token. Deploy one schema-compatible Karakoram binary with singleton/temporaryShutdown settings.

Automatic migrations are forward-only; an older binary is not a schema rollback tool. State rollback rewinds desired specs, checkpoints, accounts/tokens and inventory but **does not rewind** Zerops services or database data. Observe services, persisted Process IDs, members and PBM state before allowing restored checkpoints to drive disruptive work. A stale snapshot is not permission to replay an unknown restore initiation. Review [upgrade rollback](upgrade.md#rollback).

### Lost control plane

- **Runtime lost, state volume intact:** fence the old runtime, recreate/redeploy one control-plane runtime mounted to the retained state volume with its original secrets/hostname/origins. Do not rerun a fresh installer that replaces identities/master key. Existing members normally keep running from cache.
- **State volume lost, external snapshot available:** restore the snapshot and master key as above, then reconcile differences since the snapshot. Restore the same private advertised endpoint/trust identity that agents use; do not casually rename the control plane or regenerate its internal CA.
- **State and original key/snapshot unavailable:** there is no supported automatic adoption/import-existing-cluster command. Running members may continue using cached config, but a blank control plane cannot infer their secret ownership/PKI/checkpoints. Recover the original state/key or perform an operator-designed database recovery from usable PBM data/credentials into a new installation. Do not create name-colliding clusters and assume it adopts existing services.

Control-plane RPO is your consistent snapshot interval; RTO includes replacement, secret restoration and external-state review. Scheduling/aggregation are interrupted while it is unavailable; configured PBM agents/PITR and normal replication are not inherently dependent on the control-plane process.

## Recovering local admin access

### Zerops-native startup recovery (recommended)

No container exec is needed. Keep the existing state volume and **original `KARAKORAM_MASTER_KEY`**, and keep the control plane a singleton (`maxContainers: 1`).

1. In **karakoram → Environment variables → Secret variables**, set **`KARAKORAM_RECOVERY_PASSWORD`** to a new strong, unique temporary password (at least **12 Unicode characters**, at most **1024 bytes**). Set optional `KARAKORAM_RECOVERY_ACCOUNT` to the local account name; it defaults to **`admin`**. Use Zerops service secret variables, not source/pipeline runtime variables, shared project variables or CLI password arguments. Authorized platform automation can instead update the service's secret user data through `PUT /service-stack/{id}/user-data`; preserve unrelated entries and follow Zerops's current secret-variable schema. Never print request bodies or tokens.
2. **Restart the Karakoram service** so its process receives the updated environment. Before either HTTP listener starts, Karakoram resets the local account, re-enables it, requires a password change, and removes all its sessions/refresh families/API tokens. A missing account is created as a local administrator. Recovery restores `role:admin`'s unrestricted grant and this account's admin binding, clears their conflicting denies/outgoing role inheritance, and replaces this account's prior roles with `role:admin`; unrelated policy is retained. Review customized admin restrictions after recovery. An existing OIDC identity is not converted: choose a different, local recovery account.
3. Check the secret-free **“Local administrator access recovered; password change required”** startup log and durable `AdminAccessRecovered` event. Sign in through the normal HTTPS UI with the temporary password, complete the mandatory password change, and sign in again with the new password. Reissue automation API tokens deliberately; the old ones remain revoked.
4. **Delete `KARAKORAM_RECOVERY_PASSWORD`** (and the optional account variable) from the service's secret variables, then **restart** again to remove it from the running process environment. Store only your final password in a password manager.

The state database retains a non-reversible HMAC-SHA-256 receipt, keyed by a domain-separated key derived from the master key, over account and recovery password. Restarts with the same account/value do **not** reset again, including after the mandatory password change or after removing/re-adding the variable. A different recovery password applies again. The receipt is committed only after credentials, revocation, policy and audit persistence succeed; interrupted application is retried on the next startup before serving traffic. Invalid passwords/accounts abort startup without changing account state. A state-snapshot rollback also rewinds receipts: remove stale recovery variables before restoring state unless you intentionally want to apply recovery again.

### Offline alternative

`karakoram admin reset-password ACCOUNT` remains an **offline local-account** recovery alternative, not a remote Zerops exec API:

1. Stop the server and ensure no other process writes its state.
2. Run the matching Karakoram executable in a trusted environment with the existing state mounted and **the original master key securely supplied**. For the Local Storage maintenance container's `/data` mount, a matching Linux binary uploaded privately can be invoked as:

   ```sh
   KARAKORAM_DATA_DIR=/data /tmp/karakoram admin reset-password admin
   ```

   The executable path assumes you uploaded it there; the maintenance container does not ship Karakoram automatically. The command securely prompts on a terminal, or reads `KARAKORAM_RESET_PASSWORD` if supplied through a protected environment. Do not pass passwords as CLI arguments, print environment values or save the key/password into shared shell history.
3. Start one server again, sign in with the reset password, and complete the mandatory password change. Reset revokes sessions/tokens for the account; reconnect/reissue automation credentials deliberately.

The offline command needs the existing DB and master key, not a fresh bootstrap. Unlike startup recovery, it does not repair deleted identities, restore RBAC bindings or change an OIDC identity-provider password. Keep a complete policy backup; the offline password reset alone cannot restore administrator authorization removed by a policy edit.

## Troubleshooting by symptom

| Symptom | Inspect | Safe next action |
| --- | --- | --- |
| Member stuck importing/deploying | Cluster events/internal pending Process, Zerops service/process error and preparation/readiness logs; Local Storage ≥5 GB, name collisions, package/artifact availability | Wait for actual terminal Process and agent health. READY_TO_DEPLOY means no successful artifact deployment; import HTTP success is not cluster readiness. Provisioning imports services, then deploys the executable with the generated pipeline. Ensure readiness uses an absolute `/var/www/karakoram agent readiness` path. Do not delete retained data to reset a pipeline. |
| Agent cannot pull config | Runtime logs for waiting-first-config vs using-cache, private DNS/origin 9443, pinned CA validity, member token/state identity and volume permissions | Restore the original controller endpoint/state/CA/token environment; environment-secret updates require process restart. Do not disable verification or expose 9443 publicly. Cached data-bearing members can continue, uncached/new/router deployments cannot. |
| PBM agent not registered | PBM process tail, backup storage/config, correct backup roles/TLS/local URI and no duplicate agent | PBM uses local `127.0.0.1` **without directConnection=true**, even on secondaries, so it can discover the primary. Maintain one agent per mongod, none on mongos, same OS user as mongod. Process-running metric alone is not registration proof. |
| Physical restore stuck/offline | Restore/Operation step, PBM describe-restore through stored S3 config, all member PBM logs, durable maintenance/stop holds and receipt ambiguity | Offline mongod/successful PBM exit can be expected. Keep writers stopped; do not auto-restart processes while PBM owns dbpath. Follow terminal restore progress and provider recovery steps; resync is nonblocking and observed, not an unbounded `--wait`. Ambiguous initiation needs observation before any new request. |
| Placement warning | Best-effort groups/unknown fingerprints and whether multiple voters share a group | Treat as risk information. CPU mode/service count does not prove separation; do not destroy/recreate in a loop hoping for affinity. Maintain backups and decide whether the platform meets your availability needs. |
| Binary upgrade stalled | Agent observed binary/lag/quorum, old-major FCV, pending deployment and balancer state | Repair health/FCV prerequisites; do not upgrade remaining members manually in parallel. FCV promotion remains separate. |
| Public seed connects but normal RS driver fails | Discovered private `hello` hosts, per-member public ports, client DNS/routing and TLS SANs | Routing is not a split-horizon solution. Use private access or a designed external discovery setup; directConnection is diagnostic, not automatic failover. |
| Backup/PITR missing | Backup errors, base success, primary destination/prefix, S3 quota/access and continuous PBM ranges | Verify storage and fresh base, then observe new coverage. Count retention is not a promised time window. Do not fabricate a PITR interval from wall-clock elapsed time. |
| API 403 or UI controls absent | Current account state, effective RBAC, forced password change, CSRF for cookie writes | Use permission checks and reauthenticate after password change; reveal needs cluster-credentials read separately. |
| Root `/metrics` denied/stale | Monitoring permissions on `<namespace>/<cluster>`, enabled targets, namespace relay and latest scrape success | Grant the intended scope and repair scraper connectivity. Cached sample presence is not live heartbeat; use status/timestamps/external checks. |
| Events watch/UI live updates arrive in batches, but ordinary requests work | Compare durable `/events` and operation/status reads; inspect SSE proxy buffering and padded heartbeat delivery | Approximately 16-KiB random-base64 initial/heartbeat comments reduce buffering; small events may still wait for the fifteen-second heartbeat. Account for ~1.1 KiB/s padding per stream plus startup. Keep no-cache/no-transform/X-Accel-Buffering:no and configure a nonbuffering proxy path where possible. Preserve cursors and refresh/read durable state rather than resubmitting work. |

For destructive repairs or ambiguous restore recovery, preserve redacted evidence and involve a maintainer/operator who can inspect the original state and PBM metadata. Never silently mark an uncertain operation successful or remove safety files to suppress its error.
