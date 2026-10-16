<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Operations and troubleshooting

Operate the control plane, managed engines and Zerops infrastructure as separate systems; each has its own health and recovery signals.

## Health and logs

```sh
curl --fail --show-error https://karakoram.example.com/healthz
karactl health
karactl clusters status demo
karactl clusters members demo
karactl operations cluster demo
karactl events list --cluster demo
```

`/healthz` checks the control-plane store, not engine health. Inspect cluster conditions, observed generation, member health, agent connectivity, allocations, disk headroom and backup status. Zerops ACTIVE alone is insufficient. Operation success is not a health guarantee; see [operations and status](clusters.md#operations-and-status).

Use two log sources:

- **Zerops deployment/application logs:** preparation, readiness, startup, supervisor diagnostics and forwarded child output, including failures before the agent is reachable. Signed project-log URLs/tokens are credentials.
- **Karakoram member logs:** authenticated, bounded process tails, not a historical archive:

  ```sh
  karactl clusters logs demo <member> --process <process> --lines 100
  ```

  `GET /api/v1/clusters/{cluster}/members/{member}/logs` accepts `process` and `lines` (default 100; accepted range 1–10000). The default process is the provider's first declared process. Each process retains at most **1000 in-memory lines**, lost on agent restart. **`cursor` and `since` are currently ignored; no `nextCursor` is returned.** Requesting more lines cannot recover older history.

Process names, readiness checks and engine diagnostics: [MongoDB](mongodb/operations.md), [RabbitMQ](rabbitmq/operations.md), [Elasticsearch](elasticsearch/operations.md).

Platform failures expose a Zerops error code and sanitized field/reason summary in events and conditions. Inspect the named resource bound or missing service before changing the spec; rejected resource updates/imports have [resource diagnostics](clusters.md#resources-and-autoscaling). For `serviceStackNotFound`, check whether the recorded service was removed externally; do not delete unrelated services. Use the Zerops dashboard when details are missing/truncated.

Redact passwords, credential-bearing URIs, environments, tokens, signed URLs and private keys before sharing logs. Preserve identifiers and timestamps instead.

## Reviewing audit activity

Events and Operations identify initiating actors, including persisted account names, OIDC issuer suffixes and API-token owner/name/ID. Use the complete actor value when filtering.

```sh
karactl -n dev events list --cluster demo
karactl -n dev operations cluster demo
# Installation audit access requires audit:read on '*'.
karactl events list --actor admin --resource accounts
karactl events list --resource auth -o json
karactl -n dev events list --actor alice --resource namespaces
```

Actor/resource filters are exact, case-sensitive AND matches intersected with the namespace. Preserve filters with `--cursor`; `events watch` supports them too. Audit scope, coverage gaps, indefinite retention and durability limits are defined in [Security](security.md#audit-attribution-and-durability). Check resource/operation state before retrying an action with missing audit evidence. Transport/replay: [API events](api.md#pagination-and-events).

For `reason: ClusterSpecChangeRequested`, the message summarizes up to the first three requested changes, for example `Cluster spec changed for demo (generation 3): components.engine.resources.maxCpu: 1 → 2`, followed by `and N more` for additional changes. A no-op reports `Cluster spec change requested for demo: no changes`.

1. With [structured audit access](access-control.md#policy-format), expand **Changed fields** in either the global or cluster Events view to compare **Before** and **After**. Missing fields normally display `(unset)`, distinct from JSON `null`. **Truncated** rows display omitted values as `(omitted)`; oversized values may display `(N bytes)`. A comparison-level warning means values or additional paths may be missing. **Sensitive** rows show only [redaction markers](security.md#audit-attribution-and-durability).
2. Inspect the structured comparison with `karactl -n <ns> events list --cluster <c> -o json`. See [API events](api.md#pagination-and-events) for paths, schema and limits.
3. Check cluster status and **Operations** for convergence; a requested spec change is not completion.

## Node-agent semantics

Agents pin the internal CA, authenticate with a per-member token and pull only their assigned configuration. They apply usable cached configuration at startup, retry first configuration every **five seconds**, and normally refresh every **60 seconds**. Cached members can keep running during control-plane unavailability; uncached startup cannot. Stateless caches do not survive runtime replacement.

Accepted configuration and maintenance/stopped-process holds are atomically persisted as owner-only **0600** files. Configuration contains locally required secrets. The supervisor owns child processes, restart backoff and SIGTERM forwarding; do not start competing supervisors. Maintenance holds suppress automatic restarts. Explicit stop holds survive config pulls and agent restarts until explicitly released by the controller. Do not edit safety files to force recovery. Engine-specific files and recovery holds belong in the engine operations guides above; private agent commands are not arbitrary remote exec.

## Routine operating checks

- Review health, current allocations/disk headroom, failed Operations/events and generation convergence.
- Verify successful backups and storage capacity/access; rehearse data recovery, not just backup creation. See [backups](backups.md) and engine recovery guides.
- Preserve consistent state backups and the original master key; monitor credential/certificate expiry.
- Drain disruptive work before [upgrading Karakoram](upgrade.md). Existing member executables roll out on later provider-driven deployments, not control-plane restart alone.
- For project-token, relay or cross-namespace incidents, use [namespace operations](namespaces.md) and [relay health](namespaces.md#relay-health) before restarting members.

## Orphaned platform volumes

After runtime deletion and ownership verification, repeated mount-related volume-deletion refusals move the volume to orphan inventory. `VolumeOrphaned` allows member/cluster removal to finish, **not** storage deletion: disk usage and charges can continue.

```sh
karactl orphaned-volumes list
karactl orphaned-volumes list -o json
```

`GET /api/v1/orphaned-volumes` returns `{items: [...]}`. **Settings → Orphaned volumes** shows the same inventory: hostname, service ID, original cluster/member, failures, attempts, latest delete Process ID and retry timestamps. Cluster-read permission controls visibility; administrators also see records for deleted clusters.

The control-plane janitor retries after **two minutes**, doubling delays to **one hour**. `OrphanedVolumeDeleteFailed` warns on failure; accepted deletion removes the record and emits `OrphanedVolumeDeleted`. Events retain the original cluster. Before deletion, it checks the recorded service ID; absent/reassigned hostnames drop the record without deleting another service.

The inventory has **no force-delete action**. With separate Zerops project permissions, delete in the original namespace's Zerops GUI or use authenticated `zcli service delete <hostname>`. Persistent refusals need Zerops support: provide `serviceId` and failing `lastProcessId`, not secrets. The janitor drops manually removed volumes on its next attempt; do not edit state.

Recreated clusters/members skip orphaned and already occupied runtime/volume hostnames, allocating a new index. See [resource naming](adr/0009-resource-naming.md).

## Control-plane state backup and recovery

SQLite WAL state lives at `KARAKORAM_DATA_DIR/karakoram.db` (normally `/mnt/karakoramdata/karakoram.db`). It includes desired/observed state, ownership, checkpoints, events, identities/sessions/token hashes, policy/settings, metric history and encrypted credentials/PKI.

**Warning:** a state backup without its original `KARAKORAM_MASTER_KEY` cannot recover encrypted secrets. Preserve the key separately in a restricted secret manager, plus project identity and a replaceable integration token. Never replace the key on existing state. There is no `admin backup-db`, managed state-backup schedule or master-key re-encryption CLI.

### Consistent backup

1. Let disruptive work settle. Stop the single control-plane runtime and wait for STOPPED.
2. In the Local Storage maintenance container, the volume is mounted at `/data`. Archive the **whole state directory**, including remaining WAL/SHM sidecars and persisted identity material, to a restrictively permissioned file **outside** that directory while no writer runs.
3. Transfer to encrypted off-platform storage. Record snapshot time, Karakoram version/revision and master-key identity; remove disposable copies after transfer.
4. Restart one control plane or proceed with the [upgrade](upgrade.md).

Never copy only a live `.db`, assume a live platform volume archive is consistent, or run two servers against one volume. A correctly used native SQLite backup tool can take a consistent live snapshot, but `sqlite3` is not guaranteed in the runtime. State archives exclude engine data; use engine-native backups separately.

### Restore a state snapshot

Fence every old control-plane process and preserve the failed/current state securely. Restore the complete archive into an emptied state directory on retained/replacement storage, without merging stale WAL/SHM files. Restore `zerops` ownership and restrictive permissions. Supply the original key, correct project identity/origins and valid integration token. Deploy one schema-compatible binary with singleton/temporaryShutdown settings.

Migrations are forward-only; an older binary does not roll back schema. State rollback rewinds specs, checkpoints, accounts/tokens and inventory, **not** external services or engine data. Inspect services, persisted Process IDs, members and engine recovery state before resuming disruptive checkpoints. Never replay an unknown restore initiation just because state is stale.

### Lost control plane

| Loss | Recovery |
| --- | --- |
| Runtime; state volume intact | Fence the old runtime; redeploy one runtime on retained state with original secrets, hostname and origins. Do not rerun fresh installation with replacement identities. |
| State volume; snapshot/key available | Restore as above; reconcile external changes since the snapshot. Preserve the private advertised endpoint and agent trust identity/internal CA. |
| State/key/snapshot unavailable | No automatic adoption/import-existing-cluster command exists. Recover original state/key or design engine-native data recovery into a new installation. Name collisions do not adopt services. |

RPO is the consistent snapshot interval. RTO includes replacement, secrets and external-state review. Control-plane downtime pauses scheduling/aggregation; it does not inherently stop existing engine processes or replication.

## Recovering local admin access

### Startup recovery

Keep the state volume, original master key and singleton (`maxContainers: 1`).

1. In **karakoram → Environment variables → Secret variables**, set `KARAKORAM_RECOVERY_PASSWORD` to a strong temporary value: **12 Unicode characters minimum, 1024 bytes maximum**. Optional `KARAKORAM_RECOVERY_ACCOUNT` defaults to `admin`. Use service secrets, not shared project/pipeline variables or CLI password arguments. Authorized platform automation can use `PUT /service-stack/{id}/user-data`: preserve unrelated entries, follow the current secret-variable schema and never log bodies/tokens.
2. Restart. Before listeners start, recovery resets/re-enables the local account, requires password change and revokes its sessions, refresh families and API tokens. A missing account becomes a local administrator. Recovery restores unrestricted `role:admin`, repairs this account's binding, clears conflicting denies/outgoing inheritance and replaces its previous roles; unrelated policy remains. It does not convert OIDC accounts: choose a local name. Review customized admin restrictions afterward.
3. Check the secret-free **“Local administrator access recovered; password change required”** log and `AdminAccessRecovered` event. Sign in over HTTPS, change the password, then sign in again. Reissue automation tokens deliberately.
4. Remove recovery variables and restart again to clear the process environment. Store only the final password.

A non-reversible receipt makes the same account/password apply only once, even if removed/re-added. A different password applies again. Interrupted recovery retries before serving; invalid inputs abort startup without account changes. Snapshot rollback rewinds receipts: remove stale recovery variables first unless reapplication is intended.

### Offline alternative

Stop every state writer. In a trusted maintenance environment, mount existing state and securely supply the original master key. Upload the matching executable privately; the maintenance container does not include it:

```sh
KARAKORAM_DATA_DIR=/data /tmp/karakoram admin reset-password admin
```

Use its terminal prompt or protected `KARAKORAM_RESET_PASSWORD` environment, never password arguments/history. Restart one server and complete mandatory password change. Sessions/tokens are revoked. Unlike startup recovery, this command does not recreate identities, repair RBAC or change OIDC passwords.

## Troubleshooting by symptom

| Symptom | Likely cause | Action |
| --- | --- | --- |
| READY_TO_DEPLOY | No executable deployed | Push/deploy the artifact; import acceptance is not readiness. Preserve retained data. |
| Startup/readiness fails | Token/project access, invalid key, origin or missing mount | Inspect deployment/application logs; check project access, master key ≥32 characters, URL and state mount without printing secrets. |
| `userDataZeropsPrefixForbidden` | Reserved secret prefix | Use documented `KARAKORAM_ZEROPS_*` names. |
| Edited secret not visible | Old process environment | Restart or redeploy; check YAML environment overrides. |
| Subdomain missing after failed deploy | Public access not enabled | After successful HTTP-port deployment, enable Subdomain access in Zerops. |
| Zerops token unauthorized | Wrong project/scope or expired token | Check integration token and [security](security.md); replacing a Karakoram API token cannot repair it. |
| Agent cannot pull config | Routing, pinned CA, identity or permissions | Restore original endpoint/state/CA/token environment; restart after secret changes. Never expose 9443 or disable verification. See [relay health](namespaces.md#relay-health). |
| Ownership conflict or expired platform Process | Incomplete legacy provisioning | Follow [upgrade recovery](upgrade.md); do not delete unrelated services. |
| API 403 or missing UI controls | Account/RBAC, required password change or cookie CSRF | Reauthenticate and check effective permissions; credential reveal has a separate grant. |
| Metrics denied/stale or charts empty | Authorization, disabled collection or failed scrape | Use [monitoring troubleshooting](monitoring.md). |
| Live events arrive in batches | Proxy buffering | Preserve cursors; read durable status/events before retrying work. See [event transport](api.md#pagination-and-events). |
| Engine health/recovery/placement warning | Provider-specific condition or platform constraint | Use the engine operations guides and [limitations](limitations.md); preserve evidence rather than clearing safety holds. |
