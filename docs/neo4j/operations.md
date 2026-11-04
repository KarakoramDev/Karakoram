<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Neo4j operations

[Overview](README.md) · [Access and TLS](access-and-tls.md) · [Backups](backups.md)

## Availability and lifecycle

Community is a singleton: there is no quorum, failover, rolling availability or read replica. Restart, TLS rotation, memory/settings changes, version upgrades and offline maintenance interrupt all queries. Arrange application downtime and reconnect behavior; operation success alone is not proof of application correctness.

```sh
karactl -n NS clusters status graphdb -o json
karactl -n NS clusters members graphdb -o json
karactl -n NS clusters restart graphdb --wait
karactl -n NS clusters rotate-tls graphdb --wait
karactl -n NS clusters logs graphdb graphdbn0 --process neo4j
```

The supervised process is `neo4j`, the server JVM derived from the pinned distribution's `neo4j console --dry-run`. Data, transaction logs, configuration, certificates, logs and run files are below the member volume's `karakoram/neo4j` directory. Do not run a second server or offline administration command against its live store.

At first boot, before normal network listeners start, the member initializes the managed administrator via a temporary authentication-disabled server bound only to `127.0.0.1`, Bolt 17687 and HTTPS 17473. It sets the password through a parameterized query, stops that child and starts normally. The same recovery mechanism resets the target administrator after restore. Passwords are not passed as `neo4j-admin set-initial-password` command-line arguments.

Bootstrap completion is recorded in `credentials-initialized.json` only after
the managed password is established, `SHOW USERS` confirms that `neo4j` exists
with `passwordChangeRequired=false`, and the recovery child has stopped. If the
password already matches, recovery independently clears the password-change
requirement instead of treating password equality alone as completion. It never
needs the previous/default password or uses `ALTER CURRENT USER`.

Interrupted first creation is replayed against the existing store; do not delete
the system database, seed `auth.ini`, remove markers or start a second JVM.
`applied.json` and the applied configuration version are published later, after
the normal start action succeeds. A reachable agent with configuration version
zero is not a successfully configured database.

| Interruption point | Retry behavior |
| --- | --- |
| Before recovery starts, during store initialization, or before the password transaction | Missing completion marker starts isolated recovery again; native store damage/startup failures remain errors. |
| Password transaction committed, but response lost or agent/container killed before the marker | Recovery sees the existing password, clears the change-required flag independently and verifies the user before recording completion. |
| Clearing the flag or reading its postcondition fails | No completion marker is written. Preserve the categorized diagnostic; another apply safely repeats recovery. |
| Recovery child stop fails or times out | No completion marker is written; the temporary child is force-killed and the next apply performs store recovery again. |
| Marker published, before normal process start or applied-state publication | Existing marker plus system store skips credential mutation; the supervisor starts normally and apply publishes its state. |
| After successful apply, before the control plane observes it | The control plane observes matching readiness, configuration version and restart token; replay does not require a new credential. |

Control-plane retries and periodic agent configuration pulls use the same path.
Request cancellation can interrupt recovery; it cannot certify completion.
Persistent transport/TLS, resource, store or user-metadata failures still require
operator repair: inspect the closed diagnostic category and process logs, retain
the volume, and correct the underlying cause. A successful `SHOW USERS` readiness
probe does not prove that a subsequent password mutation succeeded. Missing or
malformed administrator metadata and a remaining password-change requirement
produce explicit verification errors rather than a completion marker.

No protocol fields or endpoints change. Both control plane and member should be
upgraded for current diagnostics and request budgets; upgrading only the control
plane does not repair an older member's bootstrap implementation.

The server uses SIGTERM and declares a **45-second graceful stop budget** for checkpointing during agent shutdown, bounded by the remaining overall shutdown deadline. A timeout is an error, not proof of a stopped JVM; the supervisor does not turn it into successful shutdown or immediately SIGKILL that child. The infrastructure may subsequently terminate the runtime according to its own grace period. The temporary credential-recovery child has a separate 45-second stop wait and is force-killed if it cannot exit. Check runtime/platform stop grace and allow crash recovery after forced termination.

### Legacy bootstrap marker with expired credentials

A v0.4.3 member could record `credentials-initialized.json` after an
unchanged-password rejection even though `neo4j.passwordChangeRequired` remained
true. Upgrading the member prevents new false completions but does **not** validate
or invalidate an existing marker. With both that marker and the system store
present, `BeforeStart` skips recovery. The normal server can therefore run while
authenticated readiness and other managed queries fail with
`Neo.ClientError.Security.CredentialsExpired`; this is not necessarily an
incorrect password. Repeated reloads or restarts alone do not repair it.

Automatic repair is not performed: readiness and metrics are observations outside
the node action lock, and a failed observation cannot authorize a singleton
outage or release a restore/backup/recovery job's stop hold. Moving the mutation
into apply would also require durable attempt/admission state to prevent repeated
stop/recovery/start cycles on persistent failure. Ordinary system-user rotation
does not enter offline repair for `CredentialsExpired`: its recovery admission
requires explicit credential rejection, not the change-required signal.

For this specific legacy-marker condition, arrange a maintenance outage and
support-assisted access to the member's authenticated private agent API and
volume (there is no public CLI bootstrap-repair command):

1. Upgrade the member agent to the fixed version. Preserve the volume and sealed
   managed password; retain a copy of the marker and relevant redacted diagnostics.
   Confirm that no backup, restore, administrator recovery or other lifecycle
   operation is active or recovery-pending. Resolve any such operation first;
   never release its hold or alter its receipts.
   Prevent new scheduled backups and lifecycle submissions for the duration of
   this procedure; an operator stop hold alone does not exclude offline jobs.
2. Explicitly stop `neo4j` through the agent process API
   (`POST /v1/processes/neo4j/stop`). This persists the operator stop hold.
   Confirm successful stop and absence of a server/recovery JVM before touching
   the volume. A timeout or a merely unreachable connector is not proof of stop.
3. Only after confirming this legacy condition and the stopped state, remove
   `karakoram/neo4j/credentials-initialized.json` from the member volume. Do not
   remove the system database, `applied.json`, controls, job receipts or any store
   directory, and do not seed `auth.ini` or launch a separate JVM.
4. Explicitly start through the agent process API
   (`POST /v1/processes/neo4j/start`), releasing only the operator hold just taken.
   The upgraded agent replays the bounded loopback-only recovery against the
   existing store, clears and verifies the change-required flag, then records a
   new completion marker before normal launch. Allow the engine-action request
   budget for recovery rather than an ordinary short request timeout.
5. Confirm authenticated readiness, matching configuration/restart token and
   application queries. If recovery fails, stop retrying starts, retain the
   volume and inspect the categorized diagnostic; missing metadata, TLS, store
   or resource failures require their own repair, not repeated marker deletion.

This narrowly scoped stopped-member procedure is an exception to the general
instruction not to remove bootstrap markers; it is not a lost-password recovery
shortcut or a remedy for arbitrary readiness failures.

## Upgrades

The supported transition is **2026.09.0 → 2026.10.0**, requested by changing `spec.version`. It requires a fresh successful backup and restarts on the new pinned artifacts; the store and system database upgrade in place. Plan an outage, verify the dump and recovery destination first, and do not combine this with an assumed rollback-by-downgrade strategy. Downgrades and other version paths are rejected. See [shared upgrade admission](../clusters.md) and [offline dumps](backups.md).

### Agent upgrades

```sh
karactl -n NS clusters upgrade-agents graphdb --wait
```

Agent upgrades redeploy the single member with its **unchanged applied Neo4j
version, runtime, persistent volume and resources**. This interrupts all database
access: arrange application downtime and reconnection. A maintenance window is
required before deployment unless explicitly overridden.

The operation freezes the control-plane agent build and records its member and
deployment progress before disruption. It skips an already-current healthy agent
unless `--include-current` is supplied. Completion requires authenticated native
readiness, the applied version/configuration and the exact frozen agent build.
Pending structural or offline maintenance work blocks admission. Cancellation or
a closing window does not abandon an already-submitted deployment; cancellation
is not rollback. If the control-plane build changes before deployment, restore
the intended build or re-run the operation as directed by its diagnostic.
If deployment intent exists but its platform receipt is missing, cancellation
first waits for native readiness on the previously observed or target agent
build; missing receipt does not prove that the platform rejected deployment.
Replay rechecks the maintenance window and native readiness before resubmitting.
When the sole expected runtime is native-ready on the exact frozen target and
that build differs from its recorded pre-upgrade build, replay adopts completion
without redeploying, even if the control-plane build or maintenance window has
changed. With `--include-current`, a member already on that target has no build
transition to prove deployment. The backend contract cannot recover a deployment
receipt by member identity, so receipt loss in that case can cause one additional
same-build redeployment and outage, subject to the window and native safety gates.


## Monitoring

Community has no native [Prometheus](https://prometheus.io/) metrics endpoint. The member gathers authenticated Query API observations (including the JVM memory bean), plus filesystem store size; generic process-tree CPU/RAM and volume metrics remain available through [Karakoram monitoring](../monitoring.md).

| Metric | Meaning |
| --- | --- |
| `neo4j_up` | Authenticated metric-query collection succeeded (1/0) |
| `neo4j_heap_used_bytes` | JVM heap used |
| `neo4j_heap_committed_bytes` | JVM heap committed |
| `neo4j_heap_max_bytes` | JVM heap maximum |
| `neo4j_heap_configured_bytes` | Configured heap |
| `neo4j_pagecache_configured_bytes` | Configured page cache, not observed usage/hit rate |
| `neo4j_nodes` | Total node count |
| `neo4j_relationships` | Total relationship count |
| `neo4j_transactions_active` | Active transactions, including the scrape transaction |
| `neo4j_store_size_bytes` | Files below database directories; not total volume usage |

The UI's [Neo4j](https://neo4j.com/) monitoring panels show Availability, JVM heap, Configured page cache, Graph size, Active transactions and Store size, alongside shared resource charts.

These are gauges. If a collection step fails, the member returns only `neo4j_up=0`, not healthy zero values for the remaining series. Offline maintenance also reports unavailable. Counts and store observations are monitoring work, not an application consistency or backup-coverage check; monitor application queries, free disk, operations and backup outcomes separately.

## Troubleshooting

- **`CredentialMismatch` / `AdminAuthenticationRejected`:** the managed password was rejected by Neo4j. Do not repeatedly reveal credentials into logs or overwrite sealed secrets. Ordinary system-user rotation requires either the current or already-applied next password to authenticate; it is not a lost-password bypass. Explicit loopback credential recovery is required. Preserve the volume and inspect the condition/operation before attempting repair; there is no documented public CLI recovery command.
- **`ConfigurationApplyFailed` / not Ready:** inspect the operation, member status and agent logs as well as `neo4j` logs. This condition means an apply step failed (including command derivation, startup or credential recovery); it does not establish that Neo4j rejected a setting. Only [allowlisted settings](README.md#memory-and-options) are accepted, but accepted strings still must satisfy Neo4j's strict validation. Readiness requires authenticated queries, matching desired version/configuration and applied restart token; a running JVM or reachable agent is insufficient. Synchronous reload uses the engine-action request budget (three minutes by default), rather than the ordinary 30-second agent-request budget; caller deadlines and individual startup/recovery limits still apply. The agent retries configuration pulls every 60 seconds, and the controller retries reconciliation, so a reload timeout alone does not establish permanent failure.
- **`Neo4j credential recovery failed`:** agent logs distinguish `category=native` (an allowlisted Neo4j status code and closed message class) from `category=request` (transport, timeout, cancellation, HTTP rejection, authentication or invalid response). Unknown native codes are reported as `unrecognized`; raw native messages, query text, passwords and transport URLs are never included. `class=not-leader`, `database-unavailable` and `password-change-required` are diagnostics, not successful initialization. The exact supported unchanged-password rejection establishes password equality only; recovery separately clears and verifies the change-required flag. Other failures remain errors and initialization is retried without writing its completion marker. Preserve the categorized failure before changing the runtime or volume.
- **TLS connection failure:** trust the cluster CA in the client, use a certificate-covered endpoint and verify the scheme. Public routing discovery can return private hostnames; use a verified direct `bolt+s` connection when appropriate. Do not disable certificate verification to conceal a trust error.
- **Interrupted backup:** recovery restarts the server and marks the receipt failed instead of resuming a partial stream. Wait for the failed operation and normal readiness before submitting a new backup. No manifest means an incomplete prefix.
- **Interrupted restore:** the durable swap journal rolls back an uncommitted replacement; a committed replacement remains. If rollback or startup is blocked, retain all stores/receipts and investigate. Do not manually delete staging/previous-store directories or remove stop holds while recovery owns them.
- **Slow stop or recovery:** allow time for checkpointing and store recovery, inspect disk latency/free space and runtime termination history. Never bypass an outstanding writer or stop hold by launching another JVM.

Logs and operations may contain sensitive application context even though managed secrets are excluded from public diagnostics. Follow [security](../security.md) and [shared operations](../operations.md) for collection and retention.
