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

The server uses SIGTERM and declares a **45-second graceful stop budget** for checkpointing during agent shutdown, bounded by the remaining overall shutdown deadline. A timeout is an error, not proof of a stopped JVM; the supervisor does not turn it into successful shutdown or immediately SIGKILL that child. The infrastructure may subsequently terminate the runtime according to its own grace period. The temporary credential-recovery child has a separate 45-second stop wait and is force-killed if it cannot exit. Check runtime/platform stop grace and allow crash recovery after forced termination.

## Upgrades

The supported transition is **2026.09.0 → 2026.10.0**, requested by changing `spec.version`. It requires a fresh successful backup and restarts on the new pinned artifacts; the store and system database upgrade in place. Plan an outage, verify the dump and recovery destination first, and do not combine this with an assumed rollback-by-downgrade strategy. Downgrades and other version paths are rejected. See [shared upgrade admission](../clusters.md) and [offline dumps](backups.md).

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
- **TLS connection failure:** trust the cluster CA in the client, use a certificate-covered endpoint and verify the scheme. Public routing discovery can return private hostnames; use a verified direct `bolt+s` connection when appropriate. Do not disable certificate verification to conceal a trust error.
- **Interrupted backup:** recovery restarts the server and marks the receipt failed instead of resuming a partial stream. Wait for the failed operation and normal readiness before submitting a new backup. No manifest means an incomplete prefix.
- **Interrupted restore:** the durable swap journal rolls back an uncommitted replacement; a committed replacement remains. If rollback or startup is blocked, retain all stores/receipts and investigate. Do not manually delete staging/previous-store directories or remove stop holds while recovery owns them.
- **Slow stop or recovery:** allow time for checkpointing and store recovery, inspect disk latency/free space and runtime termination history. Never bypass an outstanding writer or stop hold by launching another JVM.

Logs and operations may contain sensitive application context even though managed secrets are excluded from public diagnostics. Follow [security](../security.md) and [shared operations](../operations.md) for collection and retention.
