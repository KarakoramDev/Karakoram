<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Configuration and topology

[Overview](README.md) · [Access](access-and-tls.md) · [Operations](operations.md)

## Supported shape

`engineConfig` is not supported. Use string-valued, provider-allowlisted `engineOptions.parameters`; unknown settings are rejected. The provider remains authoritative even when a UI field validates locally. TLS must be `requireTLS`. Network listeners, data identity, replication, archiving and system accounts are managed settings, not raw configuration overrides.

SQL listeners follow the backend layout: IPv4-only layouts retain IPv4 sockets; IPv6-capable layouts use MySQL's `*` all-addresses listener and separate IPv4/IPv6 ProxySQL writer and optional reader sockets. Proxy administration remains loopback-only. The control plane sends this policy only to agents advertising SQL-listener support on their configuration pull; updating the control plane alone does not change older agents' listeners. MySQL/PXC do not support `upgrade-agents`; existing deployments need replacement with a corrected agent through a supported lifecycle, rather than hand-editing managed configuration.

Example shared by `mysql` and `pxc`:

```yaml
engineOptions:
  parameters:
    max_connections: "250"
    slow_query_log: "ON"
    long_query_time: "0.5"
    transaction_isolation: REPEATABLE-READ
  proxy:
    readWriteSplit: false
    readerPort: false
  binlog:
    archiveInterval: 300
    expireSeconds: 604800
```

| Option | Meaning / limits |
| --- | --- |
| `parameters` | Selected connection, InnoDB buffer/I/O, timeout, packet/buffer/cache, logging and isolation settings; values are strings |
| `proxy.readWriteSplit` | Default false; opt-in SELECT-pattern reader routing, not a complete SQL classifier |
| `proxy.readerPort` | Default false; optional private reader listener on 3307 |
| `binlog.archiveInterval` | Integer seconds, 1–86400; default 300; bounds closed-log rotation, not an archive-success guarantee |
| `binlog.expireSeconds` | Integer seconds, greater than archive interval, up to 2147483647; default 604800 |

The UI offers the supported parameter names and validates numeric bounds, booleans (`ON`/`OFF` or `0`/`1`) and isolation values. A connection limit can exhaust memory long before its syntactic maximum; size buffers and concurrency together. Use the catalog resource minima and do not shrink persistent disks.

The startup-only `innodb_buffer_pool_instances` and `table_open_cache_instances` parameters accept integers 1–64. They require rolling maintenance and remain pinned per member until that member's turn; changing them is not a live-only tuning operation.

Configuration is desired state rendered into startup options. Supported dynamic values also apply with `SET GLOBAL`; restart-required changes use managed maintenance. **Do not use `SET PERSIST`**: persisted settings travel with copied data and can resurrect obsolete topology or security settings. Managed startup discards persisted-variable overrides. Direct SQL tuning is not a durable replacement for changing the cluster specification.

## MySQL Group Replication only

```yaml
engineOptions:
  groupReplication:
    consistency: BEFORE_ON_PRIMARY_FAILOVER
    unreachableMajorityTimeout: 30
```

Consistency choices are `BEFORE_ON_PRIMARY_FAILOVER` (default), `EVENTUAL`, `BEFORE`, `AFTER`, `BEFORE_AND_AFTER`. The default holds a newly elected primary until its backlog is applied; it is not a general guarantee for reads routed to other members. Majority timeout is integer seconds, 1–31536000. Minority exit makes a member read-only; before exit, writes may block and client timeouts remain ambiguous.

Groups use authenticated TLS on the [MySQL](https://www.mysql.com/) communication stack, single-primary mode, read-only minority exit and no multi-primary routing. Formation bootstraps once. New members clone from an online donor of the exact same server version; cloning copies data, not configuration. Established members automatically rejoin after restart when the group remains available. Full-outage recovery is a separate, evidence-based operation; see [recovery](operations.md#full-outage-recovery).

Every Group Replication server starts with `super_read_only=ON`, including initialized restarts with unchanged credentials. It remains fenced while waiting to rejoin or if rejoin fails; Group Replication lifts the fence only for a legitimate primary. Single-server deployments remain writable, and [PXC](https://docs.percona.com/percona-xtradb-cluster/) uses its own wsrep eligibility rather than this Group Replication startup fence.

Group Replication data members require **at least 2 GiB minimum RAM each** on every backend, even with custom buffer parameters; a larger elastic maximum does not bypass this floor. The managed message cache is one eighth of the member's minimum RAM, with a 128 MiB floor, instead of the upstream 1 GiB default. It is derived from the control plane's half-RAM buffer-pool baseline before user overrides. At 2 GiB, the default pool is 1 GiB and the message cache is 256 MiB, leaving 768 MiB for cache metadata, mysqld, Performance Schema, connections, the co-located agent and clone. This is explicit-cache headroom, not a total-RSS guarantee: tune concurrency, transaction sizes and custom buffers for the workload. A smaller message history can require distributed recovery when a disconnected member misses retained messages.

The RAM rejection requires the corrected control plane; the cache bound requires corrected **data-member agents**. Updating the control plane alone does not change existing agents' rendering. No wire-schema change is needed. MySQL does not support `upgrade-agents`; replace agents through supported member lifecycle only on a healthy group. An unsuccessful fresh create with no healthy donor/quorum should be deleted and re-created at the supported minimum, not repaired by editing managed files or issuing forced bootstrap commands.

## PXC only

PXC has topology `cluster`, never `single` or `group`. It does not expose Group Replication consistency/majority-timeout controls; omit `groupReplication` entirely. [Galera](https://galeracluster.com/) traffic and state transfer are encrypted. IST uses retained Galera cache when possible; otherwise `xtrabackup-v2` SST transfers the full dataset using the bundled compatible tools. Allow donor/joiner capacity and bandwidth for SST.

PXC data members require **at least 2 GiB RAM each**, including when custom buffer settings are supplied; requests below this floor are rejected before provisioning. MySQL `single` retains its 1 GiB admission floor; `group` requires 2 GiB as described above. Both providers default to 2 GiB data RAM and require at least 5 GB data disk; proxy defaults are 1 GiB RAM. The control plane sizes the default InnoDB buffer pool to half the member's minimum RAM (not its elastic maximum), with a 128 MiB lower bound, and defaults to 200 connections. PXC also retains a 128 MiB Galera cache. The remaining memory is needed for mysqld, Performance Schema, connections, Galera certification/writesets and state transfer; it is not additional buffer-pool capacity. These defaults are not a hard bound on workload memory: size concurrency, transaction size and SST capacity together, and budget custom parameter overrides yourself.

The PXC admission correction is control-plane-only and does not change the agent configuration schema or require an agent rollout. Existing undersized members are not automatically resized; increase their requested minimum/maximum RAM to at least 2 GiB through supported resource maintenance, or delete/re-create an unsuccessful fresh deployment. Unrelated updates to an undersized PXC specification are rejected until its RAM is corrected.

Encryption is configured explicitly per channel: Galera/IST requires `socket.ssl=true`, and SST requires `encrypt=4`, with a shared cluster-traffic certificate/key signed by a dedicated traffic CA. That trust root is separate from SQL and agent trust; SQL/agent leaves cannot authenticate replication. The upstream convenience flag `pxc_encrypt_cluster_traffic` is `OFF` because its automatic behavior overrides channel certificate selection; **this does not disable channel encryption**. MySQL client connections retain per-member certificates and required TLS. These settings are managed, not application overrides.

This follows upstream [manual channel configuration](https://docs.percona.com/percona-xtradb-cluster/8.4/encrypt-traffic.html#ssl-manual-configuration), not a blanket encryption disable. In pinned PXC source [`33f7510`, option assembly](https://github.com/percona/percona-xtradb-cluster/blob/33f7510189c5719c852837d7ad9ba5e7d6431f4d/sql/wsrep_mysqld.cc#L1202-L1225), automatic mode appends generated TLS options after the configured provider options; the [certificate callback](https://github.com/percona/percona-xtradb-cluster/blob/33f7510189c5719c852837d7ad9ba5e7d6431f4d/sql/ssl_init_callback.cc#L553-L557) obtains those paths from the MySQL SQL-server certificates. Manual mode keeps independent SQL leaves while explicitly protecting every replication/state-transfer channel. See the [shared traffic-key trust boundary](access-and-tls.md#pxc-cluster-traffic-trust-boundary).

`pxc_strict_mode=ENFORCING` stays enabled. Use InnoDB tables with primary keys; non-transactional writes and `LOCK TABLES` are unsupported. Do not assume every incompatible DDL is rejected at creation: a non-transactional table may be created but fail on writes. Audit existing schemas and transaction patterns before migration. Slow members can trigger flow control; Galera certification can reject conflicting transactions even though routing uses one writer.

## Application routing

[ProxySQL](https://proxysql.com/) is the application endpoint on 3306. Both clustered topologies require at least one proxy replica; `mysql/single` alone may use zero and connect directly. ProxySQL monitors replication eligibility, not just TCP reachability, and keeps a single routed writer. Proxy administration is not a public application endpoint. Multiple proxy replicas do not remove client reconnection requirements.

Reader routing is opt-in. Neither SELECT-pattern splitting nor port 3307 promises causal reads, transaction classification or read-after-write consistency. Keep consistency-sensitive reads and transactions on the writer and qualify the driver's transaction/prepared-statement behavior. Public exposure applies to the normal client endpoint, not a promise of public access to every private replication or reader listener. See [access and TLS](access-and-tls.md).
