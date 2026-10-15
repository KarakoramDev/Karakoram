# 0025 — Elasticsearch engine and runtime model

Status: Accepted

## Context

Karakoram needs exact version pinning, role-aware topology, serialized upgrades, deterministic security and native snapshot automation. Zerops' managed Elasticsearch contract offers `elasticsearch@8.16` and `elasticsearch@9.2`, with fixed `NON_HA`/`HA` modes rather than Karakoram-owned membership. It does not expose the lifecycle control required for node roles, topology changes, rolling upgrades, security realms, snapshot automation and Kibana coupling ([service types](https://docs.zerops.io/references/import-yaml/type-list), [Elasticsearch](https://docs.zerops.io/elasticsearch/overview), [scaling](https://docs.zerops.io/features/scaling)). This is a limitation of the documented integration contract, not a claim that native Elasticsearch lacks those features.

## Decision

Implement provider `elasticsearch` using one `ubuntu@24.04` runtime with `maxContainers: 1` and one `local-storage:single@1` volume for every Elasticsearch node, including dedicated masters. Follow [ADR 0007](0007-member-deployment.md): deploy the control-plane executable as the node supervisor, use temporary shutdown to avoid concurrent writers, and let the provider order disruptive changes. Bind dual-stack and publish the service hostname. Runtime-plus-volume is the portable engine contract; another PaaS backend must still satisfy the kernel, network and persistence requirements.

Install exact official Elasticsearch, Kibana, Logstash and APM Server tarballs in `run.prepareCommands`, verifying each official `.sha512` sidecar before extraction. Include the version in command text so Zerops refreshes its cached prepared image on upgrades, not merely when an environment variable changes ([Zerops pipeline](https://docs.zerops.io/features/pipeline), [Elastic archive installation](https://www.elastic.co/docs/deploy-manage/deploy/self-managed/install-elasticsearch-from-archive-on-linux-macos)). Use the bundled runtimes and run processes as the unprivileged runtime user.

Render product configuration, data and logs under `DATA_DIR/karakoram`. `ES_PATH_CONF`, `KBN_PATH_CONF` and Logstash `--path.settings` target `DATA_DIR/karakoram/config`, not `DATA_DIR/config`; installed product homes remain separate from rendered runtime state.

Use deployment readiness exec `karakoram agent readiness --engine=elasticsearch <component>` for `elasticsearch`, `kibana`, `logstash` or `apm-server`. Zerops was measured to run readiness exec without the service environment, so engine selection must be explicit instead of depending on `KARAKORAM_ENGINE`. Readiness checks the local listener; formation and health remain provider observations.

Use compact names with monotonically allocated, never-reused indices ([ADR 0009](0009-resource-naming.md)); `c` is the cluster name and `i` the node index. Elasticsearch `node.name` is its hostname.

| Resource | Hostname | Local Storage volume |
|---|---|---|
| Combined or data node | `{c}n{i}` | `{c}n{i}v` |
| Dedicated master | `{c}m{i}` | `{c}m{i}v` |
| Kibana | `{c}kb` | None |
| Logstash | `{c}ls` | None |
| APM Server | `{c}apm` | None |

Stateless components follow the router model: one runtime service with N containers, no persistent volume and a shared component identity. Their lifecycle is defined in [ADR 0027](0027-elastic-stack-components.md).

A 2026-10-13 probe of an Ubuntu runtime with fixed 2-GB RAM measured LXC virtualization, cgroup-v2 memory limits visible to the process, container-adjusted memory reporting and no swap. `vm.max_map_count=262144` passed Elasticsearch 9.5.5 production bootstrap checks, but is below Elastic's recommended `1048576`; changing the shared-host sysctl was denied. The inherited file-descriptor limit was soft 1024/hard 65535: the supervisor raises the soft limit to the hard limit before starting children. Memlock was 8 MiB, so do not enable `bootstrap.memory_lock`. These are measured platform facts, not a guarantee for every deployment ([Zerops infrastructure](https://docs.zerops.io/features/infrastructure), [Elastic bootstrap checks](https://www.elastic.co/docs/deploy-manage/deploy/self-managed/bootstrap-checks), [virtual memory](https://www.elastic.co/docs/deploy-manage/deploy/self-managed/vm-max-map-count)).

Default automatic heap sizing was OOM-killed at 2 GB; explicit 768-MB heap started and passed production checks. Always set equal initial/maximum heap from configured minimum RAM: 40% at up to 4 GiB, otherwise 50%, rounded down to a 64-MiB multiple and clamped to 512–31744 MiB. Keep headroom for native memory and filesystem cache. This intentionally overrides Elastic's automatic-sizing recommendation because of the observed failure; the cap is not a universal compressed-oops guarantee ([JVM settings](https://www.elastic.co/docs/reference/elasticsearch/jvm-settings)).

A single-node 9.5.5 cluster subsequently formed on Zerops and reached green health with production bootstrap checks enabled. This is not verification of the full live three-node/component matrix.

## Consequences

Karakoram owns Elasticsearch lifecycle correctness rather than delegating it to a managed service. Node/volume count and operational cost grow with topology. Distinct volumes prevent shared writable data paths but do not guarantee physical host separation ([ADR 0016](0016-placement-anti-affinity.md)); no host anti-affinity or Elasticsearch shard-awareness policy is configured. Local Storage remains single-host and non-replicated; use native snapshots, not live volume archives, for recovery ([Local Storage](https://docs.zerops.io/local-storage/tech-details)). Small defaults and a successful single-node formation probe are not production-capacity or multi-node availability guarantees. Host kernel limits remain a platform responsibility.

## Alternatives considered

- **Zerops managed Elasticsearch:** simpler platform operations, but insufficient documented control and a platform-specific engine lifecycle.
- **One service with many Elasticsearch containers on one volume:** lacks independent node identity and storage; all attached containers are placed with that volume.
- **Privileged kernel tuning or disabling bootstrap checks:** unsafe shared-host assumptions; retain checks and document the measured recommendation gap.
- **Automatic heap sizing:** upstream default, rejected here after the measured 2-GB OOM.
