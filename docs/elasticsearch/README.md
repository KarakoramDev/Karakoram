<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Elasticsearch

Deploy [Elasticsearch](https://www.elastic.co/elasticsearch) and its optional, version-coupled Elastic Stack components on [Zerops](https://zerops.io/).

Opt-in [cluster data MCP](../data-mcp.md) provides native scoped Elasticsearch tools without a companion. Configure `scope.indexPatterns`; hidden/system/dot indices, including hidden data-stream backing indices, remain unreachable. Writes require four independent opt-ins.

## At a glance

| Area | Supported configuration |
|---|---|
| Versions | `9.5.5` (default), `9.5.4`, `9.4.8`, `8.19.23`; use your installation's catalog |
| Topologies | Combined master/data nodes or dedicated masters plus data nodes; counts below |
| Minimum RAM per instance | Data/combined: 1.5 GiB; dedicated master and Logstash: 1 GiB; enabled Kibana: 1.5 GiB on 9.x, 1 GiB on 8.x; APM: 0.25 GiB |
| Minimum disk | Elasticsearch nodes: 5 GiB persistent; optional components: 1 GiB runtime disk |
| Security | Elasticsearch HTTP/transport TLS required; `requireTLS` only |
| Backups | Native S3 snapshots; see [Snapshots](snapshots.md) |
| Components | Optional Kibana, Logstash and standalone APM Server at the exact Elasticsearch version |

[Access and security](access-and-security.md), [Stack components](stack.md), [Operations](operations.md), [platform limitations](../limitations.md) and [verification scope](../verification.md).

## Topologies and presets

| API topology | Node roles | Counts |
|---|---|---|
| `cluster` | Combined master/data/ingest/remote-cluster-client/transform | `components.engine.replicas`: 1–9 |
| `dedicatedMasters` | Master-only nodes plus data/ingest/remote-cluster-client/transform nodes | `components.extra.master.replicas`: 3 or 5; `components.engine.replicas`: 1–20 |

The UI labels these **Combined master and data nodes** and **Dedicated masters + data nodes**. Applications use data/combined nodes, not dedicated masters. Topology type is immutable.

| Preset | Initial composition |
|---|---|
| `dev` | 1 combined node + 1 Kibana |
| `prod` | 3 combined nodes + 1 Kibana |
| `observability` | 3 combined nodes + 1 Kibana + 1 Logstash + 1 APM |
| `large` | 3 dedicated masters + 3 data nodes + 1 Kibana |

Use one combined node for disposable development; usually choose 3/5 for production voting. Even counts are allowed with warnings. Two combined nodes need both voters: losing either stops writes. One data node has no redundant shard copy, regardless of master count. Presets do not establish independent failure domains.

Optional replicas are [Kibana](https://www.elastic.co/kibana) 0–3, [Logstash](https://www.elastic.co/logstash) 0–5 and APM 0–3. Zero disables a component. Each uses one service with multiple containers and ephemeral disks; components cannot attach to another cluster or upgrade independently. Without a preset, defaults are three combined nodes, or three dedicated masters plus two data nodes, with optional components disabled.

If a restart is waiting for an optional component to become ready, setting that component's replicas to zero skips it in the restart pass. Normal reconciliation then deletes its service without requiring it to recover first. Skipped components are recorded separately from completed restarts, including across control-plane restarts; re-enabling one before the pass finishes requires its restart and readiness before completion. Enabled components and Elasticsearch nodes still retain their restart readiness gates.

## Sizing and heap

API RAM/disk values use GiB; CPU values use cores. Engine driver starting resources are:

| Component | RAM bounds | Disk bounds |
|---|---|---|
| Data/combined node | 2–2 GiB | 5–20 GiB persistent |
| Dedicated master | 1.5–1.5 GiB | 5–20 GiB persistent |
| Kibana | 1.5–2 GiB | 1–5 GiB ephemeral |
| Logstash | 1.5–1.5 GiB | 1–5 GiB ephemeral |
| APM | 0.5–0.5 GiB | 1–5 GiB ephemeral |

The `dev` preset caps Kibana at 1.5 GiB. All components start with shared CPU bounds 1–2. CPU bounds are whole cores, at least one; maximum bounds cannot be below minima. Omitted resource bounds use engine driver defaults. Size for replicas, rebalance/recovery, disk watermarks and queues, not just current data size. More nodes cannot drain shards onto disks without headroom. See [resource changes](../clusters.md) for platform growth constraints.

Enabled Kibana 9.x requires at least **1.5 GiB minimum RAM** for creation, enablement, replica increases, intentional RAM changes and every version change (including patch upgrades). Kibana 8.x retains its 1-GiB admission floor; neither floor is a startup-time or workload-capacity guarantee. Disabled Kibana retains structural resource checks.

Existing smaller Kibana 9.x instances are not resized automatically: unchanged same-version updates and replica decreases/disable remain accepted. Status identifies the below-minimum allocation and directs an explicit increase of `spec.components.extra.kibana.resources.minRam` and `maxRam` together to at least 1.5 GiB. Before any version change, **resize Kibana first** at the current version and wait for readiness; a combined resize and upgrade is refused when the old enabled Kibana is below the target floor.

Heap derives from configured **minimum** RAM, not the current autoscaled allocation:

- Elasticsearch: equal initial/maximum heap; 40% through 4 GiB, otherwise 50%, rounded down to a 64-MiB multiple and clamped to 512–31744 MiB. At 2 GiB: 768 MiB.
- Kibana: Node.js old-space cap 60%, minimum 512 MiB; 614 MiB at 1 GiB and 921 MiB at 1.5 GiB. Legacy allocations retain this same formula.
- Logstash: equal initial/maximum heap 50%, minimum 384 MiB.
- APM: Go process, no JVM heap.

Heap excludes filesystem cache, native/direct memory, threads and queues. Elasticsearch's cap does not guarantee compressed object pointers: 26 GiB is conservative on most systems; some permit about 30 GiB. Check startup logs and [Elastic JVM guidance](https://www.elastic.co/docs/reference/elasticsearch/jvm-settings). Small defaults are not production throughput sizing; review [Kibana](https://www.elastic.co/docs/deploy-manage/production-guidance/kibana-traffic-scaling-considerations) and [Logstash](https://www.elastic.co/docs/reference/logstash/jvm-settings) requirements.

Host kernel/process limits remain platform constraints. Check `vm.max_map_count` against Elastic's recommendation of `1048576`; Karakoram cannot change the shared-host sysctl. A value of `262144` can meet bootstrap requirements without meeting that recommendation. Keep bootstrap checks enabled; memory locking is not supported. See [runtime rationale](../adr/0025-elasticsearch-engine-runtime-model.md).

The inherited `nofile` **hard limit must be at least 65,535**. The runner raises the soft limit only to that inherited hard limit; it cannot raise the hard limit. Elasticsearch bootstrap checks still enforce the minimum.

## Create

1. Install/authenticate [karactl](../cli.md) or open **Clusters → Create cluster**.
2. Select the namespace, **Elasticsearch**, an advertised exact version, topology/preset and component resources. Names match `^[a-z][a-z0-9]{1,11}$`.
3. Keep private exposure initially. Review aggregate resources and placement constraints. The wizard clears dependent public flags, and disabled Logstash pipelines, when disabling components, including for clones.
4. Submit; inspect cluster health, members and observed generation, not just request acceptance.

Inspect the current catalog with `karactl providers get elasticsearch -o json`. There is no CLI `--preset`: copy a catalog `presets[].spec`, retain `provider: elasticsearch`, then customize it.

This canonical file creates an observability stack with private endpoints and nightly snapshots. [Register storage](../backups.md) named `archive` in the selected namespace first, or omit `backup`. Save as `elasticsearch.yaml`:

```yaml
name: search
spec:
  provider: elasticsearch
  version: 9.5.5
  topology: {type: cluster}
  components:
    engine:
      replicas: 3
      resources:
        cpuMode: SHARED
        minCpu: 1
        maxCpu: 2
        minRam: 2
        maxRam: 2
        minDisk: 5
        maxDisk: 20
    extra:
      kibana: {replicas: 1}
      logstash: {replicas: 1}
      apm: {replicas: 1}
  tls: {mode: requireTLS, ca: selfSigned}
  exposure: {public: false}
  engineOptions:
    elasticsearch: {settings: {}}
    kibana: {public: false, settings: {}}
    logstash: {pipelines: [], persistentQueue: false}
    apm: {public: false, apiKeys: true}
  backup:
    storage: archive
    schedules:
      - name: nightly
        cron: "0 2 * * *"
        type: snapshot
        retainCount: 7
        enabled: true
```

```sh
karactl -n default clusters create -f elasticsearch.yaml --wait
karactl -n default clusters status search
karactl -n default clusters members search
```

The API accepts the same body at `POST /api/v1/clusters?namespace=default`; see [API mechanics](../api.md). For dedicated masters, change the topology and add `components.extra.master` with a permitted count; `engine` then means data-only nodes.

Settings belong in `engineOptions.elasticsearch.settings` or `engineOptions.kibana.settings`, not `engineConfig`. Undeclared fields and conflicting dotted/nested settings are rejected. Karakoram reserves roles, discovery, network/path/ports, security/TLS, [S3](https://aws.amazon.com/s3/) clients and Kibana connection/encryption/public-URL settings. See [component settings](stack.md). Patch files wrap changes in `{spec: {...}}`; arrays replace complete arrays.

## How it runs

Each Elasticsearch node has a singleton [Ubuntu](https://ubuntu.com/) runtime and a separate Local Storage volume, including dedicated masters. This is not Zerops's managed Elasticsearch service. Official exact-version archives are SHA-512-verified; unprivileged supervised processes use bundled runtimes. Formation bootstraps once, records cluster identity and removes bootstrap configuration; retained-volume resume does not bootstrap a different cluster. Security precedes optional component deployment. Local readiness is separate from formation and shard redundancy.

## Limits

No ML provisioning, arbitrary role pools, hot/warm/cold pools, version skew or downgrade is supported. No Elasticsearch shard-awareness policy maps replicas to physical hosts; see [shared placement/storage limitations](../limitations.md). Maintenance, ingress, queues and recovery have specific limits in [Operations](operations.md), [Access](access-and-security.md), [Stack components](stack.md) and [Snapshots](snapshots.md).

## Licensing

Karakoram is Apache-2.0, but contains no Elastic code and downloads rather than redistributes official default builds on user infrastructure. Elasticsearch/Kibana binaries use ELv2; AGPLv3/SSPL source options do not license those binaries. Logstash/APM offer OSS builds, but this engine driver uses default builds. Provisioning uses Basic-tier security, TLS, API keys, service accounts, snapshots and monitoring without bypassing paid-feature controls.

**Warning:** ELv2 restricts hosted/managed resale exposing substantial Elasticsearch/Kibana functionality. User-owned infrastructure and non-redistribution are not blanket exemptions; obtain appropriate Elastic rights and legal review before offering such services. OpenSearch is not this engine driver. See [ADR 0029](../adr/0029-elastic-licensing.md), [Elastic's FAQ](https://www.elastic.co/pricing/faq/licensing) and [ELv2 examples](https://www.elastic.co/licensing/elastic-license/faq); this is not legal advice.
