<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Weaviate

Deploy official native [Weaviate](https://weaviate.io/) on private member runtimes with persistent volumes. Karakoram is Apache-2.0; the downloaded Weaviate executable has a separate, mixed license boundary described below.

## At a glance

| Area | Supported configuration |
| --- | --- |
| Engine driver | `weaviate` |
| Versions | `1.40.1` (default), `1.39.12`; inspect the installation catalog |
| Topologies | `single`: one member; `cluster`: 3–9 members, three fixed Raft voters |
| Presets | `dev`: single; `prod`: three-member cluster |
| Resources | Default per member: 1 shared CPU, 2 GiB RAM, 10 GiB persistent disk |
| Security | Anonymous access off; API keys, RBAC and dynamic database users; client TLS by default |
| Backups | Native S3 `full` backups; no incremental backups or PITR |

[Access and TLS](access-and-tls.md) · [Backups](backups.md) · [Operations](operations.md) · [Platform limits](../limitations.md) · [Verification](../verification.md)

## Topology and identity

All members store data. A single member is also its only Raft voter and has no redundancy. A cluster starts with three fixed voters; extra members join as non-voters. Adding data members does not increase metadata quorum size: the three voters still require a majority of two. Topology type is immutable; a singleton cannot become a cluster in place.

Members use stable names `<cluster>n<index padded to six digits>x`, for example `vectorsn000000x`; volume names append `v`. Retired indices are not reused. The terminal letter makes generated identities substring-disjoint for upstream voter selection. Preserve voter identities during replacement; never rename a voter or try to force a fresh quorum by changing bootstrap settings.

`replicationMinimumFactor` defaults to 1 for single and 3 for cluster and cannot exceed member count. Collection replication must fit available members. More members do **not** rebalance existing shards automatically. Scale-down drains replicas using native `MOVE`, then removes only highest-index non-voters; it never removes the fixed three voters. See [scaling](operations.md#scaling).

Replica count does not guarantee independent hosts or failure domains. Require an isolated private network: client TLS does not encrypt Weaviate's internal transports.

## Create

Inspect `karactl providers get weaviate -o json` for current versions, presets, capabilities and option metadata. In the web UI choose **Clusters → Create cluster → Weaviate**, then review topology, resources, backup storage and access. Names are 2–12 lowercase letters/digits beginning with a letter.

Save this API-shaped request as `weaviate.yaml`:

```yaml
name: vectors
spec:
  provider: weaviate
  version: 1.40.1
  topology: {type: cluster}
  components:
    engine:
      replicas: 3
      resources:
        cpuMode: SHARED
        minCpu: 1
        maxCpu: 1
        minRam: 2
        maxRam: 2
        minDisk: 10
        maxDisk: 10
  tls: {mode: requireTLS, ca: selfSigned}
  exposure: {public: false}
  monitoring: {enabled: true}
  engineOptions:
    modules: []
    replicationMinimumFactor: 3
    asyncReplication: true
    queryDefaultsLimit: 25
    diskWarningPercent: 80
    diskReadonlyPercent: 90
```

```sh
karactl -n default clusters create -f weaviate.yaml --wait
karactl -n default clusters status vectors
karactl -n default clusters members vectors
```

Equivalent convenience flags for the topology and defaults above:

```sh
karactl -n default clusters create vectors --provider weaviate \
  --version 1.40.1 --topology cluster --members 3 \
  --min-cpu 1 --max-cpu 1 --min-ram 2 --max-ram 2 \
  --min-disk 10 --max-disk 10 --tls-mode requireTLS --monitoring --wait
```

There is no `--preset` flag. The API accepts the same request body at `POST /api/v1/clusters?namespace=default`. Patches wrap changes in `{spec: {...}}`; arrays replace completely. Do not use `engineConfig` or raw environment variables: only typed `engineOptions` are accepted.

## Engine options

| `spec.engineOptions` field | Default | Validation / meaning |
| --- | --- | --- |
| `modules` | `[]` | Unique API-based module names from the allowlist below; `backup-s3` is managed, not user-listed |
| `inference` | `{}` | Map of supported local module names to `{url: "https://inference.example.com"}`; HTTP(S), no embedded credentials, query or fragment |
| `replicationMinimumFactor` | single: `1`; cluster: `3` | Integer 1 through engine replicas; minimum collection replication |
| `asyncReplication` | `true` | Enable asynchronous replica repair |
| `queryDefaultsLimit` | `25` | Integer 1–10000 |
| `diskWarningPercent` | `80` | Integer; `1 <= warning < readonly` |
| `diskReadonlyPercent` | `90` | Integer greater than warning, at most 95 |

Unknown fields and explicit null option values are rejected. Module/configuration changes require member restarts.

### Modules and inference

Automatic API-module registration is disabled (`API_BASED_MODULES_DISABLED=true`). Only these explicitly selected API modules are accepted:

```text
text2vec-aws, multi2vec-cohere, text2vec-cohere, text2vec-databricks,
text2vec-google, multi2vec-google, text2vec-huggingface, text2vec-jinaai,
multi2vec-jinaai, text2vec-mistral, text2vec-octoai, text2vec-openai,
generative-deepseek, text2vec-digitalocean, text2vec-morph, text2vec-voyageai,
multi2vec-voyageai, text2vec-weaviate, text2multivec-jinaai, text2vec-nvidia,
multi2vec-nvidia, multi2vec-twelvelabs, multi2multivec-jinaai,
multi2multivec-weaviate, multi2vec-aws, generative-anthropic,
generative-anyscale, generative-aws, generative-cohere, generative-contextualai,
generative-databricks, generative-digitalocean, generative-friendliai,
generative-google, generative-meta, generative-mistral, generative-nvidia,
generative-octoai, generative-openai, generative-xai, reranker-cohere,
reranker-contextualai, reranker-voyageai, reranker-jinaai, reranker-nvidia
```

Clients supply provider API keys in their native per-request headers. Karakoram does not persist those keys or provision inference services. Select vectorizers/generative modules in collection configuration; no default vectorizer is selected for you.

The `inference` keys and their process endpoint mappings are:

| Module | Managed environment mapping |
| --- | --- |
| `text2vec-transformers` | `TRANSFORMERS_INFERENCE_API` |
| `multi2vec-clip` | `CLIP_INFERENCE_API` |
| `reranker-transformers` | `RERANKER_INFERENCE_API` |
| `multi2vec-bind` | `BIND_INFERENCE_API` |
| `img2vec-neural` | `IMAGE_INFERENCE_API` |
| `text2vec-model2vec` | `MODEL2VEC_INFERENCE_API` |
| `text2vec-gpt4all` | `GPT4ALL_INFERENCE_API` |
| `ner-transformers` | `NER_INFERENCE_API` |
| `qna-transformers` | `QNA_INFERENCE_API` |
| `sum-transformers` | `SUM_INFERENCE_API` |
| `text-spellcheck` | `SPELLCHECK_INFERENCE_API` |

These are typed URL settings, not arbitrary environment passthrough. Ollama's collection/request `apiEndpoint` is not a process environment option; Ollama modules are not part of this engine driver's process module allowlist.

## Resources and runtime

RAM/disk values are GiB; CPU is whole cores. Minimum per member is 1 CPU, 1 GiB RAM and 1 GiB disk; maximum bounds must be at least minima. Disk can grow, not shrink. Defaults are starting values, not workload sizing recommendations: reserve space for vectors, indexes, replicas, recovery and native backups.

Karakoram disables upstream automatic resource limiting (`LIMIT_RESOURCES=false`), sets `GOMEMLIMIT` to 85% of configured **maximum** RAM (truncated to MiB) and sets `GOMAXPROCS` from configured maximum CPU, at least one. These derive from configured bounds, not a live autoscaled allocation; leave sufficient minimum-RAM headroom for the workload. The Go memory limit is not a whole-process RSS cap. Data persists separately from versioned executables. Profiling and telemetry are disabled.

## Ports and network isolation

| Port | Purpose | Exposure |
| --- | --- | --- |
| 8080/TCP | REST; HTTPS-only with `requireTLS` | Private; optional public TLS client endpoint |
| 50051/TCP | Client gRPC; TLS with `requireTLS` | Private; optional public TLS client endpoint |
| 7946/TCP **and UDP** | Memberlist gossip | Private only |
| 7947/TCP | Internal data/replication HTTP | Private only |
| 8300/TCP | Raft consensus | Private only |
| 8301/TCP | Raft RPC | Private only |
| 2112/TCP | Native Prometheus metrics | Private only; upstream wildcard-bound |
| 9443/TCP | Karakoram agent HTTPS | Private authenticated management |

Native REST and gossip bind according to the provider's private address family: `0.0.0.0` for IPv4, `::` otherwise. These wildcard binds are not access controls. Namespace relay/control access is limited to exact member targets on 8080, 7947 and 9443; client gRPC is not relayed.

Internal gossip, data and Raft transports are plaintext upstream. Shared internal basic authentication protects data/replication calls and internal Raft removal, but does not encrypt them or authenticate gossip/Raft consensus. The gRPC listener also hosts internal replication services. Omitting public port mappings is **not a firewall**: use an isolated private/encrypted provider network with policy restricting untrusted peers; native metrics bind wildcard and are scraped locally.

## Limits

No topology conversion, voter retirement, automatic shard rebalance, arbitrary modules/environment, OIDC configuration, incremental backups, backup deduplication or PITR. Recovery preserves member count, not resharding. Single-member restart interrupts access; maintenance requires healthy synchronized peers and quorum. See [recovery constraints](backups.md) and [troubleshooting](operations.md#troubleshooting).

## Licensing

Official release executables contain BSD-3-Clause community code **and runtime-gated enterprise code**; the artifact lock records `BSD-3-Clause AND LicenseRef-Weaviate-Enterprise`. They are not BSD-only binaries. Karakoram enables only BSD-licensed community features, forces `NAMESPACES_ENABLED=false`, `SELF_RECOVERY_ENABLED=false` and `BACKUP_DEDUPE_ENABLED=false`, never sets a license key or key-file option, and does not request backup replica deduplication.

Members download exact official binaries from upstream (or an operator mirror) and verify locked SHA-256 digests. Karakoram does not redistribute Weaviate binaries. This feature boundary does not grant enterprise rights. Review [upstream licensing](https://github.com/weaviate/weaviate/blob/v1.40.1/LICENSE), [native artifact provenance](../native-artifacts.md) and [ADR 0054](../adr/0054-weaviate-engine.md).
