<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# OpenBao

Karakoram operates native [OpenBao](https://openbao.org/) servers on member runtimes and persistent volumes. OpenBao is MPL-2.0; Karakoram remains Apache-2.0. See [ADR 0047](../adr/0047-openbao-engine.md) for the design.

## At a glance

| Setting | Value |
| --- | --- |
| Catalog default | OpenBao **2.7.1** |
| Topology | `cluster`, component `engine`, **1/3/5** members; default 3; `dev` = 1, `prod` = 3 |
| Per-member defaults | Shared CPU, 1 core, 1 GB RAM, 5 GB persistent data volume |
| Resource minima | 1 whole core, 0.5 GB RAM, 5 GB disk |
| Storage | Integrated Raft by default; [PostgreSQL storage integration caveat](operations.md#postgresql-storage-opt-in) |
| Seal | Shamir by default, 5 shares / threshold 3; immutable transit or AWS KMS auto-unseal option |
| TLS | Mandatory `requireTLS`; self-signed cluster CA by default |
| Recovery | S3 `raft-snapshot` backups; no PITR |
| Application access | OpenBao HTTPS API and native tokens/policies; no published Karakoram management token |

Each member has its own runtime and volume. Replica count and the `prod` preset do not guarantee independent failure domains; see [placement limitations](../limitations.md). A singleton has no quorum redundancy. Runtime disk is separate from persistent Raft data; replacement temporarily needs another runtime and volume.

## Create

Save `openbao.json`:

```json
{
  "name": "secrets",
  "spec": {
    "provider": "openbao",
    "version": "2.7.1",
    "topology": {"type": "cluster"},
    "components": {"engine": {"replicas": 3}},
    "tls": {"mode": "requireTLS", "ca": "selfSigned"},
    "engineOptions": {
      "storage": {"type": "raft"},
      "seal": {"type": "shamir"},
      "unseal": {"shares": 5, "threshold": 3},
      "logLevel": "info"
    }
  }
}
```

```sh
karactl -n NS providers get openbao -o json
karactl -n NS clusters create -f openbao.json --wait --timeout 30m
karactl -n NS clusters status secrets -o json
karactl -n NS clusters members secrets -o json
```

A wait timeout does not cancel creation. Continue observing with `clusters wait secrets --timeout 30m`, rather than submitting another create. Formation includes initialization, unsealing and healthy Raft voter checks, not just runtime startup.

## Options and limits

`engineOptions` is a typed object; unknown fields and `null` are rejected. Raw `engineConfig` overrides are not supported. Inspect the running provider catalog for its schema.

- `unseal.shares` and `unseal.threshold` are immutable. Bounds: `1 <= threshold <= shares <= 10`; multiple shares require threshold at least 2. Set both explicitly for a nondefault split.
- Seal configuration and storage selection are immutable. Write-only seal credentials can be rotated; changing the seal type is not supported.
- `logLevel`: `trace`, `debug`, `info` (default), `warn`, `error`.
- Version changes are forward-only and catalog-bound. The current catalog contains only 2.7.1, so no newer-version upgrade is currently available.
- No PITR, seal migration, performance/DR replication, OpenBao namespaces or HSM seals. Lost-quorum recovery is manual.
- PostgreSQL storage needs an injected `pgbinding.Service` via `internal/server.engineOptions.Bindings`. Default server composition supplies none, so admission rejects it and provider metadata does not advertise `postgresql-storage`; concrete PgPlatform composition remains an integration prerequisite. It is not required for ordinary PostgreSQL credential connections.
- RabbitMQ credential connections require one shared default/maximum TTL pair per connection. Native TLS credential issuance, permissions and revocation have development proof; deployment-level connectivity and lifecycle acceptance remain pending.

## Guides

- [API access, TLS, offline key custody and database connections](access-and-tls.md)
- [Raft snapshots, restore and clone](backups.md)
- [Formation, unseal, membership, maintenance and troubleshooting](operations.md)
- [Verification coverage and deployment checks](../verification.md)
