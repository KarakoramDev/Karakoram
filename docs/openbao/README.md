<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# OpenBao

Karakoram operates native [OpenBao](https://openbao.org/) servers on member runtimes and persistent volumes. OpenBao is MPL-2.0; Karakoram remains Apache-2.0. See [ADR 0047](../adr/0047-openbao-engine.md) for the design.

## At a glance

| Setting | Value |
| --- | --- |
| Catalog default | OpenBao **2.7.2** |
| Topology | `cluster`, component `engine`, **1/3/5** members; default 3; `dev` = 1, `prod` = 3 |
| Per-member defaults | Shared CPU, 1 core, 1 GB RAM, 5 GB persistent data volume |
| Resource minima | 1 whole core, 0.5 GB RAM, 5 GB disk |
| Storage | Integrated Raft by default; [PostgreSQL storage integration caveat](operations.md#postgresql-storage-opt-in) |
| Seal | Shamir by default, 5 shares / threshold 3; immutable transit or AWS KMS auto-unseal option |
| TLS | Mandatory `requireTLS`; self-signed cluster CA by default |
| Recovery | S3 `raft-snapshot` backups; no PITR |
| Application access | OpenBao HTTPS API and native tokens/policies; no published Karakoram management token |

Each member has its own runtime and volume. Replica count and the `prod` preset do not guarantee independent failure domains; see [placement limitations](../limitations.md). A singleton has no quorum redundancy. Runtime disk is separate from persistent Raft data; replacement temporarily needs another runtime and volume.

Runtime preparation installs CA certificates, curl and Python 3 before downloading OpenBao and its plugins. Python validates archive members before extraction; a minimal Ubuntu runtime must not rely on a preinstalled interpreter. Dokploy performs these steps during the member image build, while Nomad performs them during runtime preparation.

## Create

Save `openbao.json`:

```json
{
  "name": "secrets",
  "spec": {
    "provider": "openbao",
    "version": "2.7.2",
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

`engineOptions` is a typed object; unknown fields and `null` are rejected. Raw `engineConfig` overrides are not supported. Inspect the running engine driver catalog for its schema.

- `unseal.shares` and `unseal.threshold` are immutable. Bounds: `1 <= threshold <= shares <= 10`; multiple shares require threshold at least 2. Set both explicitly for a nondefault split.
- Seal configuration and storage selection are immutable. Write-only seal credentials can be rotated; changing the seal type is not supported.
- `logLevel`: `trace`, `debug`, `info` (default), `warn`, `error`.
- Version changes are forward-only and catalog-bound. OpenBao 2.7.1 is deprecated; upgrade existing clusters to the default 2.7.2 with a quorum-preserving rolling member replacement.
- No PITR, seal migration, performance/DR replication, OpenBao namespaces or HSM seals. Lost-quorum recovery is manual.
- [PostgreSQL](https://www.postgresql.org/) storage uses the shared PostgreSQL binding service that `karakoram server` constructs once and passes to the PostgreSQL bindings API, [Keycloak](https://www.keycloak.org/), OpenBao and [Temporal](https://temporal.io/). Engine driver metadata advertises `postgresql-storage` only when that service is present; a composition without it rejects the option. It is not required for ordinary PostgreSQL credential connections.
- [RabbitMQ](https://www.rabbitmq.com/) credential connections require one shared default/maximum TTL pair per connection. Native TLS credential issuance, permissions and revocation are covered by development proofs (see [verification](../verification.md#openbao-coverage)).

## Security maintenance

[OpenBao 2.7.2](https://github.com/openbao/openbao/releases/tag/v2.7.2) fixes four advisories (no assigned CVE IDs): [ACL noncanonical-path deny bypass](https://github.com/openbao/openbao/security/advisories/GHSA-736f-82w7-hj22), [public OIDC client-credentials issuance](https://github.com/openbao/openbao/security/advisories/GHSA-q824-44ff-f5j5), [SSH wildcard identity principals](https://github.com/openbao/openbao/security/advisories/GHSA-h76r-48x5-36hg), and [transit import path traversal](https://github.com/openbao/openbao/security/advisories/GHSA-5pmx-2p74-x478).

SSH certificate identity templates now deny wildcard (`*`) principals by default. Review SSH roles before upgrading; explicitly setting `allow_wildcards_in_identity_templates=true` restores the previous behavior only where that risk is intended. AWS KMS plugin 0.1.1 is an upstream dependency-update rebuild, not a seal migration: retain the same KMS key and IAM access and verify auto-unseal after each member replacement. No plugin-specific CVE set is claimed.

## MongoDB plugin upgrades

v0.4.7 installs and registers **both** MongoDB database plugin 0.0.1 and [0.0.2](https://github.com/openbao/openbao-plugins/releases/tag/database-mongodb-v0.0.2). The new release is an upstream dependency-update rebuild; its notes do not enumerate fixed CVEs. The `_v1` executable suffix is the plugin protocol, not the release version.

A quorum-preserving member rollout installs the two distinct binaries before starting OpenBao. OpenBao declarative startup registers their exact locked versions; the old registration is deliberately retained for the whole v0.4.7 release. Durable member deployment receipts prevent connection migration until every member has the new binaries, so failover cannot strand a connection on an old runtime.

Plugin declarations are activated **per member**, only after its successful runtime preparation/deployment receipt is persisted. A control-plane-only upgrade or configuration pull does not prove artifact installation: members without that receipt retain MongoDB 0.0.1 and, for AWS KMS seals, KMS 0.1.0 declarations, including across process restart/unseal. Completed preparation activates dual MongoDB registration and KMS 0.1.1 on that member; fresh members prepare and activate the new set during creation. Managed connection migration still waits for every member to finish the gated rollout and recover health.

New Karakoram-managed MongoDB connections explicitly select `v0.0.2`. Reconciliation also migrates existing **Ready** managed connections with a partial native configuration update after rollout, preserving the rotated database password, roles and lease state. A failure is reported with the connection name and retried idempotently from native configuration after restart; no bootstrap credential replay or lease-prefix revocation is performed. Existing database sessions and issued credentials are not deleted; native plugin connections may reconnect during replacement or configuration update.

Operator-created database mounts/configs are **not** rewritten. They continue to use the real registered 0.0.1 binary, including renewal/revocation of existing leases. After every member has completed rollout, move a chosen native connection explicitly (replace `MOUNT` and `CONNECTION`):

```sh
bao read MOUNT/config/CONNECTION
bao write MOUNT/config/CONNECTION plugin_version=v0.0.2 verify_connection=true
bao read MOUNT/config/CONNECTION
```

Verify dynamic credential issuance, renewal and revocation before changing more connections. Do not deregister 0.0.1 or remove its executable in v0.4.7. Removal requires a later release with a separate inventory and migration gate for operator-created mounts. Rolling back the control plane does not undo native version pins: retain both binaries/registrations, or explicitly restore each migrated connection's `plugin_version=v0.0.1` while both are available before reverting to a runtime that installs only 0.0.1. Keep the same KMS key and seal configuration throughout.


## Guides

- [API access, TLS, offline key custody and database connections](access-and-tls.md)
- [Raft snapshots, restore and clone](backups.md)
- [Formation, unseal, membership, maintenance and troubleshooting](operations.md)
- [Verification coverage and deployment checks](../verification.md)
