<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
<h1 align="center">
  <a href="https://karakoram.dev">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/KarakoramDev/Karakoram/main/assets/brand/karakoram-lockup-dark.svg">
      <img alt="Karakoram" src="https://raw.githubusercontent.com/KarakoramDev/Karakoram/main/assets/brand/karakoram-lockup-light.svg" width="320">
    </picture>
  </a>
</h1>

<p align="center"><a href="https://karakoram.dev">karakoram.dev</a> · <a href="#documentation-index">Documentation</a> · <a href="https://github.com/KarakoramDev/Karakoram/blob/main/CONTRIBUTING.md">Contributing</a></p>

**Self-hosted MongoDB, RabbitMQ and Elasticsearch orchestration for Zerops.** Karakoram provisions and operates Percona Server for MongoDB (PSMDB) with Percona Backup for MongoDB (PBM), RabbitMQ, and Elasticsearch with optional Kibana, Logstash and APM Server across your own Zerops projects. It provides a Go control plane, REST API, `karactl` CLI and React web UI—without Kubernetes or a hosted coordinator.

You own the infrastructure, credentials, data and recovery procedures. Zerops is the infrastructure backend, not the operator of your databases. Project site: [karakoram.dev](https://karakoram.dev) · Source: [github.com/KarakoramDev/Karakoram](https://github.com/KarakoramDev/Karakoram).

## Features

### MongoDB

- Replica sets with 1/3/5/7 members, initial-sync-aware scaling/replacement, quorum-aware removal and retained-volume suspend/resume.
- Sharded clusters with config-server and shard replica sets, mongos routers, shard addition and balancer controls.
- S3-compatible PBM logical/physical backups, UTC schedules and count retention, PITR, in-place restore and clone.
- CPU/RAM configuration, persistent disk growth, rolling binary upgrades/restarts and explicit FCV promotion.
- Database users/roles, independently rotatable system accounts, keyfile authentication, generated or supplied TLS certificates and opt-in public TCP access.

### RabbitMQ

- [RabbitMQ 4.3 clusters](docs/user-guide/rabbitmq.md) with persistent per-node storage and quorum-gated rolling scaling, upgrades, restarts and replacement.
- Vhost, user, permission, topic-permission, policy and operator-policy administration; AMQPS and optional public management.
- S3 definitions backups and restore (**no messages or PITR**).

### Elasticsearch

- [Combined-node or dedicated-master topologies](docs/user-guide/elasticsearch.md), rolling maintenance and shard-draining scale-down.
- Native users, roles and API keys, CA-verified HTTPS, S3 snapshots and restore.
- Optional same-version Kibana, Logstash and APM Server. Review the engine guide's licensing and operating constraints.

### Across engines

- Catalog-driven creation, CPU/RAM configuration, persistent disk growth and capability-gated operations.
- Per-member monitoring, authenticated Prometheus output, actor-attributed events/audit activity and operations, logs, local accounts, OIDC, namespace-aware RBAC and owner-scoped API tokens.
- Multi-project [namespaces](docs/user-guide/namespaces.md), project-scoped tokens, outbound authenticated relays and permission-gated cross-namespace restore/clone.

**Read the [limitations](docs/limitations.md) before production use.** Zerops does not guarantee physical-host anti-affinity, Local Storage is single-host and the control plane has one replica. Healthy voters do not prove independent failure domains. Public replica-set routing does not automatically solve external member discovery. Configure backups and exercise recovery before loading important data.

## Quick start on Zerops

**Fastest path:** the [10-minute quick start](docs/quickstart.md) installs Karakoram by pasting one YAML into the Zerops dashboard; no checkout, build tools or CLI needed. The steps below deploy from a local checkout instead.

You need an existing Zerops project, an authenticated zcli and a **project-scoped Full access integration token**. **Sudo mode is not required; leave it disabled** for least privilege. A sudo-mode token can read every sensitive variable in the project, including database and Karakoram secrets; Karakoram never needs to read them. From a local checkout, import the template in `deploy/`:

```sh
PROJECT_ID='your-project-id'
zcli project service-import deploy/zerops/import-local.yaml -P "$PROJECT_ID"
```

In the Zerops dashboard, set the new **karakoram** service's secret `KARAKORAM_ZEROPS_TOKEN` to your project token. Preserve the generated `KARAKORAM_MASTER_KEY` and `KARAKORAM_ADMIN_PASSWORD`; never commit or print secrets. Deploy from the checkout root:

```sh
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --no-git --disable-logs
zcli service enable-subdomain karakoram -P "$PROJECT_ID"
```

Open the service's HTTPS subdomain. Sign in as **admin** with the generated password from its secret variables, complete the required password change, then sign in again. In the create wizard, choose **MongoDB**, **RabbitMQ** or **Elasticsearch**, use an advertised version, keep TLS required and public exposure disabled, and wait for observed readiness. Follow the [MongoDB replica-set](docs/user-guide/replica-sets.md), [RabbitMQ](docs/user-guide/rabbitmq.md) or [Elasticsearch](docs/user-guide/elasticsearch.md) guide for engine-specific setup.

See the [installation guide](docs/install.md) for prerequisites, secret handling, custom domains, CLI login and the Git-import alternative using `deploy/zerops/import.yaml`.

## Build from source

Use Go **1.25 or newer**, Node **22.6 or newer**, npm and Make:

```sh
make build-all             # build the web UI and both Go executables
make test                  # Go unit/handler tests
# All Docker-backed integration packages:
go test -tags integration -count=1 -timeout 30m ./...
```

A Go-only `make build` without built UI assets serves a “UI not built” page. See [UI development](ui/README.md), the [verification guide](docs/verification.md) and the [end-to-end suite guide](test/e2e/README.md) for additional checks. End-to-end tests run against your own disposable Zerops deployment, create real resources and incur costs.

## Documentation index

These guides belong to a complete repository checkout. Keep `docs/` beside
`api/`, `ui/` and this root README when copying them: relative links to the
OpenAPI specification, architecture decisions and UI development instructions
are repository paths, not a standalone documentation-bundle layout.

- [Quick start](docs/quickstart.md), [installation](docs/install.md), [control-plane upgrades](docs/upgrade.md), [architecture](docs/architecture.md) and [architecture decisions](docs/adr/README.md)
- [Namespaces and relay lifecycle](docs/user-guide/namespaces.md)
- MongoDB: [replica sets](docs/user-guide/replica-sets.md) and [sharding](docs/user-guide/sharding.md)
- [RabbitMQ clusters, access, TLS and definitions backups](docs/user-guide/rabbitmq.md)
- [Elasticsearch and the Elastic Stack](docs/user-guide/elasticsearch.md): topologies, connections, native access, snapshots and maintenance
- MongoDB: [backups, PITR, restore and clone](docs/user-guide/backups-and-restore.md), [scaling and upgrades](docs/user-guide/scaling-and-upgrades.md), [database users, TLS and access](docs/user-guide/users-tls-access.md)
- [Monitoring](docs/user-guide/monitoring.md)
- [Web UI](docs/user-guide/web-ui.md), [RBAC and authentication](docs/user-guide/rbac-and-auth.md), [REST API](docs/user-guide/api.md), [CLI reference](docs/user-guide/cli.md)
- [Day-2 operations and recovery](docs/operations.md), [security and vulnerability reporting](docs/security.md), [limitations](docs/limitations.md), [verification matrix](docs/verification.md)
- [Contributing](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md)

## Acknowledgements

Thank you to **Percona** for [Percona Server for MongoDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) and [Percona Backup for MongoDB](https://docs.percona.com/percona-backup-mongodb/latest/). Karakoram installs both from Percona's official package repositories at [repo.percona.com](https://repo.percona.com/). The database engine, replication, backups and point-in-time recovery come from their open-source work; Karakoram coordinates those capabilities rather than reinventing them.

**[OpenEverest](https://openeverest.io)** is the inspiration for this project. Originally Percona Everest, it is an Apache-2.0 CNCF Sandbox project whose model of self-hosted, provider-based database orchestration shaped Karakoram: Provider/Instance concepts, version bundles, topology schemas, backup storages, PITR/restore flows and RBAC. Karakoram brings that idea to Zerops rather than Kubernetes. It is an independent implementation; no OpenEverest code or UI is copied into this repository. We are grateful for the ideas and the work that made them tangible.

**The name is an homage.** Everest is the highest peak; Karakoram is the range that holds K2 and other giants. It is a nod to OpenEverest and to growing beyond a single peak, with room for more engines and backends in the future—not a claim that those are implemented today.

## License and trademarks

Karakoram is licensed under the [Apache License 2.0](LICENSE); see [NOTICE](NOTICE). Dependencies retain their own licenses and notices.

Percona Server for MongoDB and Percona Backup for MongoDB are Percona products and trademarks; OpenEverest, MongoDB and Zerops names and marks belong to their respective owners, and Karakoram is independent and is not affiliated with or endorsed by any of them.
