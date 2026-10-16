<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Karakoram

Karakoram is self-hosted MongoDB, RabbitMQ and Elasticsearch orchestration inside your Zerops projects, without Kubernetes or a hosted coordinator. A Go control plane provides a REST API, `karactl` CLI and web UI; Zerops supplies infrastructure, while you operate the installation, credentials, data and recovery procedures.

## Engines

Versions below are catalog defaults; each engine section lists supported versions and upgrade paths.

| Engine | Distribution/version | Topologies | Backups | Section |
| --- | --- | --- | --- | --- |
| MongoDB | Percona Server for MongoDB 8.0.32-14; Percona Backup for MongoDB (PBM) 2.16.0-1 | Replica sets; sharded clusters | Logical/physical; point-in-time recovery (PITR) | [MongoDB](docs/mongodb/README.md) |
| RabbitMQ | RabbitMQ 4.3.6 | 1/3/5/7 brokers | Definitions only; no messages or PITR | [RabbitMQ](docs/rabbitmq/README.md) |
| Elasticsearch | Elasticsearch 9.5.5; optional same-version Kibana, Logstash and APM Server | Combined nodes; dedicated masters | Native S3 snapshots | [Elasticsearch](docs/elasticsearch/README.md) |

## Across engines

- Catalog-driven creation, resource configuration, disk growth and capability-gated [cluster operations](docs/clusters.md).
- S3-compatible [storage, schedules and recovery](docs/backups.md).
- Multi-project [namespaces and authenticated relays](docs/namespaces.md).
- [Monitoring](docs/monitoring.md), logs and actor-attributed [audit activity](docs/operations.md#reviewing-audit-activity).
- Local accounts, OIDC, namespace-aware RBAC and owner-scoped API tokens: [access control](docs/access-control.md).

**Warning:** Review [critical limits](docs/limitations.md) before production use: replica count does not guarantee independent hosts, Local Storage is single-host, and the control plane has one replica.

## Quick start

Use the dashboard-only [Quick start](docs/quickstart.md), or the [installation reference](docs/install.md) for local deployments, custom names and CLI setup.

## Build from source

Requires Go **1.25+**, Node **22.6+**, npm and Make:

```sh
make build-all             # web UI and both Go executables
make test                  # Go unit/handler tests
# Docker-backed integration packages:
go test -tags integration -count=1 -timeout 30m ./...
```

A Go-only `make build` without UI assets serves a “UI not built” page. See [UI development](ui/README.md), [Contributing](CONTRIBUTING.md) and the [end-to-end suite](test/e2e/README.md); deployment tests create billable Zerops resources.

## Documentation index

Keep `docs/` beside the repository's `api/`, `ui/` and root README so relative source/reference links resolve.

- **Getting started:** [Overview](README.md) · [Quick start](docs/quickstart.md) · [Install](docs/install.md) · [Upgrade](docs/upgrade.md)
- **Control plane:** [Architecture](docs/architecture.md) · [Clusters and operations](docs/clusters.md) · [Backups and storage](docs/backups.md) · [Namespaces and relays](docs/namespaces.md) · [Monitoring and events](docs/monitoring.md) · [Access control](docs/access-control.md) · [Security](docs/security.md) · [Operations and troubleshooting](docs/operations.md) · [Limitations](docs/limitations.md)
- **Interfaces:** [Web UI](docs/web-ui.md) · [CLI](docs/cli.md) · [REST API and SDK](docs/api.md)
- **MongoDB:** [Overview](docs/mongodb/README.md) · [Replica sets](docs/mongodb/replica-sets.md) · [Sharding](docs/mongodb/sharding.md) · [Users, roles and TLS](docs/mongodb/access-and-tls.md) · [Backups and PITR](docs/mongodb/backups.md) · [Operations](docs/mongodb/operations.md)
- **RabbitMQ:** [Overview](docs/rabbitmq/README.md) · [Access and TLS](docs/rabbitmq/access-and-tls.md) · [Definitions backups](docs/rabbitmq/backups.md) · [Operations](docs/rabbitmq/operations.md)
- **Elasticsearch:** [Overview](docs/elasticsearch/README.md) · [Kibana, Logstash and APM](docs/elasticsearch/stack.md) · [Access and security](docs/elasticsearch/access-and-security.md) · [Snapshots](docs/elasticsearch/snapshots.md) · [Operations](docs/elasticsearch/operations.md)
- **Reference:** [Architecture decisions](docs/adr/README.md) · [Verification](docs/verification.md) · [Contributing](CONTRIBUTING.md) · [Code of Conduct](CODE_OF_CONDUCT.md)

## Acknowledgements

Karakoram installs [Percona Server for MongoDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) and [Percona Backup for MongoDB](https://docs.percona.com/percona-backup-mongodb/latest/) from [Percona's official repositories](https://repo.percona.com/). It independently implements orchestration; no OpenEverest code or UI is copied into this repository.

## License and trademarks

Karakoram uses the [Apache License 2.0](LICENSE); see [NOTICE](NOTICE). Dependencies retain their licenses and notices. Percona Server for MongoDB and Percona Backup for MongoDB are Percona products and trademarks. OpenEverest, MongoDB and Zerops names and marks belong to their respective owners; Karakoram is independent and not affiliated with or endorsed by them.
