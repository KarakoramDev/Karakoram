<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Karakoram

Karakoram is a multi-platform, self-hosted control plane for MongoDB, RabbitMQ, Elasticsearch, Weaviate, PostgreSQL, MySQL, Percona XtraDB Cluster, Neo4j Community, modular Supabase, Keycloak, OpenBao and Temporal orchestration inside your infrastructure, without Kubernetes or a hosted coordinator. A Go control plane provides a REST API, `karactl` CLI and web UI; backends supply infrastructure, while you operate the installation, credentials, data and recovery procedures. Zerops is the reference backend. Dokploy and Nomad are also available; see [Backends](docs/backends.md) for their status.

## Engines

Versions below are catalog defaults; each engine section lists supported versions and upgrade paths.

| Engine | Distribution/version | Topologies | Backups | Section |
| --- | --- | --- | --- | --- |
| MongoDB | Percona Server for MongoDB 8.0.32-14; Percona Backup for MongoDB (PBM) 2.16.0-1 | Replica sets; sharded clusters | Logical/physical; point-in-time recovery (PITR) | [MongoDB](docs/mongodb/README.md) |
| RabbitMQ | RabbitMQ 4.3.6 | 1/3/5/7 brokers | Definitions only; no messages or PITR | [RabbitMQ](docs/rabbitmq/README.md) |
| Elasticsearch | Elasticsearch 9.5.5; optional same-version Kibana, Logstash and APM Server | Combined nodes; dedicated masters | Native S3 snapshots | [Elasticsearch](docs/elasticsearch/README.md) |
| Weaviate | Official Weaviate 1.40.1; mixed-license binary, community features only | Single; 3–9 data members with three fixed Raft voters | Native S3 full; no PITR | [Weaviate](docs/weaviate/README.md) |
| PostgreSQL | Percona PostgreSQL 17.11.1 default; majors 16/17/18; extension profiles | Single; Patroni/etcd HA (3/5); Citus distributed; ProxySQL client service | pgBackRest full/diff/incr and PITR | [PostgreSQL](docs/postgres/README.md) |
| Supabase | PostgreSQL 17 plus release-locked native services | Same database topologies; modular services | PostgreSQL data only; separate S3 object protection required | [Supabase](docs/supabase/README.md) |
| Keycloak | Official Keycloak 26.8.1; OpenJDK 21 | One native runtime with a PostgreSQL binding | Through the source PostgreSQL cluster | [Keycloak](docs/keycloak/README.md) |
| OpenBao | OpenBao 2.7.1 | 1/3/5 members; integrated Raft | Raft snapshots to S3; no PITR | [OpenBao](docs/openbao/README.md) |
| Temporal | Temporal 1.32.1 (also 1.31.4); ui-server 2.56.0 | Combined server; split stateless roles on bound PostgreSQL | Source PostgreSQL backup plus cluster-state manifest; requires binding service | [Temporal](docs/temporal/README.md) |
| MySQL | Percona Server for MySQL 8.4.11-11 default; majors 8.4/8.0 | Single; Group Replication single-primary (3/5); ProxySQL | XtraBackup full/incremental and GTID-based PITR | [MySQL](docs/mysql/README.md) |
| Percona XtraDB Cluster | PXC 8.4.11-11.1 default; majors 8.4/8.0; separate `pxc` provider | Galera cluster (3/5); one ProxySQL-routed writer | Separate PXC backup provenance; full/incremental and PITR | [PXC](docs/mysql/README.md) |
| Neo4j Community | Neo4j 2026.09.0 / 2026.10.0 (default); Temurin Java 21 | Single member only | Offline system + graph dumps; full outage; no PITR | [Neo4j](docs/neo4j/README.md) |

## Backends

Karakoram is multi-platform; a namespace selects the infrastructure backend that provisions its members.

| Backend | Role / runtime | Infrastructure and guide |
| --- | --- | --- |
| Zerops | Reference backend | Managed Ubuntu runtimes, Local Storage and public routing in existing projects; [guide](docs/zerops.md). |
| Dokploy | Docker Swarm | Applications and node-local volumes on your Dokploy-managed Docker Swarm; [guide](docs/dokploy.md). |
| Nomad | Native jobs | Native jobs and dynamic host volumes on your operator-managed Nomad cluster; [guide](docs/nomad.md). |

See [Backends](docs/backends.md) for capability differences and [Operating costs](docs/operating-costs.md) for infrastructure costs.

## Across engines

- Catalog-driven creation, resource configuration, disk growth and capability-gated [cluster operations](docs/clusters.md).
- S3-compatible [storage, schedules and recovery](docs/backups.md).
- Multi-project [namespaces and authenticated relays](docs/namespaces.md).
- [Monitoring](docs/monitoring.md), logs and actor-attributed [audit activity](docs/operations.md#reviewing-audit-activity).
- Local accounts, OIDC, namespace-aware RBAC and owner-scoped API tokens: [access control](docs/access-control.md).

**Warning:** Review [critical limits](docs/limitations.md) before production use: replica count does not guarantee independent hosts, Local Storage is single-host, and the control plane has one replica.

## Quick start

Choose a backend in [Backends](docs/backends.md). For Zerops, use the dashboard-only [Quick start](docs/quickstart.md), or the [installation reference](docs/install.md) for local deployments, custom names and CLI setup. Dokploy and Nomad installation paths are linked there.

## Build from source

Requires Go **1.26+**, Node **22.6+**, npm and Make:

```sh
make build-all             # web UI and both Go executables
make test                  # Go unit/handler tests
# Integration-tagged tests (some require Docker; PostgreSQL/Supabase use native LXC):
go test -tags integration -count=1 -timeout 30m ./...
```

A Go-only `make build` without UI assets serves a “UI not built” page. See [UI development](ui/README.md), [Contributing](CONTRIBUTING.md) and the [end-to-end suite](test/e2e/README.md); deployment tests create billable Zerops resources.

Coding agents: read [AGENTS.md](AGENTS.md) for repository boundaries and contribution commands.

## Documentation index

Keep `docs/` beside the repository's `api/`, `ui/` and root README so relative source/reference links resolve.

- **Getting started:** [Overview](README.md) · [Quick start](docs/quickstart.md) · [Install](docs/install.md) · [Backends](docs/backends.md) · [Operating costs](docs/operating-costs.md) · [Upgrade](docs/upgrade.md)
- **Control plane:** [Architecture](docs/architecture.md) · [Clusters and operations](docs/clusters.md) · [Backups and storage](docs/backups.md) · [Namespaces and relays](docs/namespaces.md) · [Zerops backend](docs/zerops.md) · [Dokploy backend](docs/dokploy.md) · [Nomad backend](docs/nomad.md) · [Monitoring and events](docs/monitoring.md) · [Access control](docs/access-control.md) · [Security](docs/security.md) · [Operations and troubleshooting](docs/operations.md) · [Limitations](docs/limitations.md)
- **Interfaces:** [Web UI](docs/web-ui.md) · [CLI](docs/cli.md) · [REST API and SDK](docs/api.md) · [MCP quick start](docs/mcp-quickstart.md) · [MCP server](docs/mcp.md) · [Cluster data MCP](docs/data-mcp.md)
- **Operator agents:** [karakoram-operator skill](skills/karakoram-operator/SKILL.md) · [Skill installation](skills/README.md)
- **Control-plane recovery:** [State backups and backend rebuild runbooks](docs/control-plane-backup.md)
- **Backup verification:** [Scheduled and manual restore drills](docs/backup-drills.md)
- **Notifications:** [Webhook, Slack and email channels, signatures and delivery guarantees](docs/notifications.md)
- **MongoDB:** [Overview](docs/mongodb/README.md) · [Replica sets](docs/mongodb/replica-sets.md) · [Sharding](docs/mongodb/sharding.md) · [Users, roles and TLS](docs/mongodb/access-and-tls.md) · [Backups and PITR](docs/mongodb/backups.md) · [Operations](docs/mongodb/operations.md)
- **RabbitMQ:** [Overview](docs/rabbitmq/README.md) · [Access and TLS](docs/rabbitmq/access-and-tls.md) · [Definitions backups](docs/rabbitmq/backups.md) · [Operations](docs/rabbitmq/operations.md)
- **Elasticsearch:** [Overview](docs/elasticsearch/README.md) · [Kibana, Logstash and APM](docs/elasticsearch/stack.md) · [Access and security](docs/elasticsearch/access-and-security.md) · [Snapshots](docs/elasticsearch/snapshots.md) · [Operations](docs/elasticsearch/operations.md)
- **Weaviate:** [Overview](docs/weaviate/README.md) · [Access and TLS](docs/weaviate/access-and-tls.md) · [Backups](docs/weaviate/backups.md) · [Operations](docs/weaviate/operations.md)
- **PostgreSQL:** [Overview](docs/postgres/README.md) · [Access and TLS](docs/postgres/access-and-tls.md) · [Backups and PITR](docs/postgres/backups.md) · [Operations](docs/postgres/operations.md)
- **Supabase:** [Overview](docs/supabase/README.md) · [Components and availability](docs/supabase/components.md) · [Operations](docs/supabase/operations.md)
- **Keycloak:** [PostgreSQL binding, routing and administrator recovery](docs/keycloak/README.md)
- **OpenBao:** [Overview](docs/openbao/README.md) · [API access and TLS](docs/openbao/access-and-tls.md) · [Raft backups and recovery](docs/openbao/backups.md) · [Operations and unseal](docs/openbao/operations.md)
- **Temporal:** [Overview and prerequisites](docs/temporal/README.md) · [Access and TLS](docs/temporal/access-and-tls.md) · [Operations and recovery](docs/temporal/operations.md)
- **MySQL and PXC:** [Overview](docs/mysql/README.md) · [Configuration](docs/mysql/configuration.md) · [Access and TLS](docs/mysql/access-and-tls.md) · [Backups and PITR](docs/mysql/backups.md) · [Operations and recovery](docs/mysql/operations.md) · [Native verification](docs/mysql/verification.md)
- **Neo4j Community:** [Overview](docs/neo4j/README.md) · [Access and TLS](docs/neo4j/access-and-tls.md) · [Offline backups](docs/neo4j/backups.md) · [Operations](docs/neo4j/operations.md)
- **Reference:** [Architecture decisions](docs/adr/README.md) · [Native artifacts](docs/native-artifacts.md) · [Verification](docs/verification.md) · [Contributing](CONTRIBUTING.md) · [Code of Conduct](CODE_OF_CONDUCT.md)

## Acknowledgements

Thank you to [Zerops](https://zerops.io) for providing free-of-charge infrastructure for developing and testing Karakoram.

Karakoram installs [Percona Server for MongoDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) and [Percona Backup for MongoDB](https://docs.percona.com/percona-backup-mongodb/latest/) from [Percona's official repositories](https://repo.percona.com/). It independently implements orchestration; no OpenEverest code or UI is copied into this repository.

## License and trademarks

Karakoram uses the [Apache License 2.0](LICENSE); see [NOTICE](NOTICE). Dependencies retain their licenses and notices. Percona Server for MongoDB and Percona Backup for MongoDB are Percona products and trademarks. OpenEverest, MongoDB and Zerops names and marks belong to their respective owners; Karakoram is independent and not affiliated with or endorsed by them.
