<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Quick start

Choose a backend, install the control plane, then create a cluster and back it up. See [Backends](backends.md) for capability differences and [Operating costs](operating-costs.md) for costs.

| Backend | Quick-start path |
| --- | --- |
| Zerops — reference backend | Follow the dashboard-only steps below; no checkout or CLI required. |
| Dokploy | Follow [Install the control plane on Dokploy](dokploy.md#install-the-control-plane-on-dokploy), then use the engine guides linked below. |
| Nomad | Follow [Install the control plane on Nomad](nomad.md#install-the-control-plane-on-nomad), then use the engine guides linked below. |

The steps below use the **Zerops backend**. You need a Zerops account and existing project. LIGHT hosts the control plane; clusters need **additional capacity**. Check engine sizing below and the wizard's total against project capacity. Other Zerops deployment paths: [Install](install.md).

## 1. Create a Zerops token

Open **Settings → Access Tokens Management**. Choose **Custom access per project → Full access** for this project only, with **sudo mode off**. Save the one-time token securely; see [Security](security.md) for the privilege boundary.

## 2. Import Karakoram

Choose **Import services** in the project. Copy this template, then replace the empty `KARAKORAM_ZEROPS_TOKEN` with your token before importing:

```yaml
#yamlPreprocessor=on
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
# Set buildFromGit to your fork's public URL when deploying a fork, and set
# zeropsSetup to karakoram-source: the default setup deploys the upstream release.
# To deploy a local checkout, use import-local.yaml and install.sh.
services:
  - hostname: karakoramdata
    type: local-storage:single@1
    verticalAutoscaling:
      minDisk: 5
      maxDisk: 5
  - hostname: karakoram
    type: ubuntu@24.04
    minContainers: 1
    maxContainers: 1
    enableSubdomainAccess: true
    buildFromGit: https://github.com/KarakoramDev/Karakoram
    zeropsSetup: karakoram
    envSecrets:
      KARAKORAM_MASTER_KEY: <@generateRandomString(<48>)>
      KARAKORAM_ADMIN_PASSWORD: <@generateRandomString(<24>)>
      # Replace before import, or set in the service GUI and restart after import.
      # Zerops reserves ZEROPS_* for system variables.
      KARAKORAM_ZEROPS_TOKEN: ""
    verticalAutoscaling:
      cpuMode: SHARED
      minCpu: 1
      maxCpu: 1
      minRam: 0.5
      maxRam: 1
      minDisk: 1
      maxDisk: 2
```

This is [`deploy/zerops/import.yaml`](../deploy/zerops/import.yaml). Default `karakoram` downloads/verifies the signed release pinned on `main` (seconds); `zeropsSetup: karakoram-source` builds the exact commit (minutes). Wait for **Active**. Delete token-bearing YAML; preserve the generated master key separately.

## 3. Sign in

1. Open **karakoram → Public access** and its HTTPS subdomain.
2. Read `KARAKORAM_ADMIN_PASSWORD` under **Environment variables → Secret variables**.
3. Sign in as `admin`, set a different password of at least 12 characters, then sign in again.

## 4. Add storage

**Storage is required before any backup.** Create an S3 bucket, for example using Zerops Object Storage. Open **Settings → Backup storages**, then **Create**:

| Field | Value |
| --- | --- |
| Name | e.g. `backups`; 2–63 lowercase letters, digits or hyphens |
| Endpoint URL | Object Storage endpoint shown in Zerops |
| Region | `us-east-1` for Zerops storage |
| Bucket | Your bucket name |
| Access key ID / Secret access key | Object Storage access details |

Keep other defaults. Choose **Verify** and confirm read/write/delete access. See [Backups and storage](backups.md) for other endpoints and retention rules.

## 5. Create a cluster

Choose **New cluster**, select the engine and name, then:

| Engine | Wizard choices for a first cluster | Next page |
| --- | --- | --- |
| MongoDB | **Percona Server for MongoDB → replica set**; 1 member only for disposable tests, or 3 voters for quorum-based failover when placement permits. Host separation is [not guaranteed](limitations.md#zerops-failure-domains-and-storage). | [Create and size](mongodb/README.md#create) |
| RabbitMQ | **RabbitMQ**; 1 broker for disposable tests or 3 brokers. | [Create and configure](rabbitmq/README.md#create-and-configure) |
| Elasticsearch | **Elasticsearch → Combined master and data nodes**; `dev` preset for disposable tests or `prod` for three combined nodes. | [Create and size](elasticsearch/README.md#create) |

Review member sizing and total project capacity. Select backup storage; keep the advertised version, TLS required and public access off. Review and create; wait for **Ready**.

## 6. Take a backup

Open **Cluster → Backups → Backup now**. This is asynchronous: follow the operation and wait until backup inventory reports **Succeeded**. If storage was skipped, add it before retrying.

- [MongoDB](mongodb/backups.md): Percona Backup for MongoDB (PBM) logical or physical backup.
- [RabbitMQ](rabbitmq/backups.md): definitions only—no messages or point-in-time recovery (PITR).
- [Elasticsearch](elasticsearch/snapshots.md): native snapshot.

## 7. Connect an application

Use the private network from an application service in the same project. **Revealed credentials are the engine's administrative identity, not application credentials.** Create a scoped application identity and deliver its credentials, endpoints and CA through runtime secrets/configuration:

- [MongoDB users, roles and TLS](mongodb/access-and-tls.md)
- [RabbitMQ access and TLS](rabbitmq/access-and-tls.md)
- [Elasticsearch access and security](elasticsearch/access-and-security.md)

## 8. Connect an AI agent

The control plane serves a read-only [MCP server](mcp.md) at `/mcp` out of the box. Mint a restricted token with `karactl mcp config --client claude-code|cursor|vscode|codex` and paste the printed configuration into your client; the [MCP quick start](mcp-quickstart.md) covers each client, write access and per-cluster [data MCP](data-mcp.md).

## What next

- [CLI](cli.md), [cluster operations](clusters.md) and [Backups and storage](backups.md).
- [Upgrade](upgrade.md) existing installations; never re-import the installer.
- [Limitations](limitations.md) before production; [Operations](operations.md) for troubleshooting.
