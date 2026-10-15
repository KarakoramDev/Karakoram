<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Quick start: Karakoram on Zerops in 10 minutes

The shortest path to a running Karakoram and your first database, all from the
Zerops dashboard: no checkout, no local build and no CLI required. For custom
service names, custom domains, the CLI and every option, see the full
[installation guide](install.md).

You need a Zerops account and a project to install into. A LIGHT project is
enough; the databases you create later run as extra services in the same project.

## 1. Create a Zerops token

In Zerops, open **Settings → Access Tokens Management** and create a token with
**Custom access per project → Full access** for your project only. Copy the
value; it is shown once. Karakoram uses it to create and manage database
services in that project.

## 2. Import Karakoram

In your project, choose **Import services** and paste this YAML. Put your token
between the quotes on the `KARAKORAM_ZEROPS_TOKEN` line first:

```yaml
#yamlPreprocessor=on
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
      KARAKORAM_ZEROPS_TOKEN: "paste-your-token-here"
    verticalAutoscaling:
      cpuMode: SHARED
      minCpu: 1
      maxCpu: 1
      minRam: 0.5
      maxRam: 1
      minDisk: 1
      maxDisk: 2
```

This is the same file as [`deploy/zerops/import.yaml`](../deploy/zerops/import.yaml).
It creates the control plane (`karakoram`) and its state volume
(`karakoramdata`), and builds Karakoram from the public repository. The first
build takes a few minutes; wait until `karakoram` is **Active**.

Don't paste the token into anything else, and delete any file you saved it in.
Keep the generated `KARAKORAM_MASTER_KEY`: it encrypts Karakoram's stored
credentials.

## 3. Sign in

1. Open `karakoram` → **Public access** and follow its `…zerops.app` HTTPS
   subdomain.
2. In `karakoram` → **Environment variables → Secret variables**, read
   `KARAKORAM_ADMIN_PASSWORD`.
3. Sign in as `admin` with that password. On **Set a new password**, choose a
   new one (at least 12 characters, different from the generated one), then
   sign in again with it.

## 4. Add backup storage (recommended)

Backups need an S3 bucket. The quickest is a Zerops **Object Storage** service in
the same project. Create one, then in Karakoram go to **Settings → Backup
storages** and fill in the form:

| Field | Value |
| --- | --- |
| Name | any short name, e.g. `backups` (2–63 lowercase letters, digits or hyphens) |
| Endpoint URL | the Object Storage API URL, e.g. `https://storage-prg1.zerops.io` |
| Region | `us-east-1` |
| Bucket | the bucket name from the Object Storage service |
| Access key ID / Secret access key | the keys from the Object Storage service's access details |

Leave the other fields at their defaults and choose **Create**, then **Verify** on
the new row; it should report read, write and delete access. You can skip this
step and add storage later, but the cluster has no backups until you do.

## 5. Create your first database

Choose **New cluster**. The wizard walks through **Basics → Topology → Resources
→ Backups → Access & TLS → Review**:

- **Basics:** a cluster name (2–12 lowercase letters or digits, starting with a
  letter, e.g. `orders1`) and the engine: **Percona Server for MongoDB**,
  **RabbitMQ** or **Elasticsearch**.
- **Topology:** the default is 3 members/brokers/nodes for availability; 1 is
  fine for a trial (no failover).
- **Backups:** select the storage from step 4 and add a schedule (the default is
  daily at 02:00 UTC, keeping 7).
- Keep the other suggested defaults (advertised version, TLS required, no public
  access), review and create.

A small cluster is usually ready in 5–15 minutes; the cluster page shows
progress. When it is **Ready**, the **Connection** section on its Overview shows
the endpoint; **Reveal credentials** shows the user and password, and
**Download ca.pem** gives the CA certificate your application needs. **Backup
now** takes a first backup straight away.

Your application connects from a service **in the same Zerops project** over the
private network. Engine guides with connection examples:
[MongoDB](user-guide/replica-sets.md), [RabbitMQ](user-guide/rabbitmq.md),
[Elasticsearch](user-guide/elasticsearch.md). More on backups and restore:
[backups and restore](user-guide/backups-and-restore.md).

## What next

- Install the [`karactl` CLI](install.md#install-karactl) to script everything
  you just did.
- [Upgrade Karakoram](upgrade.md) when a new release ships; never re-import the
  YAML over an existing installation.
- Read the [limitations](limitations.md) before putting production data on it.
- Something stuck? See [troubleshooting](install.md#troubleshooting).
