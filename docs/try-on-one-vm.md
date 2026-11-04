<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Try Karakoram on one VM

This page takes one empty Linux VM to a running PostgreSQL cluster that you query with `psql`, using the **v0.4.3** release and [Dokploy](https://dokploy.com/) as the provider. It is an evaluation setup: one host, no redundancy, no backups. Delete the VM when you are done.

Read this first:

- The Dokploy provider is **experimental** for every engine, and PostgreSQL on Dokploy was not part of the v0.4.3 acceptance runs ([Providers](providers.md), [v0.4.3 known issues](https://github.com/KarakoramDev/Karakoram/releases/tag/v0.4.3#known-issues-and-continuing-limits)).
- Everything here uses the v0.4.3 tag. Documentation on `main` can describe changes that are not released yet.
- Measured on a fresh 4 vCPU / 8 GB VM on 2026-11-04: Dokploy installed in 3 min 11 s, the control plane built and started in 35 s, and the PostgreSQL cluster went from **Create** to **Ready** in 4 min 34 s. From the VM's first boot to a Ready cluster took 11 min 5 s with the clicks scripted; working through the screens by hand adds your own time. That run used an Alpine-based Docker VM, not Ubuntu, and TLS came from the hosting platform instead of Let's Encrypt.

## What you need

| | |
| --- | --- |
| VM | Ubuntu 24.04, amd64, 4 vCPU, 8 GB RAM, 40 GB or more disk, a public IPv4 address. The test run used about 6 GB of disk. |
| Network | Inbound TCP 22, 80 and 443. Port 3000 only until Dokploy has its own domain. Outbound HTTPS to GitHub, Docker Hub, Ubuntu and Percona package mirrors. |
| DNS | A name for the Karakoram UI that points at the VM. Without your own domain, `<anything>.<ip-with-dashes>.sslip.io` works, for example `karakoram.203-0-113-10.sslip.io`. |
| Cost | The VM for the hours you keep it. Nothing else is billed. |

## 1. Install Dokploy v0.30.8

Karakoram v0.4.3 was tested against Dokploy v0.30.8, so pin it:

```sh
curl -sSL https://dokploy.com/install.sh | sudo DOKPLOY_VERSION=v0.30.8 sh
```

The installer sets up Docker, a single-node Docker Swarm, Traefik and Dokploy. It needs `openssl` on the host to generate Dokploy's secrets; Ubuntu has it. Open `http://<vm-ip>:3000` and register the Dokploy administrator.

## 2. Prepare Dokploy for Karakoram

1. **Project:** **Projects → Create Project**, name `karakoram`. Dokploy opens its `production` environment; the environment ID is the last part of the address bar (`.../environment/<environment-id>`).
2. **API key:** **Settings → Profile → API/CLI Keys → Generate New Key**. Name it `karakoram`, pick your organization and leave **Enable Rate Limiting** off: Karakoram polls Dokploy continuously and a rate-limited key stops working within a day. Copy the key; it is shown once.
3. **Overlay network:** on the VM, create an attachable overlay network for Karakoram's members:

   ```sh
   sudo docker network create --driver overlay --attachable karakoram
   ```

## 3. Deploy the control plane

In the `karakoram` project, create an application named `karakoram`:

1. **Source:** Git, repository `https://github.com/KarakoramDev/Karakoram.git`, branch `v0.4.3`, build path `/`. Build type **Dockerfile**, Dockerfile path `deploy/dokploy/Dockerfile`, context `.`. The image downloads the signed v0.4.3 release and checks its signature and checksum.
2. **Advanced:**
   - Volumes: named volume `karakoram-data` mounted at `/var/lib/karakoram`.
   - Swarm settings, update and rollback config: `{"Parallelism": 1, "Order": "stop-first"}`.
   - Swarm settings, network: `[{"Target": "karakoram", "Aliases": ["karakoram"]}, {"Target": "dokploy-network"}]`.
   - No placement rule and no registry are needed on a single node.
3. **Environment:** turn off **Create .env file**. Generate the master key with `openssl rand -hex 24` and keep a copy outside the VM; losing it makes the stored secrets unreadable.

   ```sh
   KARAKORAM_MASTER_KEY=<48 hex characters>
   KARAKORAM_ADMIN_PASSWORD=<initial admin password>
   KARAKORAM_PUBLIC_URL=https://karakoram.203-0-113-10.sslip.io
   KARAKORAM_ADVERTISE_URL=https://karakoram:9443
   KARAKORAM_DOKPLOY_URL=http://dokploy:3000
   KARAKORAM_DOKPLOY_API_KEY=<API key from step 2>
   KARAKORAM_DOKPLOY_ENVIRONMENT_ID=<environment ID from step 2>
   KARAKORAM_DOKPLOY_NETWORK=karakoram
   ```

4. **Domains:** host `karakoram.203-0-113-10.sslip.io`, container port **8080**, HTTPS on, certificate Let's Encrypt. Do not publish port 9443.
5. **Deploy.** Wait until `https://<your-host>/healthz` returns `200`.

## 4. Sign in

Open `KARAKORAM_PUBLIC_URL`, sign in as `admin` with the initial password, choose a new password of at least 12 characters, and sign in again.

## 5. Create a PostgreSQL cluster

**Clusters → + New cluster.** On **Basics** enter a name (2–12 lowercase letters and digits, for example `hello`), engine **PostgreSQL**, version **17.11.1**, topology **Single PostgreSQL member**, preset **dev**. Click **Continue** through Topology, Resources, Backups (no storage needed for this test) and Access & TLS, check **Review**, then **Create cluster**.

The cluster page shows **Creating**, then **Updating**, then **Ready** with two healthy members: the database (`hellop0`) and its ProxySQL endpoint (`hellopx`). The yellow "Members share a placement group" warning is expected on one VM.

## 6. Connect with psql

On the cluster's **Overview**, the **Connection** panel shows the private endpoint `postgresql://hellopx:5432/postgres?sslmode=verify-full`. Click **Reveal credentials** for the password and save the CA certificate as `ca.pem` on the VM. The endpoint is reachable only on the `karakoram` network, so run `psql` from a container attached to it:

```sh
sudo docker run --rm -it --network karakoram -v "$PWD/ca.pem:/ca.pem:ro" postgres:17 \
  psql "postgresql://postgres:<password>@hellopx:5432/postgres?sslmode=verify-full&sslrootcert=/ca.pem"
```

```sql
create table hello(id int primary key, note text);
insert into hello values (1, 'written through ProxySQL');
select * from hello;
```

The revealed `postgres` user is the cluster's superuser. Use it only for this test; for applications create a restricted role ([PostgreSQL access and TLS](postgres/access-and-tls.md)).

## 7. Clean up

Delete the cluster in the UI if you want to watch Karakoram remove its runtimes and volumes, then delete the VM. Nothing outside the VM was created.

## Where to go next

- [Dokploy provider](dokploy.md): multi-node Swarms, registries, public TCP and HTTPS for engines.
- [PostgreSQL](postgres/README.md): HA with Patroni, extension profiles, backups and PITR (needs S3-compatible storage).
- [Limitations](limitations.md) before you put anything real on it.
