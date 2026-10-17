<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Dokploy backend (experimental)

The `dokploy` backend runs Karakoram clusters on a [Dokploy](https://dokploy.com)-managed Docker Swarm that you operate. It is **experimental**: it passes the shared backend conformance suite and was exercised with MongoDB, RabbitMQ and Elasticsearch on a three-node test Swarm, but it has no production track record. Zerops remains the reference backend. Design rationale: [ADR 0032](adr/0032-dokploy-backend.md).

[PostgreSQL](postgres/README.md) and [Supabase](supabase/README.md) use the same backend-neutral runtime contract and remain experimental on Dokploy; this is not an acceptance claim for these engines. Members need access to locked native artifacts (including an operator mirror for source-built development entries) and registered S3 destinations. Public PostgreSQL TCP has no backend CIDR enforcement and needs explicit allow-all acknowledgement plus an external firewall. Supabase gateway public HTTPS uses the backend's domain capability; unavailable component artifacts are still rejected.

## How it maps

|Karakoram|Dokploy / Swarm|
|---|---|
|Namespace target (`projectId`)|One Dokploy **environment** ID|
|Member runtime|One Dokploy application named after the member hostname (Dokploy suffixes the Swarm service name); one replica, stop-first updates and rollbacks, 120 s stop grace period|
|Member DNS name|A network alias equal to the member hostname on the target's IPv4 overlay network; private services use DNSRR task addresses so peer certificate source-address verification sees matching DNS|
|Persistent volume|A Docker named volume on one node, owned by a "volume holder" application that is never started and gives the volume its own ID|
|Deployment|Karakoram uploads a build archive (Dockerfile + the release `karakoram` archive); Dokploy builds the image on the manager and pushes it to your registry; workers pull it|
|Anti-affinity|Volumes of one replica set (MongoDB) or cluster (RabbitMQ, Elasticsearch) are pinned to distinct Swarm nodes while enough nodes exist; runtimes follow their volume|
|Public TCP|Swarm ingress-published ports (routing mesh); exposed services switch to VIP endpoint mode, returning to DNSRR after port removal|
|Public HTTPS (Kibana, RabbitMQ management)|Dokploy domains served by Dokploy's Traefik|

Docker is used on this backend only because Dokploy runs Swarm services. Each member image is a pinned Ubuntu 24.04 base with the same package installation steps as on Zerops, and the engine processes run under the same `karakoram agent` binary as everywhere else. There is no Docker-in-Docker and no Docker socket in member containers.

Engine prepare commands use exec-form Dockerfile `RUN` instructions invoking Bash with `pipefail`; embedded newlines, shell quoting and heredocs are preserved rather than parsed as Dockerfile instructions.

## Capabilities

|Capability|Dokploy|Notes|
|---|---|---|
|`publicTcp`|yes|Ingress mode: every Swarm node forwards the port. Status and TLS names use the target's `publicIPv4`/`publicIPv6` setting.|
|`tcpAllowlist`|no|No source-address filtering. Public TCP therefore requires an allowlist that covers the whole address family plus `allowAllAddresses: true`; restrict access with your own firewall in front of the nodes.|
|`publicHttps`|yes, with `domainSuffix`|Dokploy domains `<service>-<port>-<target hash>.<domainSuffix>`; needs wildcard DNS to the Traefik node(s).|
|`diskGrowth`, `volumeQuota`|no|Docker local volumes live on the node filesystem without a size limit; size requests are not enforced and disk growth is rejected. Monitor node disks.|
|`placement`|yes|Authoritative node per member (`placementGroup` = Swarm node hostname).|
|`logs`|yes|Platform logs through Dokploy.|
|`horizontalScaling`|yes|Stateless components (Kibana, Logstash, APM, mongos) scale by replicas.|
|`scopedTokens`|no|Dokploy API keys inherit the user's rights; a member user restricted to one environment cannot read the registry or nodes this backend needs. Use an admin key of a Dokploy installation dedicated to Karakoram.|
|`exec`, `metrics`, `dedicatedCpu`, `envChangeRestarts`|no|Variable writes apply at the next restart.|

## Requirements

- Dokploy **v0.30.8** (the tested version) on an amd64 Docker Swarm; the API is unversioned, so test upgrades first.
- For more than one node: a container registry configured in Dokploy (Settings → Registry) that every node can pull from. Single-node Swarms may omit it.
- Outbound HTTPS from the manager during builds (Ubuntu, Percona, RabbitMQ and Elastic package sources), from every node to Docker Hub (volume deletion runs a one-shot task from the pinned `ubuntu:24.04` digest on the volume's node), and from members to your S3 storage.
- Elasticsearch: `vm.max_map_count=262144` on every node (`/etc/sysctl.d`).
- Enough free disk on every node for volumes and images (each engine image is roughly 0.3–1.7 GB).
- An overlay network for Karakoram, normally a dedicated attachable one (the backend creates it through Dokploy when it is missing).

Optional node label: if any node carries `karakoram.data=true`, only labelled nodes receive data volumes (`docker node update --label-add karakoram.data=true <node>`).

## Target settings

|Config key|Environment variable (default namespace)|Meaning|
|---|---|---|
|`url`|`KARAKORAM_DOKPLOY_URL`|Dokploy origin, e.g. `https://dokploy.example.com` (no path)|
|—|`KARAKORAM_DOKPLOY_API_KEY`|API key (write-only token input for other namespaces)|
|`projectId` (namespace field)|`KARAKORAM_DOKPLOY_ENVIRONMENT_ID`|Dokploy environment ID|
|`network`|`KARAKORAM_DOKPLOY_NETWORK`|Attachable overlay network for member aliases; default `karakoram-<8 hex characters of the environment ID's SHA-256>`, created through Dokploy when missing|
|`registryId`|`KARAKORAM_DOKPLOY_REGISTRY_ID`|Dokploy registry used for built images|
|`publicIPv4`, `publicIPv6`|`KARAKORAM_DOKPLOY_PUBLIC_IPV4`, `_IPV6`|Address clients use for public TCP|
|`domainSuffix`|`KARAKORAM_DOKPLOY_DOMAIN_SUFFIX`|Wildcard DNS suffix for public HTTPS|
|`certificateType`|`KARAKORAM_DOKPLOY_CERTIFICATE_TYPE`|`letsencrypt` (default), `none` (Traefik default certificate) or `custom`|

**URL restriction.** A namespace's Dokploy URL receives its API key, so the control plane only talks to approved origins: the operator's `KARAKORAM_DOKPLOY_URL`, entries of `KARAKORAM_DOKPLOY_ALLOWED_URLS` (comma-separated origins), and — only while `KARAKORAM_DOKPLOY_ALLOWED_URLS` is unset — any `https://` origin whose host is a name or a public IP address (loopback, private, link-local and shared-address literals and `localhost` need an explicit allowlist entry). Production installations should set the allowlist. Redirects are never followed. Namespace settings cannot be changed after creation.

**API key rate limit.** Dokploy creates API keys with rate limiting enabled by default (in v0.30.8: 10 requests per 24 hours). Karakoram polls continuously, so create the key with rate limiting disabled (API: `user.createApiKey` with `rateLimitEnabled: false`). A namespace whose token suddenly turns invalid with HTTP 401 has usually hit this limit.

**Network isolation.** Member hostnames are DNS aliases on the configured network. Every application attached to that network can resolve and reach them, and two namespaces sharing a network could collide on names (for example the relay hostname), so the default network is per environment. Avoid `dokploy-network`, which every Dokploy application joins. Only services with public HTTPS enabled are additionally attached to `dokploy-network`, so Traefik can route to them; enabling or disabling public HTTPS therefore restarts that service. Engine authentication and the agent token still guard every port; the network is not an access-control boundary.

## Install the control plane on Dokploy

The control plane runs as one Dokploy application with a persistent volume; its default namespace manages one Dokploy environment directly over the shared overlay network.

1. In Dokploy create a project (for example `karakoram`) and, for clusters, a project or environment for the default namespace; note its environment ID (Dokploy UI URL or `GET /api/project.all`). Create an admin API key with rate limiting disabled (see [API key rate limit](#target-settings)) and, for multi-node Swarms, a registry. Before deploying the control plane, create its dedicated **attachable overlay** network (for example `karakoram`, via Dokploy's network settings or `POST /api/network.create` with `{"name":"karakoram","driver":"overlay","attachable":true}`); its Swarm service needs the network before the backend can start.
2. Create an application `karakoram` with build type **Dockerfile**, Dockerfile path `deploy/dokploy/Dockerfile` and context `.`, from this repository (Git provider) — it downloads and verifies the signed release pinned in `deploy/zerops/RELEASE`. To deploy an unreleased build, upload a zip containing that Dockerfile as **`Dockerfile` at the zip root** and a linux `karakoram` binary alongside it (Drop source); set Dockerfile path **`Dockerfile`**, context `.`, and build argument `KARAKORAM_SOURCE=local`.
3. Advanced settings:
   - Volume mount: named volume (for example `karakoram-data`) at `/var/lib/karakoram`.
   - Swarm placement: pin to the Dokploy manager node (`node.hostname==<manager>`): the named volume is node-local, and the control plane then talks to Dokploy's plain-HTTP service URL without leaving the host (Swarm overlay traffic between nodes is unencrypted). If it must run elsewhere, point `KARAKORAM_DOKPLOY_URL` at Dokploy's HTTPS domain instead.
   - Environment settings: turn off **Create .env file**, so Dokploy does not write the control plane's secrets into the build directory.
   - Swarm update and rollback config: `{"Parallelism": 1, "Order": "stop-first"}` — SQLite allows one writer.
   - Swarm network: `[{"Target": "<network>", "Aliases": ["karakoram"]}, {"Target": "dokploy-network"}]`. Members reach the control plane as `https://karakoram:9443`; `dokploy-network` lets Traefik and the control plane reach Dokploy.
   - Registry: your registry (multi-node).
4. Environment:

```sh
KARAKORAM_MASTER_KEY=<48 random characters; keep a separate backup>
KARAKORAM_ADMIN_PASSWORD=<initial admin password>
KARAKORAM_PUBLIC_URL=https://karakoram.example.com
KARAKORAM_ADVERTISE_URL=https://karakoram:9443
KARAKORAM_DOKPLOY_URL=http://dokploy:3000
KARAKORAM_DOKPLOY_API_KEY=<API key>
KARAKORAM_DOKPLOY_ENVIRONMENT_ID=<environment ID>
KARAKORAM_DOKPLOY_REGISTRY_ID=<registry ID>
KARAKORAM_DOKPLOY_NETWORK=<network>
KARAKORAM_DOKPLOY_PUBLIC_IPV4=<public address of a Swarm node>
KARAKORAM_DOKPLOY_DOMAIN_SUFFIX=apps.example.com
```

   `http://dokploy:3000` is Dokploy's own service on `dokploy-network`; because it is the operator-set `KARAKORAM_DOKPLOY_URL`, it is accepted although it is plain HTTP. The image sets `KARAKORAM_BACKEND=dokploy` and `KARAKORAM_DATA_DIR=/var/lib/karakoram`.
   For a lab using Traefik's default certificate, also set `KARAKORAM_DOKPLOY_CERTIFICATE_TYPE=none` and use certificate type `none` for the control-plane domain; clients must explicitly disable TLS verification. For private or nonstandard-port S3 storage, set `KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST` to the endpoint's `host:port` (for example `minio.internal:9000`), without a URL scheme; see [Backups and storage](backups.md).
5. Domains: add `karakoram.example.com` → port **8080**, HTTPS on. Traefik forwards WebSocket upgrades, which the UI and relays need. Do not publish port 9443.
6. Deploy, then sign in at `KARAKORAM_PUBLIC_URL` as `admin`, complete the required initial password change, and sign in again before creating API tokens or using `karactl login` ([API bootstrap](api.md#curl-bootstrap), [CLI](cli.md#login-and-contexts)).

Back up `KARAKORAM_MASTER_KEY` and the `karakoram-data` volume ([Backups and storage](backups.md)). Upgrades: redeploy with a newer release (`deploy/zerops/RELEASE`) or a newer local binary; stop-first update ordering keeps one writer.

## Managing Dokploy from another control plane

Additional namespaces of kind `dokploy` use the outbound [relay](namespaces.md): Karakoram deploys one relay application into the target environment, which dials the control plane's public URL. That URL must present a certificate trusted by the public CA bundle of the Ubuntu base image. Create the namespace with the target settings above, for example:

```sh
karactl namespaces create --name swarm --backend dokploy --project-id '<environment-id>' \
  --config url=https://dokploy.example.com --config registryId=<id> \
  --config network=kk-swarm --config publicIPv4=203.0.113.10 --token-file dokploy-key.txt
```

## Operations and limitations

- **Node loss = member loss.** A member is pinned to the node that holds its volume. If the node fails, the member stays pending until the node returns; replace the member through Karakoram to rebuild it elsewhere from its replica set or cluster peers.
- **Member replacement needs headroom.** Karakoram replaces a member by adding the new one before removing the old one. When the cluster already uses every eligible node, the new member is placed on the node of the member it replaces, so that node must have free CPU and memory (Swarm reservations) for one extra member during the replacement; otherwise the new task stays pending until capacity is available. Add a node or free capacity first.
- **Changing public ports restarts the member** (Dokploy applies ports with a service update).
- **Deleting a volume** wipes its contents with a one-shot task on its node and removes the holder application. Dokploy can remove Docker volume objects only on the manager; an empty volume object can remain on workers (`docker volume prune` on that node removes it).
- `keepVolumes` deletion keeps the holder application and its Docker volume; resuming the cluster reattaches it on the same node.
- Images are rebuilt by Dokploy for each deployment; the layer cache makes later members fast, but the first build of an engine version takes about one to two minutes. Use Dokploy's cleanup settings for old images.
- Variable changes are stored immediately and applied at the next restart (`envChangeRestarts: false`).
- Elasticsearch's S3 client trusts only public CAs; use a publicly trusted S3 endpoint for snapshots.

## Dokploy terms and licence

Dokploy is Apache-2.0 except the `/proprietary` directory. Its [terms](https://github.com/Dokploy/dokploy/blob/v0.30.8/TERMS_AND_CONDITIONS.md) forbid "any commercial resale or redistribution of Dokploy as a service" without explicit consent. Running Karakoram on your own Dokploy for your own databases is internal use; offering Dokploy-backed databases to third parties as a service needs Dokploy's written consent. Karakoram does not include or redistribute Dokploy.
