<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Dokploy provider

The `dokploy` provider runs Karakoram clusters on a [Dokploy](https://dokploy.com)-managed [Docker Swarm](https://docs.docker.com/engine/swarm/) that you operate. It is **experimental**: it passes the shared provider conformance suite and was exercised with [MongoDB](https://www.mongodb.com/), [RabbitMQ](https://www.rabbitmq.com/) and [Elasticsearch](https://www.elastic.co/elasticsearch) on a three-node test Swarm, but it has no production track record. [Zerops](https://zerops.io/) remains the reference provider. Design rationale: [ADR 0032](adr/0032-dokploy-backend.md).

[PostgreSQL](postgres/README.md) and [Supabase](supabase/README.md) use the same provider-neutral runtime contract and have not been accepted on Dokploy. Members need access to locked native artifacts (including an operator mirror for source-built development entries) and registered [S3](https://aws.amazon.com/s3/) destinations. Public PostgreSQL TCP has no provider CIDR enforcement and needs explicit allow-all acknowledgement plus an external firewall. Supabase gateway public HTTPS uses the provider's domain capability; unavailable component artifacts are still rejected.

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

[Docker](https://www.docker.com/) is used on this provider only because Dokploy runs Swarm services. Each member image is a pinned [Ubuntu](https://ubuntu.com/) 24.04 base with the same package installation steps as on Zerops, and the engine processes run under the same `karakoram agent` binary as everywhere else. There is no Docker-in-Docker and no Docker socket in member containers.

Engine prepare commands use exec-form Dockerfile `RUN` instructions invoking [Bash](https://cgit.git.savannah.gnu.org/cgit/bash.git/) with `pipefail`; embedded newlines, shell quoting and heredocs are preserved rather than parsed as Dockerfile instructions.

### Platform deployment admission

Dokploy queues application deployments and builds uploaded Dockerfiles on its server.
Karakoram therefore admits at most **two** in-flight deployments per Dokploy origin,
shared across all environments/namespaces in the control-plane process. Set the
control-plane environment variable `KARAKORAM_DOKPLOY_MAX_INFLIGHT_DEPLOYS` to an
integer **>= 1** to change this limit; it is not a per-namespace setting.

Admission covers archive uploads, retained-source rebuilds and volume-wipe deployments.
It reads owned applications across the server's projects/environments and their
authoritative deployment rows, adding local reservations while submission visibility
is uncertain. A queued/running/unknown deployment blocks another submission for that
application even if it was started outside Karakoram. Terminal `done`, `error` and
`cancelled` rows release capacity. Waiting returns a typed deployment wait with a
retry deadline at least 15 seconds away, for example
`waiting for a platform deployment slot: 2/2 in flight (Dokploy server)`;
it does not upload or enqueue another build.

Additional admission observations are bounded to **128 reads per attempt**, shared
by project details, application deployment lists and exact-ID corroboration of
reserved applications missing from inventory (plus one project-list request).
This is an inventory-size ceiling, not a throughput limit: one project with 128
owned applications already exceeds it, even with no builds running. Waiting for
deployments to finish or increasing the deployment limit will not resolve this
diagnostic. Reduce the total project/owned-application inventory on that Dokploy
installation (safely delete retired resources), or use a separate installation;
splitting namespaces/environments on the same origin does not help. Unavailable or
incomplete inventory fails closed. The API key must see all Karakoram applications
on the installation. This is process-wide admission, not a distributed semaphore:
independent control-plane processes and direct operator submissions must
coordinate their own combined limit.

The cap also applies during initial cluster bootstrap; there is no quorum-group
exception. Initial member deployment readiness means the agent supervisor is
running and initialising, not that the database has formed a quorum. Stateless
day-2 replacements retain native readiness when dependencies are healthy; the
engine persists that choice for stable retries. Dokploy does not map either
probe to a Docker `HEALTHCHECK`, because Swarm service
discovery must be available to bootstrapping tasks. Application/native health,
configuration acknowledgement and rolling-replacement catch-up remain enforced
by Karakoram's engine-level convergence gates. See
[member startup and admission](operations.md#deployment-admission-and-member-startup).


Synchronous Swarm `application.reload` operations reuse an existing image and do not
enter Dokploy's build queue, so they do not consume deployment slots. Existing pending
build/lost-delivery fences still protect lifecycle operations. Failed runtime builds
already require explicit operator action; a failed volume-wipe deployment/task now
also remains terminal, retaining the holder, rather than resubmitting on every delete
reconciliation. Ambiguous wipe submission responses retain their original fence.

Dokploy reloads increment Swarm's task `ForceUpdate`, even for a replica-only
change. A pure count reduction back to an already-running count therefore uses
temporary `stop-first`, `Parallelism=1` when at least two replicas remain running.
This frees one replacement slot without deliberately stopping the entire service.
The saved policy returns to `start-first` after the operation becomes terminal,
without another reload; superseding specification changes and Start/Restart/Stop
also restore it before replacing the pending operation, including Stop→Start.
Singleton reductions retain `start-first` because `stop-first` would retire the
only remaining serving task before its replacement starts.

This exception requires a matching application-specification receipt from a
successful deployment or reload. Staged environment, resource, port or image
changes cannot use the count-only exception; older records without that receipt
conservatively keep their existing policy until an ordinary rollout establishes
it. Unlike Nomad's count-only reconciliation, Dokploy still replaces tasks because
its API exposes no count-only Swarm update. Runtime Running is not native SQL/API
readiness; the engine's application-health and every-instance gates still apply.
Specification receipts use the shared versioned HMAC-SHA256 helper, keyed by the
control-plane-held Dokploy API credential with a dedicated value domain. This
prevents a metadata reader from guessing low-entropy environment secrets offline.
API-credential rotation invalidates older receipts conservatively until a
successful ordinary apply establishes a receipt under the new credential.

### Resolving an unknown receipt or failed wipe

An unknown enqueue stays pending (a runtime may eventually report
stalled/unknown-outcome); admission shows a previous-deployment or deployment-slot
wait. A failed wipe reports `volume wipe deployment failed; the holder was retained`
or `volume wipe task failed; the holder was retained`. It can block cluster
deletion. Restarting alone does **not** clear durable receipts, and
`retry-deployments` is not a Dokploy wipe-rearm command.

1. Identify the **exact application ID**, environment, generated `appName`, pinned
   node and volume from the Karakoram service/operation and the Dokploy application
   description. Preserve a copy of the description before any manual change.
   In Dokploy's application Deployments UI, or
   `GET /api/deployment.all?applicationId=<ID>`, inspect the deployment IDs and
   terminal state, logs and queue entries. Compare against `pending.deployments`
   (runtime) or `wipePreviousDeployment` (holder) in the description. Inspect
   `docker service ps --no-trunc <appName>` on a manager and the pinned node's
   containers/mounts. Do not mistake an empty deployment list for absence:
   `GET /api/application.one?applicationId=<ID>` must return not-found to establish
   that the application was removed.
2. If the original deployment appears, let Karakoram track it; new authoritative
   rows replace the unknown capacity reservation automatically. If the exact
   application is absent, the reservation is retired automatically, including
   after restart. A wipe still waits for running tasks and final volume cleanup.
   Do not remove a data-bearing runtime application just to free capacity.
3. If no row appears, first obtain authoritative confirmation from the Dokploy
   queue/worker and request logs that the original request cannot still execute.
   **No row and elapsed time alone are not sufficient.** Stop every Karakoram
   control plane managing this installation before changing metadata. **For a
   runtime only**, when the request is confirmed never enqueued, edit that
   application's description via Dokploy's settings or
   `POST /api/application.update` with only `applicationId` and the updated
   `description`. Keep the existing prefix and all unrelated JSON fields intact;
   remove only `pending` and `mutationUntil`. Restart the control plane to discard
   its in-memory reservation. The next reconciliation can make a fresh admitted
   attempt. Never clear ownership, volume, node or writer-fence fields, and never
   edit Karakoram's SQLite operation records.
4. For an **unknown or failed wipe**, use manual erasure and exact-holder removal,
   not a metadata reset. Clearing `wiping`, `wipePreviousTasks` or
   `wipePreviousDeployment` and restarting does **not** re-arm a wipe operation
   handle already persisted by an engine: that handle will keep waiting for its
   original task/deployment, even if Dokploy confirms nothing was enqueued.
   There is no product wipe-rearm action. If these fields were already cleared,
   the manual cleanup below still resolves the original tracked handle.
   With control planes stopped, conclusively exclude late enqueue delivery and
   confirm no queued/running deployment or task for that holder. Verify no runtime
   still mounts its exact named volume; a failed task is not proof of erased data.
   Correct the platform cause (node access, image pull, permissions), then manually
   complete the intended erasure on the original pinned node. This is destructive:
   back up anything that must survive. Remove the holder application by exact ID
   through Dokploy (`POST /api/application.delete`), remove its stopped task
   containers, and remove the **exact named Docker volume on the original node**
   once no container uses it. Never use a broad volume prune. Confirm application
   not-found, no running task/service, and volume absence on that node. Restart
   Karakoram: the existing wipe operation observes the absent holder, verifies
   cleanup and can finish cluster deletion. Keep the original node available
   until Karakoram acknowledges completion.

If queue delivery cannot be conclusively resolved, keep the receipt fenced and
repair Dokploy observation/queue state; do not force a second potentially
destructive deployment.



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
|`scopedTokens`|no|Dokploy API keys inherit the user's rights; a member user restricted to one environment cannot read the registry or nodes this provider needs. Use an admin key of a Dokploy installation dedicated to Karakoram.|
|`exec`, `metrics`, `dedicatedCpu`, `envChangeRestarts`|no|Variable writes apply at the next restart.|

Swarm ownership checks resolve the service ID from `swarm.getNodeApps` before
calling `docker.getConfig`; inspecting an application name can return its local
build image instead. This also works for zero-replica services with no task
containers and does not require cached metadata. An empty/unavailable inventory,
or an ambiguous inspection of a listed service, blocks mutations until observation
can be retried; it is not evidence of a foreign owner. Dokploy excludes service
names starting with `dokploy-` from this inventory: these names (including generated
application names for a `dokploy` hostname) cannot be inspected safely. Observation
and lifecycle mutations refuse them rather than treating them as absent or falling
back to ambiguous name inspection. Use hostnames outside this reserved prefix.

## Requirements

- Dokploy **v0.30.8** (the tested version) on an amd64 Docker Swarm; the API is unversioned, so test upgrades first.
- For more than one node: a container registry configured in Dokploy (Settings → Registry) that every node can pull from. Single-node Swarms may omit it.
- Outbound HTTPS from the manager during builds (Ubuntu, [Percona](https://www.percona.com/), RabbitMQ and Elastic package sources), from every node to [Docker Hub](https://hub.docker.com/) (volume deletion runs a one-shot task from the pinned `ubuntu:24.04` digest on the volume's node), and from members to your S3 storage.
- Elasticsearch: `vm.max_map_count=262144` on every node (`/etc/sysctl.d`).
- Enough free disk on every node for volumes and images (each engine image is roughly 0.3–1.7 GB).
- An overlay network for Karakoram, normally a dedicated attachable one (the provider creates it through Dokploy when it is missing).

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

1. In Dokploy create a project (for example `karakoram`) and, for clusters, a project or environment for the default namespace; note its environment ID (Dokploy UI URL or `GET /api/project.all`). Create an admin API key with rate limiting disabled (see [API key rate limit](#target-settings)) and, for multi-node Swarms, a registry. Before deploying the control plane, create its dedicated **attachable overlay** network (for example `karakoram`, via Dokploy's network settings or `POST /api/network.create` with `{"name":"karakoram","driver":"overlay","attachable":true}`); its Swarm service needs the network before the provider can start.
2. Create an application `karakoram` with build type **Dockerfile**, Dockerfile path `deploy/dokploy/Dockerfile` and context `.`, from this repository (Git provider) — it downloads and verifies the signed release pinned in `deploy/zerops/RELEASE`. To deploy an unreleased build, upload a zip containing that Dockerfile as **`Dockerfile` at the zip root** and a linux `karakoram` binary alongside it (Drop source); set Dockerfile path **`Dockerfile`**, context `.`, and build argument `KARAKORAM_SOURCE=local`.
3. Advanced settings:
   - Volume mount: named volume (for example `karakoram-data`) at `/var/lib/karakoram`.
   - Swarm placement: pin to the Dokploy manager node (`node.hostname==<manager>`): the named volume is node-local, and the control plane then talks to Dokploy's plain-HTTP service URL without leaving the host (Swarm overlay traffic between nodes is unencrypted). If it must run elsewhere, point `KARAKORAM_DOKPLOY_URL` at Dokploy's HTTPS domain instead.
   - Environment settings: turn off **Create .env file**, so Dokploy does not write the control plane's secrets into the build directory.
   - Swarm update and rollback config: `{"Parallelism": 1, "Order": "stop-first"}` — [SQLite](https://www.sqlite.org/) allows one writer.
   - Swarm network: `[{"Target": "<network>", "Aliases": ["karakoram"]}, {"Target": "dokploy-network"}]`. Members reach the control plane as `https://karakoram:9443`; `dokploy-network` lets [Traefik](https://traefik.io/traefik/) and the control plane reach Dokploy.
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

### Desired state and partial platform loss

Karakoram owns its application records and their randomly suffixed Swarm service
names. Use **Karakoram Stop**, not Dokploy Stop or `docker service scale ...=0`,
for an intentional suspension. For an owned application with durable running
intent, service observation recreates a missing Swarm service and restores an
externally zeroed replica count, including a stop from Dokploy's UI. It reuses
the recorded built image when it is present on the manager; otherwise it requests
one rebuild from the retained Drop Dockerfile source. Application identity,
mounts, node placement, environment and configured replica count are retained.

The application description durably records desired state and the current
lifecycle operation. A target-wide lock serializes provider instances within the
control-plane process. Restoration submissions have a ten-second request deadline;
an ambiguous response retains a one-minute delivery fence. Stop/Delete record
their intent before waiting on that fence. A queued/running build must complete
before another build, Stop or Delete is submitted: an accepted build can create
the service much later, after a nominal stop would otherwise finish. A lost
submission response with no new deployment record is deliberately not resubmitted:
inspect Dokploy's queue/deployment logs before resolving it. Archive bytes are
hashed while rendering, and an identical retry recovers the saved attempt handle
instead of uploading again. Queued and running builds have a separate six-hour
progress bound: after it, the operation fails with a stalled/unknown-outcome
diagnostic, but retains its exclusion because the original work may still commit.

Delete and stop waits older than **three minutes** report their elapsed age,
exact owned-service desired replica count, and task desired/current state counts.
The message distinguishes tasks still stopping from a Swarm inconsistency
(`desired 0 but a task is desired-running`). Unknown/failed ownership or service
inspection remains a wait, not evidence that deletion succeeded.

During PostgreSQL full-cluster deletion, that inconsistency can trigger a single
**escalated removal** through Dokploy's final `application.delete` step. This
requires persisted full-delete intent, the exact CP-recorded application identity,
matching immutable Swarm ownership labels/service identity, fresh zero-replica
inspection, no active or queued deployment, and no competing pending mutation or
delivery fence. Suspension/retained-volume cleanup and ordinary Stop never
escalate. The attempt is durably recorded before submission; uncertain delivery
is not replayed. The original receipt continues polling until application and
running-task absence are observed, including after a control-plane restart.
Missing queue permissions or incomplete observations disable escalation safely.
The generic two-hour operation-tracking deadline still does not cancel desired
cleanup; eventual authoritative deletion can complete the original operation.
The preceding idempotent stop has a separate durable confirmation: if a delete
journal survives before stop delivery (or after a refused stop), cleanup recovers
the stop against the exact owned service without replacing the receipt. An
explicitly observed native zero confirms older receipts without resending Stop.
Missing or null replica counts are unknown, never proof of zero.

Resolve the original queue/build and obtain a terminal deployment receipt before
retrying; never clear the receipt merely to force a second build. Failed terminal
deployments require an explicit corrected deployment, not an automatic retry loop.

Cluster deletion does not require a failed runtime build to become healthy.
Cleanup observes the recorded application identity without restoring or rebuilding
it, and deletion supersedes the obsolete engine deployment wait. A queued/running
mutation still retains its exact receipt: deletion waits for terminal delivery
without submitting another build, then removes the owned application even when
that deployment failed or was cancelled. An uncertain/stalled delivery remains
fenced and requires the operator inspection described above.

The ordinary operation failure budget is distinct from the exact-version
`DeploymentRetriesExhausted` budget. A failed Dokploy build or an exhausted
create/delete operation alone does not qualify for `retry-deployments`; its `409`
does not mean deletion requires another deployment attempt. Use cluster deletion
to remove a failed initial application.

An authoritative enqueue refusal (HTTP 4xx other than a request timeout) is
different from lost/ambiguous acceptance: archive uploads, retained-source rebuilds
and wipes retire that attempt's local and durable receipt, permitting a later
corrected retry. HTTP 429 still carries its `Retry-After` deadline. Transport
failures, timeouts and 5xx responses retain the original exclusion; elapsed time
is never permission to duplicate that submission.
Waiters for the shared target guard honor request cancellation even while another
application uploads an artifact. A canceled waiter does not detach or interrupt
the already-running upload.

Automatic restoration refuses visible foreign service labels/identity, a missing
or changed volume holder/mount/node, a wiping holder, and ambiguous Docker
observations. Dokploy can return `null`/`[]` when Docker commands fail; those
responses alone **do not prove absence**. Restoration requires a nonempty valid
Swarm service inventory that omits the service name. An application record removed
while its labelled Swarm service remains is reported as an orphan conflict, not
adopted or replaced. Restore the original record from backup, or stop/remove the
orphan after verifying its data and ownership before requesting replacement.
No holder or named volume is automatically recreated by this repair.

Runtime metadata binds the original holder application ID and that holder's own
ownership marker, as well as its node and exact randomly named volume mount.
Runtime and holder ownership markers are intentionally different because each
marker authenticates its own service hostname; equality between them is not
evidence of a valid attachment.

New volume-backed images hold an exclusive file lock on
`.karakoram-writer.lock` for the member's lifetime, so a recreated Swarm service
cannot start a second writer while an old task is still draining after deletion.
Existing images must be explicitly redeployed through Karakoram to acquire this
fence before automatic service recreation is enabled. Legacy application metadata
without durable desired state is migrated only while an owned running service is
visible; if it is already missing/stopped, explicitly Start it after verifying
retained data. A lost or physically damaged node-local volume still requires
operator recovery or engine member replacement; a holder record is not a backup.

Application creation itself includes a non-serving ownership/import-spec receipt.
Interrupted imports remain identifiable as CREATING and are resumed by replaying
the original specifications; setup failures do not silently roll back or orphan
that identity. Independent application search and known-ID reads corroborate
environment listings. Same-named foreign or duplicate receipts are refused.
Unknown creation outcomes are not blindly retried while their receipt is hidden.
The in-process fence is not a durable distributed uniqueness primitive: after a
control-plane restart, an API that simultaneously hides the committed receipt
from all lookup paths can still defeat name-based discovery. Restore reliable
platform visibility before retrying an unknown creation.
The neutral service observation explicitly marks unfinished reservations as
`importing`; ordinary reconciliation resumes setup rather than adopting a
nonnull owned service as deployment-ready. Relay enrollment is not rotated
mid-import, so replay retains the original sensitive environment.

Public-port changes record pending desired state before changing Dokploy rows.
A matching row or an empty port list is not an apply receipt: retries complete
the remaining Swarm reload after lost create/delete/reload responses. Stopped
services retain their desired settings without being started.
PostgreSQL whole-cluster cleanup withdraws saved port rows without requesting a
replacement task immediately before deletion. Any already-issued restart keeps
its exact journal and must converge before deletion submits Stop; ambiguous
port-delivery and queued-build fences are not discarded.

Ingress-published TCP/UDP ports are Swarm-wide, not per application, environment,
namespace or IP family: only one service can own a given ingress port/protocol.
Before a new publication, Karakoram checks Dokploy's Swarm service inventory,
including unmanaged services and other environments. A confirmed conflict fails
the operation promptly with `ExposureSupported=False` / `PublicPortInUse`; a
concurrent conflict discovered after a refused reload restores the prior Dokploy
port rows, endpoint settings, application status and update policy **without
reloading the healthy private service**. The requested public exposure remains visible in the spec:
free the port at its other owner or disable public exposure here.
Dokploy omits `dokploy-*` services from that inventory and can hide Docker's
reason behind `Error reloading application`. When that explicit failure is
followed by proof that the original owned private DNSRR service is still
unchanged, Karakoram restores the staged settings and fails with
`PublicPortPublishFailed` instead of claiming an unobserved port owner.
Lost/unknown reload delivery retains its durable pending journal and blocks a
new publication, private correction or other lifecycle mutation until the
applied port specification and original replacement-task outcome are observed.
A competing owner alone is not evidence that an outstanding request failed:
it might free the port before that request reaches Docker. Only explicit
completed refusal evidence permits rollback and a terminal clean-refusal claim.
If the handler fails after applying the requested ports, Karakoram durably
records that completed response; fresh matching service settings and the
original replacement tasks' completion repair Dokploy's error status without a
second reload. Stopped/undeployed settings-only changes do not acquire a reload
fence and can finish after reconstruction without an applied-service receipt.
If recording the completed response is interrupted before the rollback
decision, later observation resumes either the applied-public outcome or the
proven-unchanged-private refusal classification; it does not wait indefinitely
for a refused publication to appear.

Resource and replica-count updates consume any completed prior deployment before
merging new settings, so the platform settings and persisted intent stay aligned.
Volume-less application updates use start-first rolling replacement; namespace
relays remain serial stop-first singletons, including rollback, because two live
relay sessions can evict one another.

Unavailable, drained/paused and missing original storage nodes are diagnosed
separately. Reactivate/restore the original node and retained volume, or explicitly
replace the member using healthy replicas or a verified backup; Karakoram does
not relocate a local volume to empty storage. Volume-wipe completion is not
acknowledged if the original cleanup node can no longer be identified.

HTTP 429 responses carry their `Retry-After` reset (seconds or HTTP date) to the
control-plane scheduler without immediate transport retry or a short reset cap.
The original unavailable classification and credential-silent detail are retained.

**Exclusivity requirement and API limitation.** Run one Karakoram control plane
per target; all writes to its managed applications/service names must go through
it. Dokploy v0.30.8 has neither metadata compare-and-swap nor an absent-only
reload/deploy API. The shared lock is not a distributed lease across independent
control planes. A privileged operator creating a foreign service under the exact
randomly suffixed name *between* the ownership/absence check and Dokploy's
unconditional update-or-create can still race restoration; a queued build has
the same limitation. Do not create or mutate foreign objects under those owned
names. Visible foreign objects are refused, but the API cannot enforce the
check atomically.


- **Node loss = member loss.** A member is pinned to the node that holds its volume. If the node fails, the member stays pending until the node returns; replace the member through Karakoram to rebuild it elsewhere from its replica set or cluster peers.
- **Member replacement needs headroom.** Karakoram replaces a member by adding the new one before removing the old one. When the cluster already uses every eligible node, the new member is placed on the node of the member it replaces, so that node must have free CPU and memory (Swarm reservations) for one extra member during the replacement; otherwise the new task stays pending until capacity is available. Add a node or free capacity first.
- **Changing public ports restarts the member** (Dokploy applies ports with a service update). Endpoint-mode/port cutovers use temporary `stop-first` with `Parallelism=0` (all replicas in one batch), rather than serial per-slot replacement, to avoid old/new endpoint overlap. This deliberately interrupts the whole service during cutover, including multi-replica proxies. The exact previous update policy is journaled and restored after completion or lifecycle supersession; ordinary updates retain their existing rolling policy. Public addresses still use the configured Swarm-node address and routing mesh.
- **Published PostgreSQL proxy endpoint recovery.** If every proxy task is natively healthy but the logical private endpoint fails, reconciliation can force a journaled stop-first update even when the public-port rows already match. Recovery is limited to three attempts per incident, at least five minutes apart; a successful logical endpoint probe acknowledges recovery and resets that budget. Task convergence alone never acknowledges recovery. A repair that was journaled but never delivered can consume the next bounded attempt after its startup deadline; queued or uncertain deployments are never superseded. Ownership and lifecycle delivery fences still apply. If the logical endpoint remains broken after the final completed attempt, the tracking operation fails with `PublishedEndpointRepairExhausted` and an actionable condition instead of remaining Running. Automatic repair stays stopped across control-plane restarts; inspect and repair the Swarm private DNS alias/VIP and overlay connectivity, or disable public exposure. A later successful logical probe resets the incident budget. Public addresses and ports are not changed by this repair.
- **Deleting a volume** wipes its contents with a one-shot task on its node and removes the holder application. Dokploy can remove Docker volume objects only on the manager; an empty volume object can remain on workers (`docker volume prune` on that node removes it).
- `keepVolumes` deletion keeps the holder application and its Docker volume; resuming the cluster reattaches it on the same node.
- Images are rebuilt by Dokploy for each deployment; the layer cache makes later members fast, but the first build of an engine version takes about one to two minutes. Use Dokploy's cleanup settings for old images.
- Variable changes are stored immediately and applied at the next restart (`envChangeRestarts: false`).
- Elasticsearch's S3 client trusts only public CAs; use a publicly trusted S3 endpoint for snapshots.

## Dokploy terms and licence

Dokploy is Apache-2.0 except the `/proprietary` directory. Its [terms](https://github.com/Dokploy/dokploy/blob/v0.30.8/TERMS_AND_CONDITIONS.md) forbid "any commercial resale or redistribution of Dokploy as a service" without explicit consent. Running Karakoram on your own Dokploy for your own databases is internal use; offering Dokploy-backed databases to third parties as a service needs Dokploy's written consent. Karakoram does not include or redistribute Dokploy.
