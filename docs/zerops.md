<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Zerops provider

[Zerops](https://zerops.io/) is Karakoram's reference infrastructure provider. It supplies [Ubuntu](https://ubuntu.com/) runtimes, Local Storage and public routing in an **existing Zerops project**; Karakoram owns the engine topology, credentials, health and recovery. Karakoram does not create the project or manage its system `core` service.

For a provider comparison, see [Providers](providers.md). For engine configuration and supported topologies, start with [MongoDB](mongodb/README.md), [PostgreSQL](postgres/README.md), [Supabase](supabase/README.md), [RabbitMQ](rabbitmq/README.md) or [Elasticsearch](elasticsearch/README.md).

## How it maps

| Karakoram | Zerops |
| --- | --- |
| Namespace target | Existing project, with its own project-scoped token |
| Control plane | One Ubuntu 24.04 runtime, normally `karakoram`, and a separate `karakoramdata` Local Storage volume |
| Data-bearing member | Singleton Ubuntu 24.04 runtime and independently identified Local Storage volume |
| Stateless component | Engine driver-supported runtime without a durable data volume; replicas share a service identity |
| Engine and agent deployment | Prebuilt artifact upload and a rendered `zerops.yaml`, including preparation commands, ports, readiness and volume mount |
| Private connectivity | Project-private service hostnames; protected agent API on TLS 9443 |
| Public TCP | Routes on project IPv6 or dedicated IPv4, with CIDR allowlists |
| Public HTTPS | Zerops subdomain access for supported HTTP ports; TLS terminates at Zerops's L7 balancer |
| Additional namespace | Outbound relay runtime in that target project |
| Asynchronous operation | Persisted Zerops Process IDs, polled until a terminal outcome |

Importing infrastructure and deploying code are separate steps. Runtimes are initially `READY_TO_DEPLOY`; import acceptance, Process completion and engine readiness are different observations. Karakoram uploads artifacts and observes the engine before declaring convergence. See [Architecture](architecture.md) and [ADR 0006](adr/0006-zerops-backend-client.md) for the infrastructure boundary.

## Capabilities

| Capability | Zerops behavior |
| --- | --- |
| Placement | Best-effort observations only; no host anti-affinity guarantee |
| Persistent storage | Node-local Local Storage with enforced capacity and supported growth; no shrink |
| Public TCP | Project public addresses and provider-enforced CIDR allowlists |
| Managed public HTTPS | Supported for engine driver-approved HTTP components |
| CPU and RAM | Platform autoscaling bounds; shared or dedicated CPU |
| Stateless replicas | Supported where the engine driver permits them; not database replication |
| Credential scope | Full access to one project, without sudo mode |
| Additional namespaces | Outbound authenticated management relay required |

Provider capabilities do not establish engine availability or independent failure domains. Engine catalogs and validation determine which components, topologies and exposure settings are accepted. See [capability differences](providers.md#capability-differences) before choosing a provider.

## Requirements

- An existing Zerops project and permission to import services and deploy code. Organization Owner/Admin access and project-creation rights are not required.
- A project token from **Settings → Access Tokens Management → Custom access per project → Full access**, for that project only, with **sudo mode off**. Read-only access cannot provision, update secrets, grow storage or configure routing. See [Zerops token permissions](https://docs.zerops.io/features/rbac#integration-tokens).
- Capacity for the control plane: one Ubuntu 24.04 container, shared CPU, 0.5–1 GiB RAM, 1–2 GiB runtime disk and a separate 5 GiB Local Storage volume. A LIGHT project can host this footprint, but cluster services need additional capacity.
- Available control-plane service names, normally `karakoram` and `karakoramdata`. Additional targets need capacity and a free relay hostname, normally `karakoramrelay`.
- For public TCP, project IPv6 or dedicated IPv4 and clients that can reach the chosen address family. Shared IPv4 HTTP routing is not a generic database TCP gateway.
- For a local checkout deployment, authenticated [zcli](https://docs.zerops.io/references/cli) with deployment access. The default prebuilt installation needs no local [Go](https://go.dev/) or [Node](https://nodejs.org/) toolchain.

Full access without sudo mode cannot read sensitive environment values, but it retains infrastructure mutation authority. Keep the token server-side and preserve service environment isolation. Karakoram is not a sandbox against project administrators. See [Security](security.md).

## Install the control plane on Zerops

Use the dashboard-only [Quick start](quickstart.md) to import the release without a local checkout. Use [Install](install.md) for local deployment, prebuilt versus source setups, custom service names, custom domains and CLI installation. These are the authoritative installation steps; do not re-import new-install templates over an existing installation. Use [Upgrade](upgrade.md) instead.

The default provider is `zerops`. The control plane reads these target settings:

| Environment variable | Purpose |
| --- | --- |
| `KARAKORAM_BACKEND` | Provider kind; defaults to `zerops` |
| `KARAKORAM_ZEROPS_TOKEN` | Project-scoped infrastructure credential, supplied as a service secret |
| `KARAKORAM_ZEROPS_PROJECT_ID` | Existing project ID; the installation pipeline uses injected `projectId` |
| `KARAKORAM_ZEROPS_API_URL` | Optional non-default API endpoint, configured by the installation operator for all Zerops namespaces |
| `KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS` | Optional integer at least 1; default 2 in-flight owned deployments per API endpoint/project in this control-plane process |

Other installation settings, including `KARAKORAM_MASTER_KEY`, `KARAKORAM_DATA_DIR` and `KARAKORAM_PUBLIC_URL`, are described in [required secrets and generated settings](install.md#required-secrets-and-generated-settings). Preserve the master key separately from consistent control-plane state backups.

Do not name user secrets `ZEROPS_TOKEN` or `ZEROPS_PROJECT_ID`: the `ZEROPS_` prefix is reserved. Use `KARAKORAM_ZEROPS_*`. Secret changes do not update existing processes until restart/redeployment, and pipeline runtime variables override same-named service secrets.

## Namespaces and relays

The `default` namespace manages the control plane's own project using its environment-held token and direct private networking. Each additional Zerops namespace registers another existing project and a write-only project token. Zerops accepts no per-target `--config` keys; the API endpoint is operator configuration, not a namespace override.

Register through **Settings → Namespaces → Add namespace → Validate and add**, or use the CLI:

```sh
# Keep the project token in a private, untracked file; '-' reads stdin.
karactl namespaces create --name dev --backend zerops --project-id '<project-id>' \
  --token-file namespace-token.txt
karactl namespaces get dev -o yaml
karactl -n dev clusters list
```

Registration validates project identity and service-list access synchronously; relay deployment follows asynchronously. A read-capable token can pass validation yet lack mutation permissions, so wait for `token.valid: true`, namespace/relay `Ready` and `relay.connected: true` before managing clusters.

The relay dials `KARAKORAM_PUBLIC_URL` over outbound HTTPS/WebSocket and carries an inner mutually authenticated TLS tunnel. The origin must be reachable from the target project, with WebSocket Upgrade forwarded at `/relay/v1/connect` and no browser-login interception. It carries approved management and agent traffic, not application connections or bulk backup transfers. Applications still need their own private or public connectivity to the engine.

Rotate an additional namespace's token with `karactl namespaces rotate-token dev --token-file replacement-token.txt --yes`, then revoke the old token after success. For `default`, change the control-plane service secret and restart; API rotation is rejected. Provider kind and target identity are immutable, so switching the default provider is not an in-place cluster migration. See [Namespaces and relays](namespaces.md) for validation, relay health and removal.

## Placement and persistent storage

Each data-bearing member has its own runtime and volume; the runtime is co-located with that Local Storage volume. Local Storage is **single-host, non-replicated storage**. Separate services, volumes or dedicated CPU do not prove separate hosts, racks or zones. One host failure can remove several voters.

Placement groups are heuristic observations, not authoritative host identities or a placement control. Missing or changing signals can make groups unknown or stale; different groups do not certify future separation. See [ADR 0016](adr/0016-placement-anti-affinity.md) and [Limitations](limitations.md#zerops-failure-domains-and-storage).

Runtime root disks are wiped on deployment. Engine data belongs only on its assigned persistent volume, not the root disk or a shared filesystem. Local Storage has a 5 GB minimum; mounted capacity follows the volume service's `maxDisk`, not runtime disk or volume `minDisk`. Growth is supported, but reducing either desired persistent disk bound is rejected. Retaining a volume does not replicate its contents or move them to another host.

Use [engine-native backups](backups.md), not live Local Storage archives, for consistent database recovery. Back up control-plane [SQLite](https://www.sqlite.org/) state consistently and keep its original master key; see [control-plane state backup and recovery](operations.md#control-plane-state-backup-and-recovery).

## Public TCP and HTTPS

Engines are private by default. Explicit public TCP exposure uses a project public address, allocated public ports and the requested CIDR allowlist. Dedicated IPv4 is a separate project capability and cost; shared HTTP/IPv4 routing cannot substitute for it. CIDR rules complement, rather than replace, engine authentication and TLS.

Route synchronization is project-wide. The provider accounts for other services' routes, including staged/deleting routes, when allocating ports, and waits for the synchronization Process and observable route state. `Ready` or `isSynced` is not proof of external reachability: test allowed and denied clients independently, including address-family connectivity.

Discovery-based clients may need more than one reachable seed endpoint. For [MongoDB](https://www.mongodb.com/), external discovery must reach the required members with matching advertised addresses, DNS and certificate SANs. Follow the engine's access guide rather than exposing only the primary or substituting one load-balanced seed. See [MongoDB access and TLS](mongodb/access-and-tls.md) and [ADR 0017](adr/0017-public-tcp-exposure.md).

Managed public HTTPS is separate from public TCP. For supported HTTP components, the provider enables Zerops subdomain access and reports HTTPS URLs for HTTP-enabled runtime ports. Zerops terminates TLS at its shared L7 balancer and forwards HTTP to the runtime. This is not end-to-end engine TLS or a TCP database endpoint. The control-plane UI/API uses public HTTPS to port 8080; keep the private agent listener on TLS 9443 unexposed. Custom control-plane domains are covered in [Install](install.md).

## Resources and autoscaling

CPU/RAM bounds are translated to platform autoscaling settings, with `SHARED` or `DEDICATED` CPU. CPU bounds are whole cores, at least 1; optional `startCpu` must be whole and within the bounds. RAM/disk bounds use GiB in the API, with engine driver-specific floors and maximums at least their minimums. Catalog floors are not workload sizing advice.

```sh
karactl -n dev clusters scale '<cluster>' --min-cpu 1 --max-cpu 2 --wait
```

Use engine-supported component and membership controls for topology changes. Increasing a runtime's container count is not database replication; persistent members remain singleton runtimes, while engine driver-supported stateless components may have replicas.

Resource updates preserve the complete platform autoscaling configuration, including unspecified bounds. Persistent capacity changes target the volume rather than ephemeral root disk. Even CPU-only changes can restart a runtime, so let Karakoram apply member-aware disruption precautions instead of changing platform settings independently during reconciliation.

Member `autoscaling` reports effective platform bounds, not desired spec. Member `resources` reports live allocation from the agent, with an effective-bounds fallback when unavailable; `diskTotalBytes` is mounted data-volume capacity. A matching generation and `Ready` do not mean allocation changes instantly or establish a hard memory-allocation cap.

CPU-mode rate limits surface `PlatformCooldown` with a persisted retry deadline; reconciliation resumes automatically, including after a control-plane restart. Platform resource/import rejections remain authoritative and report safe validation details and attempted bounds. See [Resources and autoscaling](clusters.md#resources-and-autoscaling) for status fields and diagnostics.

## Operations and limits

- **One control-plane replica:** SQLite requires a single writer. Keep `maxContainers: 1` and `temporaryShutdown`; upgrades interrupt management. Lost state cannot be reconstructed by automatically adopting existing services.
- **Asynchronous platform work:** Zerops Processes are polled, not watched. Accepted work and terminal Process success do not establish engine health.
- **Deployment admission:** the default cap is two in-flight owned deploy/build processes per API endpoint/project in this process, shared across namespaces. New version allocation/upload also waits for prior deployment/build work on that service. Finished history does not consume slots or impose a service lifetime limit. Each observation queries all known unfinished process states (PENDING, WAITING, RUNNING, ROLLBACKING, CANCELING) independently of history ordering or retention; a ten-second project cache shares observations. Filtered pagination rejects changed totals and duplicate IDs instead of accepting incomplete evidence. History is fully indexed on cold start and at least every ten minutes, with incremental prefix reads and unfinished-process refresh between rebuilds. History shrinkage or an ID gap also requires rebuilding. Observed unknown states fail closed, but a late-visible unrecognized state hidden behind an indexed prefix may remain undiscovered until the next full rebuild. Running or unknown work counts regardless of age once observed. Unreadable or incomplete evidence reports `DeploymentEvidenceUnavailable` in deployment-wait status and events rather than authorizing more mutations.
- **Environment snapshots:** writing service secrets does not restart members. Engine drivers own safe rolling restarts to apply changes.
- **Deletion and retained costs:** platform refusals can leave verified owned volumes in orphan inventory. Cluster deletion completion does not prove all charged storage is gone.
- **No arbitrary container exec:** node work uses the bounded authenticated agent, not a remote shell API.

See [Limitations](limitations.md) for shared control-plane, security, networking and engine constraints, and [Operations and troubleshooting](operations.md) for recovery procedures.

## Costs

Karakoram is self-hosted software; infrastructure costs depend on the project, control plane, engine members, stateless components, relay runtimes, Local Storage, object storage and public addresses. Dedicated CPU, larger resource bounds and dedicated IPv4 affect the footprint. Replication adds members and volumes rather than making one volume replicated.

Use [Operating costs](operating-costs.md) for current assumptions, worked engine footprints and platform pricing links. Include backup retention and retained/orphaned volumes in cost review; do not equate deleting a cluster record with removing every billable resource.

## Troubleshooting

| Symptom | Check / action |
| --- | --- |
| Token validation fails | Check project identity and project-scoped Full access. Keep sudo mode off; it is not required. For `default`, update the service secret and restart. |
| Validation passes but provisioning fails | Validation checks reads, not every mutation grant. Inspect token permissions, project capacity and safe platform error details. |
| `READY_TO_DEPLOY` after import | Import is not code deployment. For the control plane follow [Install](install.md); for members inspect the operation and platform deployment logs. |
| Waiting for a platform deployment slot / previous deployment | Inspect the named service/Process and its status. Running work counts regardless of age. For `DeploymentEvidenceUnavailable`, inspect the accompanying API/identity/pagination diagnostic and restore readable, complete evidence; admission retries automatically. This is a non-terminal wait, including when the selected app version is absent: transient process-read failures do not consume the deployment retry budget or mark the deployment failed. Cold history scans can take longer for long-lived services. Do not enqueue parallel manual deployments to bypass admission. |
| Resource change remains `Updating` | Inspect `PlatformCooldown`, the update operation and cluster events. Allow the persisted cooldown retry; platform validation failures report attempted bounds. |
| Disk size did not change as expected | Inspect the Local Storage volume's `maxDisk` and mounted `diskTotalBytes`, not runtime root-disk allocation. Shrinking is unsupported. |
| Public TCP is unreachable | Check IPv6/dedicated IPv4 availability, client address family, allowlist, route synchronization, engine TLS/authentication and discovery endpoints. Test a denied client as well. |
| HTTPS URL is missing or wrong | Check HTTP port support and enabled subdomain access. A TCP route does not create an HTTPS endpoint; custom control-plane origins require pipeline configuration and redeployment. |
| Additional namespace relay is disconnected | Check public origin/certificate reachability and WebSocket Upgrade, then namespace/relay fields and logs. Do not restart database members first. |
| Co-located or unknown placement | Treat the observation as a risk signal, not proof of independent hosts. Zerops provides no anti-affinity guarantee. |
| Volumes remain after deletion | Inspect [orphaned platform volumes](operations.md#orphaned-platform-volumes); review ownership and platform refusal before cleanup. |

Start with cluster status, members, events and operation details:

```sh
karactl -n dev clusters status '<cluster>'
karactl -n dev clusters members '<cluster>'
karactl -n dev clusters get '<cluster>' -o yaml
karactl -n dev operations cluster '<cluster>'
karactl namespaces get dev -o yaml
```

For exhausted deployment retries, use the explicit recovery procedure in [Clusters and operations](clusters.md#retry-exhausted-platform-deployments) after correcting the underlying failure. A control-plane restart does not reset that budget. Never disable TLS verification, replace ownership markers or re-import over the installation to clear a diagnostic.
