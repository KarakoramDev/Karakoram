<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Replica sets

A replica-set cluster is a managed PSMDB set named `rs0`. Choose **1, 3, 5 or 7** data-bearing members; even counts and counts above seven are rejected. Each member is a singleton Zerops runtime with its own persistent Local Storage volume. A one-member set is useful for development but cannot retain availability during member restart, replacement or host loss.

Before creating a cluster, [install and authenticate](../install.md), check the [version catalog](scaling-and-upgrades.md), and read the [placement limitations](../limitations.md). Three members do not guarantee three physical machines.

Choose the project's [namespace](namespaces.md) in the UI or add `-n NAME` to
CLI commands below (omitted single-object namespace defaults to `default`).
Names are unique within a namespace. Connection details use that project's
private member hostnames: a management relay does not make them reachable by
application clients in another project.

## Create

In **Clusters → Create cluster**, choose `replicaSet`, a provider-advertised version, engine replica count and CPU/RAM/disk bounds. Keep `requireTLS` and private exposure unless you have a specific access plan. Names must match `^[a-z][a-z0-9]{1,11}$`: 2–12 lowercase letters/digits, beginning with a letter.

```sh
karactl providers get psmdb
karactl clusters create demo --topology replicaSet --members 3 \
  --version 8.0.32-14 --min-ram 1 --max-ram 1 \
  --min-disk 5 --max-disk 5 --tls-mode requireTLS --wait
karactl clusters status demo
karactl clusters members demo
```

Use the version currently advertised by your installation, not an arbitrary apt version. The CLI supplies its own resource defaults; explicit bounds above keep this example at 1 GiB RAM and 5 GiB persistent disk per member. The provider's omitted-field defaults are also 1 GB RAM/5 GB disk, while CLI convenience defaults allow larger maxima. CPU is expressed in cores; RAM/disk fields are GiB in the public API.

REST create requires `spec.provider`; omitted provider is not inferred. Omitted counts/resources use catalog defaults, but explicitly supplied zero is rejected. PSMDB CPU bounds are whole cores at least 1; RAM bounds are at least 0.5 GiB and disk bounds at least 5 GiB, with every maximum at least its minimum. Request bodies reject unknown fields, including YAML misspellings submitted through `karactl -f`; inspect the named field in the returned error before retrying.


Creation is asynchronous: services are imported, artifacts deployed, agents obtain protected config, the set is initiated and system users are established. `--wait` follows observed cluster-generation convergence. Ready TCP alone is not replica-set health.

Create acceptance returns `operationId` and `Karakoram-Operation-Id`; save that ID and use `karactl operations wait ID` if the initial wait times out. Resource, replica, exposure, TLS and version updates also have tracking operations. Their current `step`/`message` describe convergence, and a newer change cancels older tracking with the superseding ID. Status messages expose the current provisioning stage instead of calling initial formation scaling.

## Observe health

```sh
karactl clusters get demo -o yaml
karactl clusters members demo
karactl clusters placement demo
karactl operations cluster demo
karactl events list --cluster demo
```

Inspect phase/message/conditions, observedGeneration, member health/state/role, agent connectivity/configVersion, binary version and lag. A caught-up secondary is different from a service merely marked ACTIVE by Zerops. The cluster overview and topology tabs expose these observations; [monitoring](monitoring.md) adds time series.

Default status connection URIs omit passwords. [Explicit connection reveal](users-tls-access.md#connection-details) requires a separate permission; use application-specific users for workloads. Follow the [complete application TLS recipe](users-tls-access.md#connect-an-application-user-with-tls) to create `appwriter@appdb`, export `ca.pem`, build a URI with `authSource=appdb` and `replicaSet=rs0`, and connect with mongosh or PyMongo. Run private connections from a service in the database's Zerops project, not an ordinary workstation. For public seeds, read the [replica-set discovery caveat](users-tls-access.md#public-exposure-and-discovery) before assuming external failover works.

`spec.version` is desired; `status.version` comes from observed runtime versions, not the requested upgrade target. Unavailable observations are explicitly unknown, not proof that the target is deployed. Members leaving or blocked from leaving stay visible as non-healthy until their runtime is gone. Condition transition timestamps change only when status or reason changes, not every observation.

## Add, remove and replace members

```sh
karactl clusters scale demo --members 5 --wait
karactl clusters scale demo --members 3 --wait
karactl clusters replace-member demo demom1 --wait --yes
```

Use an actual member name from the inventory for replacement. Added members join non-voting with priority zero, initial-sync, then become voters after observed healthy replication and lag at most ten seconds. Scale-down removes high-index members safely, stepping down a primary when required and checking voting majorities. Replacement creates a new monotonic member identity rather than reusing the old hostname; names can therefore have gaps. At seven voters it does not create an eighth voter during replacement.

An unhealthy set can block disruptive work. Restore quorum/replication health rather than repeatedly submitting replacement or restart commands. A single-member replacement is necessarily a special availability transition, not a promise of zero downtime. [Troubleshooting](../operations.md#troubleshooting-by-symptom) explains stuck deployment/config states.

## Delete or suspend

Deletion options control **two separate assets**: database volumes and PBM backups.

```sh
# Suspend: remove runtimes, retain database volumes and recovery objects.
karactl clusters delete demo --keep-volumes --keep-backups --wait --yes
karactl clusters resume demo --wait

# Destructive full deletion: volumes and owned backups are removed.
karactl clusters delete demo --wait --yes
```

With `keepVolumes`, the cluster remains `Suspended`, retains secrets/volume identity and can resume by recreating its runtimes. Keeping volumes does not automatically keep backups: specify both flags if you want both. Without retained volumes, deletion is not resumable. With `--keep-backups`, backup records, snapshot provenance and protected physical-recovery credentials survive full deletion: use `karactl backups list/get/delete` with the original cluster name, or create a new compatible cluster with `spec.dataSource` referencing a retained successful snapshot. See [retained-backup management and cloning](backups-and-restore.md#retained-backups-after-full-cluster-deletion). Retained backups do not recreate the deleted resource or volumes; preserve control-plane state and its encryption key for disaster recovery.

DELETE returns a 202 Operation body and tracking header; its success means runtime suspension or full cleanup has completed. Suspended members report `SUSPENDED`, non-healthy/disconnected agents and an observed suspension generation; historical allocation is not a running service. Resume returns a Cluster with its new `operationId`. Running deletion cannot be cancelled safely; inspect its message/events and repair the blocking dependency. See the [API async contract](api.md#declarative-updates-and-asynchronous-work) for bounded operation deadlines and cancellation limits.

Normal scale-down/replacement removes the retired member's volume; cluster deletion retention is not a per-member retention option. Cleanup of recovery objects runs before removing agents that PBM needs. See [backups and restore](backups-and-restore.md) and [operations](../operations.md).
