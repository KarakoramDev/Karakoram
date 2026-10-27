<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Replica sets

Create and maintain a managed [PSMDB](https://docs.percona.com/percona-server-for-mongodb/) replica set named `rs0`.

## Create

### Web UI

1. Select the database project's [namespace](../namespaces.md), then **Clusters → Create cluster → [MongoDB](https://www.mongodb.com/)**.
2. Choose **replicaSet** and an exact [catalog version](README.md). Select **dev** only for disposable single-member use, or **prod** for three voting members.
3. Review member resources and total project capacity. Keep private exposure and `requireTLS`; configure [backup storage](../backups.md) if recovery is required.
4. Review and create. Follow the resulting operation and **Topology** until the desired generation is observed and active members are healthy.

### CLI alternative

[Install and authenticate karactl](../cli.md), then select the namespace with `-n NAME` (omitted single-object namespace defaults to `default`):

```sh
karactl providers get psmdb
karactl -n default clusters create demo --provider psmdb \
  --topology replicaSet --members 3 --version 8.0.32-14 \
  --min-ram 1 --max-ram 1 --min-disk 5 --max-disk 5 \
  --tls-mode requireTLS --wait
```

Use a version advertised by your installation. Explicit bounds avoid the CLI's larger resource maxima. REST requires `spec.provider`; omission selects defaults only for optional fields, while explicit zero/unknown fields are rejected. See [API validation](../api.md) and [OpenAPI](../../api/openapi.yaml).

## Observe health

```sh
karactl -n default clusters get demo -o yaml
karactl -n default clusters members demo
karactl -n default clusters placement demo
karactl -n default operations cluster demo
karactl -n default events list --cluster demo
```

Ready means the desired `generation` is observed by `observedGeneration` **and** authenticated member observations show healthy replication. Check phase/message/conditions, role/state/lag, agent connectivity/configVersion and observed binary versions. A caught-up secondary is not merely a [Zerops](https://zerops.io/) ACTIVE service or an open TCP port. [Operation success alone is not a health guarantee](../clusters.md#operations-and-status).

`spec.version` is desired; `status.version` reflects observed binaries, with missing observations unknown. Leaving members remain visible until removed; the UI counts active members and leaving members separately. Condition transition timestamps change on status/reason transitions, not every poll.

Writable-primary readiness requires a freshly selected primary's `hello` and
fresh reachability of the set's members, not just a secondary's potentially stale
replication view. A failed observation is retried once within the same bounded
cycle; it never extends cached readiness. See the shared
[health freshness and transient tolerance contract](../clusters.md#operations-and-status)
for the 2-second read-probe, 5-second sweep and 15-second maximum sample-age bounds.

Use [application connection instructions](access-and-tls.md); private hosts belong to the namespace's project, not necessarily the control-plane project.

## Add, remove and replace members

```sh
karactl -n default clusters scale demo --members 5 --wait
karactl -n default clusters scale demo --members 3 --wait
karactl -n default clusters replace-member demo demom1 --wait --yes
```

Choose an actual inventory name for replacement. Added members join non-voting, priority zero; after initial sync, healthy replication and lag ≤10 seconds, they become voters. Scale-down removes high-index members, checks voting majorities and steps down a primary when required. Replacement uses a new hostname, so indexes may have gaps; it never creates an eighth voter at the seven-voter limit. Retired member volumes are removed: cluster-level retention is not per-member retention.

Unhealthy replication/quorum blocks disruptive work; repair it instead of repeatedly resubmitting. If replacement fails while removing the old member, ordinary convergence finishes removal before promoting the caught-up replacement; failure is not rollback. Inspect voting state before raising FCV. A singleton replacement necessarily interrupts availability.

For resource changes/restarts, see [MongoDB operations](operations.md). For deletion, suspension and resumption, use [Delete or suspend](../clusters.md#delete-or-suspend); for [PBM](https://docs.percona.com/percona-backup-mongodb/) cleanup, see [backups](backups.md).
