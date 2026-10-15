// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/*
Package psmdb manages Percona Server for MongoDB without Kubernetes.

# Catalog provenance

The noble amd64 package indexes at
https://repo.percona.com/psmdb-60/apt/dists/noble/main/binary-amd64/Packages,
https://repo.percona.com/psmdb-70/apt/dists/noble/main/binary-amd64/Packages, and
https://repo.percona.com/psmdb-80/apt/dists/noble/main/binary-amd64/Packages
provide the catalog pins 6.0.29-23 (deprecated),
7.0.40-22, 7.0.43-23, 8.0.29-13 and default 8.0.32-14. Package pins add
.noble. PBM is pinned to 2.16.0-1.noble. Percona's 8.3 documentation at
https://docs.percona.com/percona-server-for-mongodb/8.3/ explicitly labels
the family Technical Preview, so it is not advertised as supported.
Telemetry opt-out follows
https://docs.percona.com/percona-server-for-mongodb/8.0/telemetry.html:
PERCONA_TELEMETRY_DISABLE=1 is set during apt installation and at runtime.
Repository setup uses percona-release --scheme https.

# Durable state

Cluster.Internal stores uid and pbmPrefix, phase, configVersion, members,
sets, nextIndex, pending, retry, platformRetries, configHash, pbmHash, pbmStorage, cloneQueued,
balancerStopped/balancerWasEnabled, cleanupBackups/cleanupSecrets,
cleanedStorages, usedStorages, rotationSecrets, replaceActive, suspended and TLS transition
fields tlsMode/targetTLS. Each member records hostname, volume and verified platform
service/volume IDs, replica-set role, monotonic index and shard index; lifecycle stage; deployed/desired
versions; individually activated configVersion/TLS/config/cache/ConfigDB; complete
applied runtime resources, volume bounds and router count; removal/replacement
flags; and a stable public-port reservation and exposure hash. Each replica
set records bootstrap/users/ready stage and whether mongos registered it.
Pending stores the infrastructure operation ID, action, member and proposed
version/resources/count. Proposed settings become applied only after an
observed terminal successful infrastructure operation. Failed and canceled
processes are errors, never implicit successes.
Re-issuable import, deploy/upgrade, resources, disk, containers and runtime/volume
deletion processes (including cleanup) clear the failed pending process and
persist its exact action/member/target plus a per-member/action consecutive
failure count, platform message and retryAfter timestamp. Retries start after
30 seconds, double up to ten minutes, and continue indefinitely at that cap.
Errors/events retain the platform failure message and attempt count during
backoff; after ten failures the status explicitly says the action keeps failing.
Successful completion resets the counter. Import retries re-verify ownership,
adopt already-created services and import only missing services. Upgrade retries
retain the original member/version and recheck disruption safety; rolling
operation checkpoints cannot advance past a pending platform action or retry.

Provider saves merge owned keys into the existing JSON object. PBM's
pbmCleanup, pbmMaintenance and pbmStopped fields, and the freshBaseBackup*
gate, freshness boundary and retry state, survive every provider save. All passwords,
CA keys, certificate bundles and agent tokens live in the encrypted secret
store, not Internal or operation checkpoints. Certificate and key material
for one node is written as one atomic combined-PEM secret. Per-node backup
and monitor credential snapshots prevent a rotation from propagating to all
agents during a periodic configuration pull.

# Sync paths

Creation starts with a stable UID/prefix and topology plan. The next call
ensures the CA, keyfile, five system-user passwords, hashed/encrypted agent
tokens and member/agent certificates. A member progresses planned -> imported
-> deployed -> healthy. Runtime and local-storage services are imported
without inline deployment YAML to avoid Zerops's empty-artifact build;
a retained volume is reused only with verified recorded identity or matching
cluster/member/agent-token ownership. Untagged volumes without recorded IDs
require a token-verified runtime and its exact KARAKORAM_DATA_DIR mount, even
when a partial import left the member planned. A recorded volume ID remains
authoritative: a different service at that hostname is a conflict. All members
are verified before mutations or cleanup, checkpointing newly adopted IDs
before deleting runtimes.
Import/deploy operation IDs are persisted and observed non-blockingly. Deploy
uses the real agent artifact, exact package pins, temporaryShutdown and the
absolute readiness executable.

Each replica set progresses planned -> users -> ready: bootstrap initiates all
configured members, follows the elected primary's agent for localhost-exception
user creation, and accepts authenticated verification of an existing userAdmin
after a lost response. Authenticated userAdmin reconciles PBM and system roles.
The config-server replica set must reach ready before any mongos deployment.
Shards use shardsvr; mongos receives cfg/<config-hosts> and registers new shards.

Scale-up creates fresh monotonically indexed members. A member enters
healthy -> syncing through a non-voting, priority-zero reconfig. It becomes
active only after SECONDARY/PRIMARY health and replication lag <=10 seconds
are observed and its vote is promoted. Scale-down marks highest indexes,
processes them highest-first within each set, transfers a primary before
removal, checks old and new voting majorities, then removes the member's
runtime, volume, public routing and secrets. Shard removal and topology-type
changes are rejected. Adding shards follows the initial replica-set bootstrap
and addShard path. Router replica changes use SetContainerCount; CPU/RAM
updates send complete runtime resources; persistent disk growth targets the
volume service rather than the ephemeral runtime root disk.

Version changes use deployed per-member versions rather than assuming an
accepted deployment changed every process. A member enters upgrade-wait after
deploy and returns active only after the agent reports the pinned binary and
replication catches up. Secondaries precede primary stepdown; config servers
precede shard sets, which precede routers. The sharded balancer's prior state
is saved, stopped once and restored only after binary cutover. A next-major
cutover requires the old-major FCV; binary upgrades never implicitly raise
FCV. Raise-FCV is a separate operation that checks every deployed and observed
binary's major, and sets confirm:true for 7.0 and later.

TLS/config/RAM cache changes advance configVersion, but MemberConfig exposes
only an individually activated member snapshot. Before one reload the
provider checks quorum, transfers a primary when needed, and returns a
publication intent for the reconciler to commit. A later Sync publishes the
member's secrets/snapshot and invokes /v1/reload; crash recovery retains that
member's intent and a fresh acknowledgement version. Other agents cannot pick
up the change early through their periodic pull. A newer desired configuration
can repair the same waiting member even if its rejected configuration stopped
mongod, without unblocking another member. Disabled/required TLS transitions
pass through preferTLS on every member before the final mode is activated.
PBM maintenance/stopped-process maps are reflected in agent desired state.
Controller-issued member and HTTPS agent certificates are renewed within thirty
days of expiry. Renewals are staged in separate secrets, published only after
that member's disruption check, and block the next reload until configuration
acknowledgement, process health and replication recovery are observed. Supplied
member leaves remain operator-owned and report impending expiry through the
TLSCertificate status condition rather than automatic renewal.
Provider validation and agent rendering share one managed-key override policy.
WiredTiger cache is max(0.25, 0.5*(maxRAM-0.5)) GB.

Backup/PITR configuration uses PBM EnsureConfig with its applied hash. A
post-restore fresh base backup is uniquely enqueued, and PITR remains disabled
until it or a later snapshot on primary storage succeeds. Failed, cancelled or
missing bases receive at most three replacement attempts with warning events;
exhaustion leaves PITR gated, not cluster convergence, until a manual base succeeds.
Optional public routing requires CIDRs; ports are reserved project-wide from 27017
using persisted cluster plans, a process-local mutex, and every project's existing platform public-port route.
IPv6 connection hosts use bracketed addresses. Sharded clusters publish only
mongos services and remove obsolete shard/config routes.
Public and localhost IPs are issued certificate SANs. External replica-set discovery still requires clients to map
advertised private member hostnames; a directConnection to a public member
can be used where such DNS mapping is unavailable.

A dataSource queues one deterministic Restore record and operation after
initialization, with SourceCluster preserved. The terminal Sync path removes
retained-volume bookkeeping, observes the desired generation and becomes
ready. Pending work requeues in five seconds; a converged cluster requeues
in thirty seconds. At most one disruptive database/infrastructure action is
performed by a call; primary stepdown returns without also restarting it.

# Imperative operations and cleanup

Backup/delete-backup/restore delegate to PBM using the selected backup storage,
not necessarily the cluster's primary storage. Restart checkpoints completed
members and a currently waiting member, checks quorum and processes
secondaries before a primary. Replacement uses a new monotonic index,
non-voting initial sync, old-member removal, then promotion; it does not
attempt to create an eighth voting member. Singleton replacement instead
promotes the caught-up replacement before primary transfer/removal, because
a non-voter cannot take over the sole primary. Password rotation stages a new
secret, updates one replica set per checkpoint (retrying userAdmin with the
staged credential after a crash), persists the cluster credential, then
reloads backup/monitor credentials one member at a time. TLS rotation issues
and reloads one member certificate at a time. Restore hooks reset system users
after both in-place and clone recovery through the preserved target PBM account,
or encrypted snapshot-time userAdmin credentials for physical recovery, while
the whole router service remains stopped. Backup provenance (destination prefix,
topology/version and protected recovery-identity reference) survives source deletion.
Running replacements opt into Sync only for join/removal; other operations,
especially restore, exclude ordinary convergence.

Cleanup deletes snapshots/PITR at all known backup destinations while agents
are still running. It then deletes public routing and runtimes, observing each
operation before proceeding. keepVolumes preserves volume records and all
credentials, sets Suspended, and the resume Sync path recreates runtimes on
the same volumes. Full deletion removes volumes and provider-owned secrets /
agent tokens. Placement always describes fingerprints as best-effort and
warns about unknown fingerprints or apparent co-location; no anti-affinity
or physical-host failure tolerance is claimed.
Failed provisioning operations and their backoff do not block deletion; failed
deletion operations remain visible errors and retry under the same policy.
Backup cleanup visits destinations actually used by PBM or recorded
backups, not a desired storage that was never configured during failed creation.
*/
package psmdb
