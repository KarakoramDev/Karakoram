<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# 0066 — PostgreSQL administrator rotation with a sealed transition journal

Status: Accepted

## Context

The revealed `postgres` superuser is also the member agent's management login,
Patroni's superuser, a backup identity, a ProxySQL frontend/probe identity, Citus's
outbound identity and the database identity of selected Supabase components.
Replacing the sealed password before native SQL locks out an agent that only
knows the desired password. Native application-role and binding APIs deliberately
refuse this engine-owned role. A PostgreSQL role supports one password, not an
old/new login grace period.

## Decision

Use the existing asynchronous system-user rotation API and operation for
`postgres`, without changing public request/response schemas. Require the live
member-status `adminRotation` capability on every data agent, including replicas
that may become primaries. Refuse unsupported/unobservable agents with HTTP 409
and `upgrade-agents` guidance, and refuse structural/rolling maintenance conflicts.
The optional member-spec `alternateSuperuser` and `localPeerAuth` fields preserve
old-control-plane/new-agent behavior when absent. `localPeerAuth` is sent only
when the authenticated configuration pull advertises `postgres-local-peer`;
v0.4.3 agents never receive the new field. A control-plane-only upgrade cannot
perform administrator rotation against those agents.

### Password-independent node-local management

The runtime owner already controls the PostgreSQL data directory and can read
all rendered management credentials. All native supervised children inherit the
agent's effective UID; Patroni and pgBackRest already connect over the private
Unix socket. Map exactly that UID's account name to database role `postgres` in
an owner-only rendered `pg_ident.conf`, with first-match `local ... postgres
peer map=karakoram-local` HBA rules. Resolve the account from the effective
kernel UID, never environment-supplied USER/LOGNAME. No other OS account is
mapped, and remote TCP authentication remains SCRAM/TLS. This changes neither
the runtime-owner trust boundary nor customer authentication.

Warm activation keeps the existing native identity file valid. Before publishing
either a single-server HBA or Patroni `pg_hba`, connect using the current working
authentication, read the live identity-file path, atomically add only the
reserved map while retaining other native mappings, and signal configuration
reload. Observe a newer `pg_conf_load_time()` plus the exact valid entry in
`pg_ident_file_mappings`, then persist the observation locally. A failed or
interrupted observation leaves peer HBA unpublished and working SCRAM unchanged;
replay uses the same map without duplicate grants. Subsequent passwordless
connections provide direct live activation evidence. Do not infer activation
merely from a new binary, rendered file, or reload request acceptance.
Open the map's parent once with directory/no-follow flags and verify its owner
and lack of group/other write permission using the opened inode. Read existing
maps relative to that descriptor without following symlinks; require a regular,
runtime-owned file without group/other write permission before preserving even
unchanged content. Unsafe maps fail closed. Publish a mode-0600 exclusive
temporary file relative to the same descriptor, fsync it, rename within that
directory and fsync the directory; pathname replacement cannot redirect publication.

For **every future start**, single-server and Patroni parameters point instead
to the agent-managed `pg_ident.conf` outside PGDATA. Stage that file too before
publishing the configuration; never make the startup identity depend on a
backed-up/restored PGDATA file. Patroni can recreate a replica autonomously from
an old pgBackRest backup or basebackup without another agent Apply in between.
Single restore/clone likewise replaces PGDATA without removing the external map.
The currently active map stays valid until the next restart adopts the stable
external path. The agent does not configure a Patroni `pg_ident` list, so Patroni
does not replace these mappings ([Patroni configuration implementation](https://github.com/patroni/patroni/blob/v4.0.6/patroni/postgresql/config.py)).

For stopped existing data, also inspect the previously configured identity path
with `postgres -C` and stage its map before rendering, covering a Patroni parent
still using its older configuration. Fresh servers receive fully staged matching
identity/HBA files before either PostgreSQL or Patroni starts.

If an already-running server loses its map, recover without first needing SQL:
re-stage the durable last-observed active file and the external startup file,
signal `pg_ctl reload`, and require a fresh passwordless connection before
re-observing/persisting native activation. A vanished old directory does not
authorize a guessed credential or native role change; the live peer probe must
prove recovery. An unobservable server without a trustworthy local observation
remains blocked rather than overwriting an unknown authentication file.


The agent's local connection/recovery rekey first uses that socket with no
password. Patroni's existing `use_unix_socket` and pgBackRest's `pg1-socket-path`
use the same HBA map even while their rendered password is temporarily old.
Legacy or not-yet-active peer configurations retain desired, alternate and
base-backup password fallbacks. `adminRotation` is advertised only after an
actual passwordless local connection succeeds, so native ALTER is never
admitted while Patroni still depends on the rotating local password.

This is deliberately preferable to retaining an unbounded sealed password
history: PITR can restore any intermediate native password after one or several
rotations, including a clone from an older base or in-place recovery to an
intermediate WAL target. Its base-backup recovery password need not match that
catalog. Password-independent local rekey can always apply the target-owned
credentials after recovery, without guessing the restored credential generation.

Seal one journal under `admin/rotation` before native dispatch. It contains the
operation identity, previous password, selected replacement, stable SCRAM verifier,
immutable group order (workers first, coordinator last for new Citus journals),
completed-group cursor and in-flight intent. Existing journals retain their order.
Public operation checkpoints contain only the stage, never credentials. A completed or
rejected journal retains its identity/stage without passwords or verifiers.

1. Stage and acknowledge transition configurations on all members. Peer local
   authentication is attempted first; desired and alternate passwords remain
   fallbacks while peer is unavailable. Ordinary bootstrap role reconciliation
   must not rewrite `postgres` during the transition, including from cached SQL.
2. Discover each writable group primary afresh. Patroni leader-lock and recovery
   checks are repeated by the agent before SQL. On single/HA there is one group;
   Citus needs one transaction per group because role DDL is node-local. Use
   owner-local peer access without password fallback. Disable both Citus DDL
   propagation and metadata synchronization in connection startup parameters,
   before recovery queries or BEGIN, and retain transaction-local guards before
   receipt schema/table DDL. Commit `ALTER ROLE postgres PASSWORD` and the
   existing credential-free operation/hash/OID receipt together.
   Replay observes the receipt and original role identity instead of redoing ALTER.
   Replicas receive role DDL and receipts through native streaming replication.
3. After every group acknowledges, publish `users/postgres`, bump the access
   revision and converge configuration. Keep the previous credential available to
   management while Patroni, backup pgpass, proxy users/probes, health and metrics
   adopt the new desired credential. Writable Citus readiness refreshes
   `pg_dist_authinfo` using the existing transactional authentication reconciler.
4. Require current member/proxy configuration acknowledgements and native
   readiness, plus `CredentialsPublished` when ProxySQL is enabled. Supabase also
   refreshes component payloads and waits for every enabled instance, including
   meta, studio and functions, to acknowledge the new configuration and readiness.
5. Remove the alternate from desired configuration, bump the revision again, and
   await member/proxy acknowledgements before clearing sealed credential material.

The journal is an independent recovery owner: ordinary Sync can resume it after
control-plane restart or operation failure, and failure cleanup retains forward
recovery. A definite rolled-back native rejection on the first group retires the
pending configuration and leaves `users/postgres` unchanged. A lost response,
receipt conflict, changed native identity, or acceptance by any group is not
permission to roll back: retry/inspect the same intent and converge forward.
Pending operation messages identify the group, member, proxy publication barrier
or Supabase component that still needs acknowledgement.

From v0.4.7, the rotation recovery owner can deploy the current agent artifact
on a Citus primary that lacks `adminRotationLocalReceipts`, honoring maintenance
windows and retaining the deployment receipt in the sealed journal. This is
part of the existing rotation, not a second `upgrade-agents` operation. Observe
local peer capability rather than cross-group readiness before resuming SQL;
the latter depends on the very credentials being repaired. After 30 unsuccessful
native receipt attempts, report an actionable error while retaining the journal
for forward recovery. Deployment errors likewise retain the credential intent.
Only definitive terminal deployment observations retire a saved deployment
receipt; unknown/in-progress observations cannot authorize resubmission. Failed
or retry-required deployments re-enter the backend's bounded same-version
deployment lifecycle. Retry exhaustion is recorded through the normal cluster
deployment fence; `retry-deployments` advances its epoch and re-arms recovery.
An already-capable current primary can advance past an obsolete terminal failure.
The journal retains the selected group/member even after a terminal receipt is
retired. Polling and bounded resubmission of that runtime do not require its
agent or PostgreSQL to be running: a failed singleton deployment may have stopped
the only primary. Unknown/in-progress receipts still prevent overlapping work,
and fresh primary/leader-lock checks remain mandatory before any native SQL.
A candidate saved before its first submission is marked separately from a
dispatched runtime. After any maintenance wait, first submission re-observes the
pending group's current primary and reselects it if leadership changed; it does
not roll the former primary replica. The dispatch intent is durable before the
backend call, so a lost response conservatively retains that runtime for offline
repair rather than permitting overlapping disruption of a new target.
New recovery deployments use the retained rotation operation's own maintenance
authorization, never a queued caller's or desired-generation override. If that
operation record cannot be resolved and matched to the cluster, deployment
fails closed while preserving the sealed journal.

On Citus, configuration publication temporarily moves already-joined members
back to `deployed`. The matching sealed rotation owner may continue observing
those members through its own transition, publication and cleanup barriers;
queued operations still require `joined`. This exception does not waive group
formation, member-count, provisioning/recovery, structural/rolling maintenance,
or Citus backup/restore ownership fences.

## Consequences

Rotation itself requires no database restart, including on a single member.
Warm peer activation during the preceding agent upgrade loads the current map
with a reload. Adopting the stable external `ident_file` startup path can require
the ordinary pending configuration restart during that preceding rollout.
Complete the rollout first; admission stays closed until every member proves
password-independent local authentication.
Supabase component refreshes restart their processes and may interrupt service.
Citus can have transient cross-group authentication failures while the node-local
transactions and outbound authentication converge. Workers-first ordering keeps
the coordinator usable with the published credential until its final native
transaction. As on single/HA PostgreSQL, new administrator connections through
ProxySQL can fail between that native transaction and proxy configuration
acknowledgement. Publication starts only after every group receipt; no password
is published early to mask failures. Normal progress is bounded by reconciliation
and configuration acknowledgement, not a guaranteed wall-clock outage limit:
an unavailable member/proxy can extend it and produces recovery diagnostics.
This is not an atomic formation-wide transaction or dual-password grace period.

External clients are not enumerable: they must obtain the newly revealed password
and reconnect. Existing sessions are not terminated, so rotation is not session
revocation or remediation of arbitrary superuser compromise. Bindings use bounded
`kb_…` roles and Data MCP uses dedicated `kk_mcp_ro`/`kk_mcp_rw` roles; neither
inherits the administrator credential. Other managed system identities remain
unsupported on this public rotation route.

## References

[Access and TLS](../postgres/access-and-tls.md),
[Citus authentication](../postgres/distributed.md),
[Supabase operations](../supabase/operations.md),
[PostgreSQL core](0034-postgres-engine.md),
[Supabase composition](0035-supabase-engine.md),
[Citus topology](0046-citus-distributed-topology.md).
