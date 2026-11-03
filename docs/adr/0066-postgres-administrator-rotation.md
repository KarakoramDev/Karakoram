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

Activation preserves the existing native `ident_file` path. Before publishing
either a single-server HBA or Patroni `pg_hba`, connect using the current working
authentication, read the live identity-file path, atomically add only the
reserved map while retaining other native mappings, and signal configuration
reload. Observe a newer `pg_conf_load_time()` plus the exact valid entry in
`pg_ident_file_mappings`, then persist the observation locally. A failed or
interrupted observation leaves peer HBA unpublished and working SCRAM unchanged;
replay uses the same map without duplicate grants. Subsequent passwordless
connections provide direct live activation evidence. Do not infer activation
merely from a new binary, rendered file, or reload request acceptance.

The renderer preserves that observed path in member-local PostgreSQL/Patroni
parameters, avoiding the postmaster-only path transition entirely. The agent
does not configure a Patroni `pg_ident` list, so Patroni does not replace the
staged mappings ([Patroni configuration implementation](https://github.com/patroni/patroni/blob/v4.0.6/patroni/postgresql/config.py)).
An unobservable live server blocks activation before any peer HBA publication.
For stopped existing data, inspect the configured path with `postgres -C` and
stage its map before rendering; fresh servers receive fully staged matching
identity and HBA files before either PostgreSQL or Patroni starts. This also
preserves password-independent startup after an offline restore.


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
immutable sorted group order, completed-group cursor and in-flight intent. Public
operation checkpoints contain only the stage, never credentials. A completed or
rejected journal retains its identity/stage without passwords or verifiers.

1. Stage and acknowledge transition configurations on all members. Peer local
   authentication is attempted first; desired and alternate passwords remain
   fallbacks while peer is unavailable. Ordinary bootstrap role reconciliation
   must not rewrite `postgres` during the transition, including from cached SQL.
2. Discover each writable group primary afresh. Patroni leader-lock and recovery
   checks are repeated by the agent before SQL. On single/HA there is one group;
   Citus needs one transaction per group because role DDL is node-local. Disable
   Citus role-DDL propagation inside the transaction. Commit `ALTER ROLE postgres
   PASSWORD` and the existing credential-free operation/hash/OID receipt together.
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

## Consequences

Rotation itself requires no database restart, including on a single member.
Peer activation during the preceding agent upgrade uses the existing identity
path and a reload, not an identity-path database restart. Complete that rollout
first; admission stays closed until every member proves password-independent
local authentication.
Supabase component refreshes restart their processes and may interrupt service.
Citus can have transient cross-group authentication failures while the node-local
transactions and outbound authentication converge. This is recoverable but not an
atomic formation-wide transaction or a dual-password grace period.

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
