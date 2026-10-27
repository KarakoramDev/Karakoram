<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL consumer bindings

A binding is a durable dependency from a managed consumer to a `postgres` source cluster in the same control plane. Consumer providers create and release bindings during reconciliation; operators inspect them on the source's **Bindings** tab. [Keycloak](https://www.keycloak.org/) is the sample consumer. [Temporal](https://temporal.io/) and [OpenBao](https://openbao.org/) can use the same in-process binding contract; a binding is not an external-database import or an assertion that those providers are installed.

`pgbinding.BindingID(request)` is deterministic before provisioning: `pgb-` followed by the first 16 bytes of SHA-256 in lowercase hexadecimal, hashing consumer kind/namespace/name and source namespace/cluster separated by NUL bytes. Empty namespaces normalize to `default`. Database, extension and version-range changes do not change this URL-safe ID. Consumers must call `Release(ctx, pgbinding.BindingID(request), keepData)` after removing runtimes even when `Ensure` never succeeded. Releasing an unknown ID is a successful no-op; partial provisioning does not strand a source deletion guard.

## Ownership and credentials

Each binding owns one login role and one to eight databases. Its reserved `kb_` role has no superuser, database-creation, role-creation, replication or bypass-RLS privileges or memberships. Requested extensions must be available on the source. The role owns the application databases and their public schemas; unrelated consumers should use separate bindings.

Binding databases do not retain [PostgreSQL](https://www.postgresql.org/)'s default `PUBLIC CONNECT` and
`TEMPORARY` privileges. Karakoram revokes both from `PUBLIC` and explicitly grants
them to the owning binding role, so another consumer's `kb_` role cannot connect
or create temporary tables there. Explicit product grants, such as `CONNECT` for
enabled Data MCP readers scoped to that database, are preserved; administrative
superusers retain access. New databases start with connections disabled; the
maintenance connection commits the restricted ACLs and enables connections
together before public-schema ownership and extension installation. Interrupted
creation resumes against that same closed database.

After a control-plane upgrade or restart, existing ready bindings are repaired
automatically on their next reconciliation or credential resolution. After the
revoke commits, Karakoram terminates existing sessions whose non-superuser login
role no longer has `CONNECT`, including sessions holding temporary tables.
Owner, explicitly granted reader and administrator sessions remain connected.
Repair is not complete until no unauthorized session remains.

Successful repair is cached per binding incarnation, credential generation and
source bootstrap/restore/repository generation and spec generation. In-progress
recovery cannot publish a successful cached repair; a fresh source revision is
checked before caching. Untracked native database replacement is revalidated on
the next reconciliation after the five-minute cache interval expires; this is
not a hard exposure-time bound. There is no SQL on every status poll. Repairs
are serialized per binding, not across all sources. Idle binding lock entries
are reclaimed after their holder and all waiters leave; successful repair
results are cached separately. Credential resolution gives repair at most two
seconds or a quarter of its remaining deadline, including waits for the source
credential-mutation lock. Transient repair failures retry without marking a
healthy ready binding unbound; ownership conflicts fail closed.

The cleanup ID remains deterministic, but each newly inserted binding row receives a random, durable incarnation nonce. The native role name hashes the binding ID and that nonce; creation receipts, mutation request identities and rotation secrets are scoped to that incarnation. Retries reuse the persisted incarnation, while recreating a released consumer creates a different role even with the same consumer/source names.

Passwords are generated and sealed by the control plane. Only the managed consumer resolves the credential reference and projects it into sealed member configuration. Neither list nor rotate returns a password or connection URI. Do not modify binding roles through the ordinary PostgreSQL access API or direct SQL.

Rotation, keep-data disable and role deletion first verify the sealed creation receipt against the native PostgreSQL role OID. Every binding role mutation carries that expected OID in its journaled request; the agent locks the role catalog and checks identity in the same transaction as `ALTER ROLE` or `DROP ROLE`. A removed/replaced role is never adopted by name: identity mismatch rejects the operation and marks the binding `conflict`. Resolve the ownership conflict before retrying; Karakoram does not modify the foreign replacement. A committed drop can be replayed safely when the original role is absent.

The binding role has an explicit 100-connection limit. Sources must use TLS; consumers verify the source certificate (`verify-full`). HA sources require a writer proxy; proxy-free connections are supported only for single-member sources.

## Inspect and rotate

```sh
karactl -n NS postgres bindings list SOURCE -o json
karactl -n NS postgres bindings rotate SOURCE BINDING_ID --yes
```

The corresponding endpoints are `GET /clusters/{cluster}/postgres/bindings` and `POST /clusters/{cluster}/postgres/bindings/{binding}/rotate`, under `/api/v1`, with the namespace query parameter. List requires `database-users:read`; rotation requires `database-users:update`, with the same additional cluster authorization metadata as PostgreSQL role password rotation. Session-authenticated mutations require CSRF protection. Supply an `Idempotency-Key` for safe retries.

The metadata includes consumer kind/namespace/name, databases, extensions, role, supported minimum/maximum major, credential generation, provisioning state and timestamps. Rotation changes the source password and increments the credential generation. Consumers observe that generation and refresh their configuration on reconciliation. Existing connections may be interrupted; wait for consumer convergence before treating rotation as complete.

## Deletion, recovery and upgrades

A source with inbound bindings cannot be deleted: the API returns a conflict naming its consumers. Delete consumers first. Binding release occurs after their runtimes are removed, dropping the owned databases and role; a keep-data release instead preserves databases and disables the role. Bindings identify a source incarnation, so recreating a source name does not inherit old bindings.

Release only drops databases whose owner OID matches this incarnation's receipt-verified role OID. A keep-data release removes the binding row but retains the old disabled role, its ownership receipt and its databases. Recreating the same consumer does not adopt them: `Ensure` creates a new role and fails closed with a conflict naming the retained database. Deleting that failed consumer removes only its newly created objects; the old role and databases stay untouched.

### Explicit adoption of retained data

Adoption is a deliberate administrative operation, not an automatic reconciliation step:

1. Back up the retained databases and stop any clients that could still access them. Recreate the consumer and let it reach the retained-database conflict. Keep that consumer and binding in place while adopting; deleting and recreating it again changes the destination role.
2. List bindings on the source to obtain the **new** incarnation's `kb_…` role. As a PostgreSQL administrator, verify each retained database and its old disabled owner against the intended consumer. Do not rename or re-enable the old role, copy its receipt, or edit binding rows/secrets.
3. For each intended database, connect to it as an administrator and run `REASSIGN OWNED BY "OLD_ROLE" TO "NEW_ROLE";`, then explicitly set `ALTER DATABASE "DATABASE" OWNER TO "NEW_ROLE";` and `ALTER SCHEMA public OWNER TO "NEW_ROLE";`. Substitute the verified identifiers and quote them as SQL identifiers. `REASSIGN OWNED` affects every object owned by the old role in that database, and shared objects such as databases; review all of that role's ownership first. Never run it for an unrelated shared owner.
4. Retry consumer reconciliation. `Ensure` accepts the administrator-transferred owner OID and repairs the public schema/extensions before enabling the consumer. Review and remove the old disabled role only after verifying it owns no retained objects in any database. Once adopted, a later drop-release of the new consumer intentionally drops the adopted databases.

Alternatively, preserve the old data under different database names (or restore a backup into newly named databases) and configure the consumer to use fresh names. Never drop a retained database merely to clear the conflict unless its deletion is intended.

### Restores and upgrades

An in-place restore overwrites shared consumer data and can roll back role credentials. It returns HTTP 409 with code `bound_consumers` and consumer identities unless the request explicitly sets `acknowledgeBoundConsumers: true`. The UI requires an acknowledgement checkbox after listing affected consumers. CLI convenience requests use:

```sh
karactl -n NS restores create SOURCE --backup BACKUP --acknowledge-bound-consumers --yes
```

For file-based requests, set `acknowledgeBoundConsumers` in the request document. Restoring into a new cluster does not overwrite the existing source. Acknowledgement does not promise application-consistent recovery or credential repair; check source and consumer readiness after restoration.

Restore acknowledgement and binding admission are serialized in the same store writer transaction. A newly created binding is rejected with a retryable conflict while an accepted or in-progress restore targets its source; existing bindings remain readable. A restore through a binding implicitly acknowledges that binding's consumer, but must explicitly acknowledge any other bound consumers.

Consumers declare supported PostgreSQL majors. Source major-upgrade admission rejects targets outside any binding's range. The Upgrades tab lists consumers and maximum majors. Upgrade consumers' compatibility declarations before changing the source major; do not bypass the guard with SQL. Bindings do not provide cross-consumer transactions, coordinated backups or automatic isolation of restore blast radius. Prefer dedicated sources for unrelated services.
