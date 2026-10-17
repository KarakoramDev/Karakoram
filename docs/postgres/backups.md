<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# PostgreSQL backups and recovery

PostgreSQL and Supabase use pgBackRest physical backups and WAL in registered S3-compatible storage. Read [shared storage, schedules, restore and clone procedures](../backups.md), including namespace permissions and endpoint verification. Database backups and control-plane state/master-key backups protect different assets.

## Configure and create

Set `spec.backup.storage` to a storage registered in the target namespace. The default storage provides continuous archiving; a backup/schedule may select another registered destination. Repositories are isolated at `<storage-prefix>/postgres/<namespace>/<cluster>-<uid>`, stanza `kk`. Leading and trailing slashes in the storage prefix are removed from recorded S3 repository keys; pgBackRest receives the corresponding absolute repository path. Members transfer backup/WAL bytes directly to S3; control-plane endpoint verification alone does not prove member connectivity.

```yaml
spec:
  backup:
    storage: archive
    pitr:
      enabled: true
      oplogSpanMinutes: 1
    schedules:
      - name: nightly
        cron: "0 2 * * *"
        type: full
        retainCount: 7
        enabled: true
```

Use a patch file with `clusters patch CLUSTER -f FILE --wait`, or configure the Backups UI. Explicitly choose a PostgreSQL backup type: the generic CLI default `logical` belongs to MongoDB.

```sh
karactl -n NS backups create appdb --type full
karactl -n NS backups list appdb
```

`full` is independent; `diff` needs an existing full; `incr` needs a consistent predecessor chain. Backups run on the current primary with operation-bound durable receipts. Acceptance is not completion: follow the operation and resulting backup status. Once a usable base exists, replicas may bootstrap from pgBackRest with basebackup fallback.

## Retention and deletion

Karakoram schedules own count retention; pgBackRest automatic expiration is disabled. Retention defers expiration of the newest full until a newer successful full exists in that repository. It also preserves ancestors needed by retained backups, including newer backups from other schedules and manual backups. A skipped set remains eligible for a later retention pass, so counts may temporarily exceed the configured limit. Live `delete-backup` uses `pgbackrest expire --set=<label>`; explicit deletion can remove dependent differentials/incrementals and their records, regardless of schedule. The newest full cannot be explicitly deleted until a newer full exists. Failed or rejected expiration fails its operation and releases the serialized queue rather than blocking future backups. Expiration prunes WAL no longer needed by the oldest retained full chain.

After source deletion, retained cleanup uses recorded repository/dependency metadata and checksummed native inventory, and removes the isolated repository when its last set is deleted. Cleanup validates the recorded repository's namespace and cluster incarnation, independently of later changes to the registered storage prefix. Interrupted expiration plans are rebased onto current inventory before resuming, preserving deletions completed by other expiration operations. Keep registered storage and sealed backup credentials available until cleanup/recovery is finished. Externally deleting or changing repository objects can invalidate inventory and recovery.

Retained expiration tracks surviving backups separately for each database-history ID. When the final backup chain for an older major's history is removed, its entire WAL archive-ID prefix is deleted; histories still referenced by backups retain WAL from their oldest required segment onward. A surviving history with no known archive-start boundary keeps its WAL rather than being treated as unreferenced.

## PITR

Enable `spec.backup.pitr.enabled` and configure default storage. `oplogSpanMinutes` controls PostgreSQL `archive_timeout`, defaulting to one minute; the inherited field name does not mean MongoDB oplog behavior. Low-activity WAL switching is not an exact commit-time recovery guarantee.

Restore and clone `pitrTime` inputs use RFC 3339, including `Z` or numeric offsets. Native recovery arguments normalize the instant to UTC with an explicit numeric offset and PostgreSQL's microsecond precision; sub-microsecond fractions are truncated, not rounded forward.

GET `/api/v1/clusters/{cluster}/pitr` reports a conservative interval from the oldest successful consistent base's stop time to a committed primary WAL watermark covered by an archived segment. Archive upload completion time is never treated as WAL-content coverage. Status sampling emits a small committed WAL heartbeat at most every ten seconds, so idle clusters continue advancing with `archive_timeout` (one minute by default). After a member restart or failover, the window remains unavailable until a sample from the current database system and primary timeline is archived. No interval is advertised when disabled or archiving is failing; external removal of repository objects can still invalidate recovery. Without configured storage, the archive command is a no-op and provides no recovery history.

## Restore and clone

**In-place restore is destructive and offline.** Quiesce applications and retain a recovery copy. PITR requests are checked against the source's live conservative WAL watermark before any member is stopped or lineage state changes. Single topology stops PostgreSQL, performs a delta restore and starts/promotes. HA fences every data member, resets only its Patroni DCS scope, restores a seed by custom bootstrap and rebuilds replicas before completing. System identities remain target-owned: recovered database roles are rekeyed to the target's existing passwords, never published as source credentials. Cross-source recovery keeps archiving disabled until target repository configuration and archive-override removal reach every member. When PITR is enabled, a new full is queued; it is not protection until successful.

Restore stop holds survive configuration refreshes and member restarts until the workflow explicitly starts each restored member. ProxySQL administration credentials remain target-local during restore and clone; they are not database catalog credentials.

When recovery reads a repository other than the target's current repository, the target switches to a fresh `<cluster>-<uid>-g<generation>` repository after all members are stopped. This includes repeated cross-source recovery and recovery from an older generation of the same cluster. `stanza-create` initializes the new system identifier before archiving is enabled; timeline numbers may safely repeat across generations. Earlier target backup sets remain listed and restorable with their original repository metadata and sealed recovery credentials. Explicit deletion of these retired sets uses retained-backup cleanup, so even their final full can be deleted without a running writer. A fresh full is queued after every repository-generation change, whether or not PITR is enabled.

Source repository access is read-only in the recovery workflow: no source stanza mutation, archive-push, expiration or backup is performed. Recovery temporarily places the registered source storage's S3 credentials in owner-only target configuration. **These are not automatically scoped read-only credentials:** providers do not universally support temporary scoped keys. A compromised target runtime during recovery can therefore exercise the source key's underlying permissions. Use narrowly scoped source storage credentials and a trusted target runtime; cross-namespace restore authorization does not itself reduce S3 permissions. After recovery, every member refreshes its durable configuration and removes the source restore/action configuration, CA file and bootstrap script before managed client logins are published and the restore completes. Subsequent configuration applies do not recreate source credentials.

During recovery, remote database-client HBA rules and ProxySQL client users are held closed. Local administration and replication remain available for recovery and replica rebuilds. Client rules and managed proxy logins return only after source-only configuration has been removed.

Patroni uses the private Unix socket for local administration and replication
probes; connections between members retain hostname-verified TLS. Custom restore
bootstrap persists its generation marker with a file `fsync`, atomic rename and
directory `fsync`. It does not require permission to synchronize the entire
mounted filesystem.
Singleton restore bootstrap follows the same completion-receipt rule: the mere
presence of `PG_VERSION` never proves a restore finished. A missing, malformed or
different-generation receipt keeps PostgreSQL stopped and resumes pgBackRest
with `--delta`, including for bootstrap generation zero. The completion receipt
is durable before startup is allowed. Bootstrap restores use the operation's
lifecycle rather than the two-minute deadline used for short native commands.
Offline system-role recovery commits durable local WAL without waiting for
replicas that the restore workflow has not started yet. This transaction-local
setting does not change the cluster's configured synchronous policy or reopen
client access.

For a new cluster, `spec.dataSource` names a source backup or PITR target, optionally in another namespace. Use the shared [clone commands and permission rules](../backups.md#cross-namespace-recovery). Requirements:

- Provider matches exactly: postgres→postgres or supabase→supabase, never across providers.
- PostgreSQL major matches; source/target topology may differ.
- Configure target-local backup storage to establish an independent history.
- Source bootstrap is read-only with archiving disabled; target stanza creation and a fresh full establish its own repository afterward.
- Retained-source clones use a sealed snapshot credential only for temporary offline recovery where necessary, never a deleted source's live credential scope or a target-visible source system identity.

Supabase backups contain PostgreSQL data only, **not Storage API S3 objects, sealed function sources or all control-plane-held service keys**. Protect object storage and control-plane state independently, and plan coordinated recovery; a database clone is not a complete application clone.

### Managed application-role passwords

Each backup seals the managed role index and passwords with its recovery provenance. A clone imports that snapshot manifest; an in-place restore deliberately keeps the target's **current** manifest, so a password rotated after the backup stays current. After recovery, password-only access actions reapply those passwords to roles present in the recovered catalog, without changing recovered privileges or memberships. Missing or non-login roles are removed from the proxy manifest rather than recreated. Proxy users are published only after recovery credential cleanup. Roles created directly in SQL still have no managed proxy password.

Keeping current in-place passwords avoids rolling application secrets backward; copying snapshot passwords on every restore would silently break clients after a rotation. Clone passwords instead come from the backup, because a new cluster has no existing application-role identity to preserve. Protect sealed control-plane state and its master key alongside physical backup objects.
