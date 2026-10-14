<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Upgrade and recover Karakoram

This guide upgrades **Karakoram itself**, not a database engine. Database version
changes and feature-compatibility changes are separate cluster operations.

## Before deploying a new version

1. Read the target version's release notes and any state-schema compatibility
   restrictions. Retain the previous source revision or Zerops application
   version so you can redeploy it if required.
2. Let active provisioning, backup, restore, scaling, and upgrade operations
   settle. Avoid changing cluster desired state during the maintenance window.
   Database processes are independent of the control-plane process, but
   reconciliation, new operations, scheduling, and monitoring pause while it is
   down. Choose a window that does not miss a required backup schedule.
3. Back up the **control-plane state volume** consistently as described below.
   Preserve `KARAKORAM_MASTER_KEY` separately in your secret manager. A database
   copy without its original master key cannot decrypt stored cluster credentials.
4. Keep the same `karakoramdata` volume, service secrets, project ID, and public
   origin. Do not rerun the installer or regenerate the master key.

## Consistent pre-upgrade state backup

There is no `karakoram admin backup-db` command. Use a stopped-writer copy;
ordinary copying of a live SQLite main file, or a live Local Storage tarball, is
not a reliable database backup. SQLite uses WAL, so a standalone copy of
`karakoram.db` can omit committed changes while the server is running.

1. Stop Karakoram through the service dashboard or zcli:

   ```sh
   zcli service stop karakoram -P "$PROJECT_ID"
   ```

   Wait for the stop operation to finish and the runtime to be `STOPPED`. Do not
   continue while an old control-plane process is still writing.
2. Open **karakoramdata → Web shell** in the Zerops dashboard. The Local Storage
   maintenance container exposes the same files at `/data`, while the runtime
   sees them at `/mnt/karakoramdata`. Create an archive **outside** that directory:

   ```sh
   # Run in the Local Storage maintenance container, after the runtime stopped.
   umask 077
   tar -czf /tmp/karakoram-state-before-upgrade.tar.gz -C /data .
   ```

   Copy the whole state directory, not just `karakoram.db`. Include any remaining
   `karakoram.db-wal` and `karakoram.db-shm` sidecars and persisted identity files.
   Only Karakoram may write the directory; do not attach a second running control
   plane while taking this copy.
3. Download the archive using that maintenance container's **File browser**.
   Store it in encrypted off-platform storage and record the Karakoram version
   and backup time. `/tmp` is not durable, and a copy on the same volume alone
   does not protect against volume/host loss. Restrict access to this archive;
   it contains authentication and encrypted infrastructure secrets.
4. If deployment is not immediate, start the old version again:

   ```sh
   zcli service start karakoram -P "$PROJECT_ID"
   ```

   If deploying immediately, leave it stopped for the new deployment.

The [Zerops Local Storage backup documentation](https://docs.zerops.io/local-storage/how-to/manage)
explains the maintenance container and live-tarball limitations. A scheduled
consistent SQLite backup can be added separately using SQLite's native backup
API/tooling; do not assume the base runtime ships the `sqlite3` CLI.

## Deploy the new control plane

For a local checkout, check out the desired published release or your intended
source revision, then run from its root:

```sh
zcli service push karakoram -P "$PROJECT_ID" --setup karakoram \
  --zerops-yaml-path zerops.yaml --no-git --disable-logs
```

For a Git-connected service, redeploy the chosen revision using Zerops's normal
Git deployment controls. `buildFromGit` in the installer is a one-time source
import, not an automatically configured GitHub webhook integration. You may
always use the local push path instead. Before a `--no-git` push, provide both
`KARAKORAM_VERSION` and `KARAKORAM_COMMIT` in the **remote build environment**;
the archive has no `.git`, so Git discovery cannot identify that revision.
Use a release tag/source label and the checkout's `git rev-parse HEAD` output.
These are build-time inputs, not settings that can be changed in a running
binary. The pipeline gives the commit override priority and records a UTC build
date using the same linker fields as `make build`. See
[build provenance](install.md#build-provenance-for-local-pushes) for local builds.
Public `/api/v1/version` and the UI explicitly report
`unknown (built without git metadata)` when provenance was not supplied.

The pipeline rebuilds the UI and static Go binary and deploys the executable to
the existing runtime. `deploy.temporaryShutdown: true` stops the old container
**before** the new one starts, preventing simultaneous SQLite writers against
the mounted volume. Keep this setting and the single-container maximum. A short
UI/API interruption is intentional; do not replace it with zero-downtime overlapping
containers or horizontally scale the control plane.

SQLite migrations run automatically when the new server opens the state store.
There is no separate migration command to run. The deployment readiness probe
checks `GET /healthz` on port 8080 before traffic is switched to the new runtime.
Wait for the deployment operation to finish and for the service to become
`ACTIVE`, then confirm:

```sh
curl --fail --show-error https://YOUR-SUBDOMAIN/healthz
karactl --context production health
```

Sign in and check existing clusters, operations, backup inventory, and member
agent health. A healthy control plane alone does not prove a cluster backup or
restore works. Check the deployed version and logs without exposing secrets.

## Existing single-project installations

Migration **004** runs automatically after upstream actor/audit migration **003**.
Existing clusters, events and metrics become namespace `default`; legacy records and
cluster/storage secret identities retain their old encoding/AAD. **No re-sealing
or master-key change is required.** The default project/token still comes from
the existing service environment.

At startup, stored RBAC object `*` normalizes to `**`, and bare legacy objects
such as `orders*` become `default/orders*`, preserving their old project scope
rather than granting remote-project access. New `namespaces` rules keep bare
namespace names. Submitted policy is normalized the same way. Review the
canonical policy and new namespace-qualified objects in [RBAC](user-guide/rbac-and-auth.md).

The built-in operator/viewer `namespaces, read, **` grants are **fresh-install
defaults**. Upgraded installations retain their stored policy; normalization
does not add these grants. If those roles should see namespaces, add these
rules to the existing complete policy, retaining administrator access:

```csv
p, role:operator, namespaces, read, **
p, role:viewer, namespaces, read, **
```

Without namespace-read grants, the UI shows only `default`, even if the account
can read clusters in other namespaces. Use narrower namespace-read grants
instead if those roles should enumerate only selected projects.

Take the consistent pre-upgrade state backup above **before** this migration.
A pre-namespace binary cannot read the migrated schema: rollback requires
stopping the writer and restoring the complete **pre-upgrade state backup**,
with its original master key, before starting the old binary. Redeploying an old
application version against the migrated database is not rollback.

For enrolled remote projects, the namespace reconciler updates relay executables
to the control plane's digest. Expect brief management interruptions and check
each relay's desired/reported version and connected state afterward. This is
separate from member-agent rollout below.

## Member-agent updates

The control plane uploads its own static Karakoram executable to member services
as the node agent. Existing members do not receive a new binary merely because
the control plane restarted: they receive the current binary on their **next
member deployment** performed by the control plane. Treat the transition as a
mixed-agent-version period. Coordinate agent rollout through normal
control-plane member deployments and inspect member health/version afterward;
do not copy binaries into running member containers or introduce a second
supervisor. Keep protocol compatibility in mind before changing the control
plane while old agents remain deployed.

## Rollback

- If a new build fails before deployment, the current application version is
  unchanged. If deployment fails after temporary shutdown, inspect the Zerops
  process status and service state; do not assume the previous server is still
  running. Start/redeploy the previous application version if necessary.
- Redeploying the old binary is safe **only if it supports the current database
  schema**. Automatic migrations are forward-only; an old binary is not a
  database downgrade procedure. Consult release compatibility notes before
  selecting a previous Zerops application version.
- If the old version cannot read migrated state, stop every control-plane writer,
  retain a protected copy of the failed-version state, then restore the complete
  pre-upgrade archive into the **emptied** state directory in the Local Storage
  maintenance container. Do not merge the restored database with stale WAL/SHM
  files or new-version identity files. Restore ownership to the `zerops` user
  and keep permissions restrictive. Restore the matching master key separately,
  and deploy the old binary before starting it against that restored volume.
- State rollback also rewinds desired specifications, durable operations, tokens,
  sessions, and backup inventory. Zerops services and database contents may have
  changed since the snapshot. Review infrastructure and in-flight operations
  before allowing reconciliation to resume; do not blindly restore old control
  state over newer database operations.
- Karakoram state backups do **not** back up MongoDB data. Recover database data
  using PBM backups/PITR and the corresponding cluster recovery procedures.
  Rolling back Karakoram does not undo a database engine upgrade or FCV raise.

Retain the pre-upgrade state archive, master key, and previous binary/revision
until the new version and member-agent transition have been accepted.
