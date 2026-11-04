<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# OpenBao backups and recovery

## Raft snapshots

Integrated Raft clusters support **`raft-snapshot`** only; there is no PITR. The control plane streams a native snapshot over CA-verified HTTPS to registered [S3](https://aws.amazon.com/s3/)-compatible storage, without buffering the complete snapshot. Objects use `<prefix>/<clusterUID>/raft/<backup>.snap`; the recorded location remains authoritative if storage configuration later changes. Snapshot streams are bounded to **4 GiB**. Protect the source unseal/recovery material independently: a snapshot alone is not enough to recover a sealed server.

[Register storage](../backups.md#register-s3-storage) in the cluster namespace, then:

```sh
karactl -n NS backups create secrets beforechange --type raft-snapshot --storage archive
karactl -n NS backups list secrets
karactl -n NS backups get secrets beforechange
```

Acceptance is not completion. Follow the returned operation and require backup `phase: Succeeded` before recovery. Upload checkpoints and S3 HEAD recovery allow interrupted uploads to resume. The **control plane**, not the members, needs access to the S3 endpoint.

For schedules, patch `spec.backup` (the schedules array replaces the previous array):

```json
{
  "spec": {
    "backup": {
      "storage": "archive",
      "schedules": [
        {"name": "nightly", "cron": "0 2 * * *", "type": "raft-snapshot", "retainCount": 7, "enabled": true}
      ]
    }
  }
}
```

Use `clusters patch secrets -f backup-settings.json --wait`. Cron is UTC; count retention keeps successful backups of that schedule. Each snapshot is independent. Live and retained-backup deletion use the recorded object identity; keep the storage registration and credentials until recovery/cleanup is complete. See [shared retention and deletion](../backups.md).

## In-place restore

**Restore replaces target [OpenBao](https://openbao.org/) state.** Quiesce writers, plan service interruption and preserve another recovery copy. Restore requires a successful OpenBao `raft-snapshot`; PITR timestamps are rejected.

```sh
karactl -n NS restores create secrets recovery --backup beforechange --yes
karactl -n NS restores list secrets
```

Same-incarnation restore uses the normal native Raft restore endpoint and retains unseal material. If the snapshot predates the management token, Karakoram generates temporary root access from the stored shares, creates a replacement management token and revokes that temporary root token. Restore owns recovery while normal synchronization is paused: it first re-unseals any sealed target members **one at a time with the current target shares**, then prepares import. Auto-unseal targets wait for their configured seal service. Completion waits for native convergence. Verify recovered secrets, native policies/tokens and application access before reopening traffic.
Forced-restore retries reset incomplete share attempts before switching candidates and stop on transport or permission errors. When a foreign Shamir snapshot leaves stale native seal configuration, Karakoram refreshes already sealed processes **one at a time**, persisting each member's exact restart generation before reload so controller restarts or lost replies cannot repeatedly restart it; access recovery still does not prove import without the authenticated snapshot-marker check.
Management-token regeneration uses a dedicated private 8202 recovery listener with independent control-plane mutual TLS, never the customer/API or metrics listener; its client key and CA signing key remain sealed only in the control plane. See the [recovery security boundary](access-and-tls.md#private-control-plane-recovery).

Before import, Karakoram persists a cryptographically random, request-specific marker identity in its encrypted secret store and writes a deny-only `karakoram-restore-<random>` ACL policy into OpenBao. It arms the marker durably only after that write succeeds. The selected snapshot predates this unique policy; importing it removes the policy together with the other post-snapshot state. Following an interrupted POST or lost checkpoint, Karakoram recovers seal/management access and requires a **successful authenticated policy inventory**: marker present means import still needs to run; marker absent proves the prepared snapshot rewind. Sealing, a token lookup returning 403, and token expiry alone never prove import. Inventory/permission errors fail closed. This also handles same-cluster restores whose existing token survives the snapshot. Do not delete or modify these reserved marker policies during an active restore. Successful recovery removes marker policy/staging material idempotently.

## Clone or restore from another source

```sh
karactl -n NS restores create secrets clone --backup beforechange --into-new secretscopy
```

For explicit clone creation, POST `/api/v1/clusters?namespace=NS` with:

```json
{
  "name": "secretscopy",
  "spec": {
    "provider": "openbao",
    "version": "2.7.1",
    "topology": {"type": "cluster"},
    "components": {"engine": {"replicas": 3}},
    "engineOptions": {"seal": {"type": "shamir"}},
    "dataSource": {"cluster": "secrets", "backup": "beforechange"}
  }
}
```

Karakoram first recovers sealed target members with the **current target** material, retains that material encrypted for interrupted-import recovery, and copies the live source's stored unseal/recovery material to the target **before** forced snapshot import. It then serially re-unseals members and recovers management access. A foreign snapshot seals OpenBao and requires the **source** keys, not the target's initial keys. When a POST outcome is unknown, native key rejection permits trying the retained target keys: the control plane may still be talking to the unapplied target state. Key acceptance establishes recovery access, not import completion; the snapshot marker determines whether to replay. Source and target seal types must match. Auto-unseal recovery also requires access to the source's external transit/KMS key; matching a type name alone does not make different keys interchangeable. Clones contain source OpenBao secrets, policies and tokens; isolate their access accordingly.

[Cross-namespace recovery](../backups.md#cross-namespace-recovery) requires source export and target permissions; `spec.dataSource.namespace` selects the source namespace for clone, and `sourceNamespace` does so for in-place restore.

## Deleted source: supply shares

Retained snapshots of a deleted source require write-only **`openbaoUnsealShares`**. This also applies when the former cluster name has been reused: backup provenance includes the immutable cluster UID, not just the name. Use a protected request file, never command-line literal shares.

POST `/api/v1/clusters/secretscopy/restores?namespace=NS`, or `karactl -n NS restores create secretscopy recovery -f restore.json --yes`, with this shape (replace every sample share with real base64 material):

```json
{
  "name": "recovery",
  "source": {"sourceCluster": "oldsecrets", "backup": "beforechange"},
  "openbaoUnsealShares": {
    "shares": ["SOURCE_SHARE_1", "SOURCE_SHARE_2", "SOURCE_SHARE_3"],
    "threshold": 3,
    "sealType": "shamir",
    "recovery": false
  }
}
```

For a clone, the same top-level `openbaoUnsealShares` object accompanies the complete `{name, spec}` create request; it is **not** an `engineOptions` or `dataSource` field. Supply 1–10 distinct nonempty shares, at least the threshold (1–10); the browser rejects duplicates and excess shares before submission. For transit/[AWS KMS](https://aws.amazon.com/kms/) use the corresponding `sealType` and `recovery: true`, with the original external seal key available. Request material is staged only in the encrypted secret store, not spec, operation payload/checkpoint or member configuration, and consumed on success. Do not delete offline recovery material after submitting a request.

## PostgreSQL-storage caveat

The opt-in [PostgreSQL](https://www.postgresql.org/) storage path has no Raft snapshots, Raft restore/clone or OpenBao PITR. Protect and recover the **bound PostgreSQL cluster** and retain OpenBao seal material. OpenBao rejects snapshot schedules and `dataSource` with this storage mode. The standard server supplies the shared PostgreSQL binding service for this mode; [details](operations.md#postgresql-storage-opt-in).
