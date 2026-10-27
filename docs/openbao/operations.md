<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# OpenBao operations

## Formation and initialization

Karakoram persists one initialization owner before calling `sys/init`. Only that member initializes; others join its Raft cluster. The member encrypts the initialization response to an ephemeral control-plane recipient key and durably retains only an encrypted receipt until the control plane stores shares and management access and acknowledges it. Retrying recovers that receipt rather than initializing a second seed.

The owner is unsealed and becomes active first. Further members join one at a time; each must become a healthy Raft voter in autopilot before the next. Runtime deployment or an agent configuration acknowledgement alone is not readiness. [OpenBao](https://openbao.org/) active members report role `primary`; unsealed standbys report `secondary`.

```sh
karactl -n NS clusters status secrets -o json
karactl -n NS clusters members secrets -o json
karactl -n NS events list --cluster secrets
```

## Automatic re-unseal

With the default Shamir seal, the control plane observes seal status on every sync (normally every 30 seconds once converged) and sends threshold shares directly over the member's verified native API. Shares never enter the agent's cached member configuration. After a process crash, the supervisor restarts OpenBao sealed, and reconciliation unseals it again. A reachable control plane with its original secret store/master key is required; OpenBao does not autonomously retrieve Karakoram-held shares.

Reveal and store recovery material offline using the [explicit reveal procedure](access-and-tls.md#reveal-and-store-recovery-material-offline). Do not put shares in runtime environment variables, manifests, support bundles or logs.

## Transit and AWS KMS auto-unseal

Select auto-unseal **at creation**. Seal type/configuration, storage and share settings are immutable; seal migration is unsupported. With auto-unseal, initialization produces recovery shares, and Karakoram does not send Shamir unseal calls. Availability instead depends on the external transit/KMS service and key.

Transit `engineOptions` example (replace the example token through a protected request file):

```json
{
  "seal": {
    "type": "transit",
    "transit": {
      "address": "https://transit.example.com:8200",
      "mountPath": "transit/",
      "keyName": "openbao-seal",
      "token": "REPLACE_WITH_TRANSIT_TOKEN"
    }
  },
  "unseal": {"shares": 5, "threshold": 3}
}
```

The external key must already exist, and its token must permit encrypt/decrypt for that key. `address` is HTTPS; optional `caPEM` and `tlsServerName` configure trust. Empty CA uses system roots. Keep this service independent from the cluster it seals.

[AWS KMS](https://aws.amazon.com/kms/) alternative:

```json
{
  "seal": {
    "type": "awskms",
    "awskms": {
      "region": "eu-west-1",
      "kmsKeyId": "REPLACE_WITH_KMS_KEY_ID",
      "accessKeyId": "REPLACE_WITH_ACCESS_KEY_ID",
      "secretAccessKey": "REPLACE_WITH_SECRET_ACCESS_KEY"
    }
  }
}
```

An optional HTTPS `endpoint` overrides the AWS endpoint. The AWS KMS plugin is installed only for this seal. Supply both AWS credential fields together. Transit token and AWS keys are write-only: extracted into encrypted control-plane secrets before spec persistence, never echoed in spec reads. Unlike unseal shares, the seal credentials are delivered to members because OpenBao itself needs them; protect member volumes/configuration too. Replacing write-only credentials with a spec update triggers serialized member restarts; preserve the same nonsecret seal configuration. Do not revoke old credentials until the roll succeeds.

## Scaling and Raft membership

Patch the desired count to **1, 3 or 5**, for example:

```json
{"spec":{"components":{"engine":{"replicas":5}}}}
```

```sh
karactl -n NS clusters patch secrets -f scale.json --wait
```

Scale-up deploys, unseals and verifies each healthy voter before proceeding. Scale-down removes highest indexes first: step down an active victim, remove its peer through the active server, verify native Raft membership no longer lists it, then stop/delete runtime and volume. The join-list update alone does not restart existing members. Autopilot automatic dead-server cleanup stays disabled; Karakoram is the membership writer.

Karakoram refuses unsafe removals with `QuorumBlocked`. Do not manually delete runtimes/volumes to bypass that check. Scaling to one removes redundancy; separate volumes are not a placement guarantee.

## Replace a member

Use the exact member name from `clusters members`:

```sh
karactl -n NS clusters replace-member secrets MEMBER --yes --wait
```

Replacement joins a new healthy voter **before** removing the old peer and deleting the old runtime/volume. A dead member can be removed by its Raft node ID while quorum still exists. Temporary spare capacity and working connectivity to a majority are required. Do not use replacement as lost-quorum recovery.

## Rolling maintenance

```sh
karactl -n NS clusters restart secrets --yes --wait
karactl -n NS clusters rotate-tls secrets --yes --wait
```

Restart and binary upgrade process standbys first, one at a time, re-unsealing and waiting for healthy-voter/autopilot gates. The active member steps down before its turn. Resource/process-trust changes also use serialized convergence. **TLS rotation is a serial certificate reload via SIGHUP**, not a process restart, and does not require re-unseal.

Upgrades are forward-only to catalog-listed versions; inspect `providers get openbao -o json`. Only 2.7.1 is currently listed, so a newer-version upgrade cannot yet be exercised. Never install an uncataloged binary or downgrade data in place. Retain a successful [snapshot](backups.md) before maintenance and follow operation status separately from cluster health.

## PostgreSQL storage opt-in

The provider implements `engineOptions.storage: {"type":"postgresql","postgresql":{"cluster":"appdb"}}` through a [PostgreSQL](https://www.postgresql.org/) binding service: same-namespace PostgreSQL 13+, a dedicated owned database/role, verified TLS, and native OpenBao HA tables. Members are stateless HA peers, not Raft voters. Binding credential changes roll members; deletion releases the binding after runtimes terminate, with data retention following `keepVolumes`.

PostgreSQL peers retain cluster/member/volume identity fencing, but their local volume is not marked as initialized durable native storage: replacing an empty peer disk must not trigger the Raft data-loss guard when all durable state remains in PostgreSQL. Raft peers retain that guard unchanged. An already-initialized external database without recoverable control-plane initialization material or an encrypted initialization receipt still fails closed with `InitializationOutcomeLost`; stateless disk replacement never authorizes reinitialization.

The standard server constructs one PostgreSQL binding service and passes the same instance through `internal/server.engineOptions.Bindings` to `openbao.ProviderOptions.Bindings` (and to [Keycloak](https://www.keycloak.org/), [Temporal](https://temporal.io/) and the bindings API). Provider metadata advertises `postgresql-storage` only when that dependency is present, and the UI gates the choice from that capability; a composition without the service rejects PostgreSQL storage at admission. The native integration scenario `TestNativeControlPlanePostgresStorage` creates a real PostgreSQL role/database and exercises native HA initialization, unseal, persistence across replacement of all local member volumes, and both release retention modes. There are no OpenBao snapshots/PITR in this mode: recovery belongs to the bound PostgreSQL cluster plus retained seal material. Ordinary [PostgreSQL credential connections](access-and-tls.md#managed-database-connections) do not depend on this service.

## Lost-quorum manual recovery

If a majority of Raft voters/data is lost, Karakoram does not reset membership or initialize a fresh seed. Preserve surviving volumes and control-plane secrets, fence old members and follow the upstream [OpenBao Raft recovery procedure](https://openbao.org/docs/concepts/integrated-storage/#manual-recovery-using-peersjson) for `peers.json`, or restore a validated snapshot into a suitable target. This is a manual disaster-recovery operation, not an automatic failover or replace-member workflow. Never run two independently recovered histories with the same intended cluster identity.

## Metrics

The agent scrapes a loopback-only TLS listener on 8210 and republishes a fixed metric vocabulary through Karakoram monitoring. Signals include initialized/sealed and active/standby state, Raft commit/applied indexes and leader contact, autopilot health/failure tolerance, requests/leases and runtime memory. Use the provider's metrics catalog and [shared monitoring guide](../monitoring.md) rather than assuming every upstream metric is forwarded. A sealed server can still be reachable and emit seal metrics; reachability is not readiness. Track freshness and distinguish unknown observations from healthy ones.

## Troubleshooting by condition

| Condition | Meaning and action |
| --- | --- |
| `Sealed` | One or more members are sealed. Check control-plane reachability, CA/hostname validation, secret-store availability and the original master key. For auto-unseal check external transit/KMS reachability, credentials and key availability. Allow reconciliation to retry; do not initialize the member again. |
| `InitializationOutcomeLost` | The owner is initialized but the initialization response/receipt and recoverable material are unavailable. The workflow fails closed. Preserve its volume, encrypted receipt and control-plane state; recover the original receipt/key or offline material. Do not delete the receipt or initialize another seed to make status green. |
| `QuorumBlocked` | A membership or rolling change cannot satisfy native quorum/health safety gates. Restore connectivity or unseal surviving voters, inspect actual Raft/autopilot health and wait for recovery. If a majority is permanently lost, use manual recovery rather than forcing scale-down/deletion. |

Transport errors do not preserve stale healthy observations. Inspect cluster conditions, per-member seal/initialization state, operation checkpoints and bounded logs before changing desired state. Never attach revealed shares/tokens to a support report.

## Suspend, resume and deletion

`keepVolumes` deletion suspends the cluster: reachable native processes are stopped before runtime deletion; volumes, CA, member authentication and initialization material are retained for resume. Runtime deletion must finish before volumes are retained or removed. Use [shared lifecycle procedures](../clusters.md) and observe the existing operation rather than recreating the name. Retained volumes consume storage. Lost volumes are not permission to silently initialize a new secrets store; recover surviving quorum or restore a snapshot.
