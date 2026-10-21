// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package openbao manages TLS-only OpenBao servers with Raft or opt-in PostgreSQL storage.
// Members progress through planned, imported, deployed, configured and ready.
// An exact credential-free ImportPlan is committed before infrastructure import;
// independent deployment receipts permit at most two in-flight deployments,
// subject to shared backend admission and durable deployment retry limits.
// Runtime and volume ownership require recorded IDs, import receipt evidence,
// ownership markers, or exact mounts from an independently verified runtime.
// Hostname equality never authorizes adoption or deletion.
//
// Initialization has one durable owner, elected before any init RPC. Its
// X25519 recipient key is sealed in SecretStore before requesting initialization.
// The member retains an encrypted, cluster/member-bound receipt until the
// control plane commits the Shamir material, creates management access and
// acknowledges the receipt. An initialized server without recoverable material
// fails closed. No other member is initialized. Plaintext shares and tokens
// exist only in the control plane's encrypted secret store and native API calls;
// they never enter member configuration, runtime environment or durable state.
//
// The control plane reobserves seal status and unseals restarted members. Native
// formation remains serial: a follower's durable Unsealing fence is cleared only
// after Raft and autopilot report it a healthy voter. Platform deployment and
// agent configuration acknowledgement alone do not establish readiness. Only
// complete native convergence advances the observed cluster generation.
// Snapshot clones wait for initial healthy native formation before preparation;
// only prepared snapshot submission/recovery bypasses ordinary unseal convergence.
//
// Per-member native observations have bounded deadlines and never borrow stale
// health after transport failures. Removals persist step-down, remove-peer,
// configuration verification, local stop, runtime deletion and volume deletion
// boundaries. Replacement joins a healthy voter before retiring the old peer.
// Rolling operations checkpoint completed/waiting members in Operation.Checkpoint;
// restart-required convergence records its single current change on the member.
// Standbys precede the active node, and TLS rotation only reloads certificates.
// Quorum and leadership are rechecked at each disruptive rolling boundary,
// including platform retries after backoff and persisted apply checkpoints.
// Established sealed peers recover through a durable serial unseal fence before
// a blocked change or retry proceeds; missing observations retain that fence.
// Formation retains ownership of new followers, and rolling wait retains its
// current runtime until it recovers. Lost-volume replacement alone owns the
// affected member's recovery gate while its new peer joins before retirement.
//
// Raft snapshots stream directly between the CA-verified native API and guarded
// S3 storage, bounded to 4 GiB. Immutable UID-scoped object locations and upload
// HEAD recovery keep interrupted backups resumable without holding whole snapshots.
// Restore checkpoints contain stages only. Foreign snapshots require the source
// shares before snapshot-force: OpenBao 2.7.1 seals the target, rejects its former
// keys, and accepts the source keys. Members are re-unsealed serially. If a
// foreign Shamir restore leaves cached seal configuration behind, an already
// sealed member is restarted with a persisted request/member generation fence;
// another member waits until it recovers. Lost reload replies never bump that
// generation again. Management access is regenerated, and only authenticated
// inventory of the armed snapshot marker proves import. Deleted-source restores
// require explicit write-only shares, staged in SecretStore and consumed on success.
// A pending target initialization receipt does not authenticate restored shares:
// material without a bootstrap root token recovers management via generate-root,
// retaining the receipt until normal acknowledgement and revoking the temporary root.
// Shares-based generate-root uses only the private recovery listener (8202)
// with an independent per-cluster control-plane client CA and mandatory mTLS.
// Members receive only its public CA; signing/client keys stay in SecretStore.
// API/metrics keep legacy generation disabled, and no API fallback is attempted.
// PostgreSQL-storage clusters cannot use Raft snapshots; PITR is not supported.
//
// Whole-cluster suspension stops reachable local processes before deleting any
// runtime and preserves volumes, CA, member tokens and initialization material.
// Runtime deletion must become terminal before a volume is retained or deleted.
// Lost volumes are never silently initialized or reconstructed from hostnames.
//
// The package's extension boundaries are deliberately native: rolling workflows
// bump member.ProcessGeneration one member at a time, then use nativeUnsealStep
// and healthy-voter gates; membership removal must precede runtime deletion.
// RunOperation owns imperative backup and rolling workflows. Access implements
// engine-owned customer access separately from generic DataPlane methods.
// validateSealAndStorage is the admission seam for optional seal/storage modes.
//
// Transit and AWS KMS auto-unseal are immutable creation choices. Their write-only
// credentials are extracted before persistence; recovery shares stay in SecretStore.
// Auto-unseal members never receive Shamir unseal calls. Credential rotation rolls
// members serially. PostgreSQL storage requires ProviderOptions.Bindings; the server
// composition root must inject that service before advertising this integration.
// The binding owns the openbao database; OpenBao 2.7.1 creates its KV and HA tables
// itself (skip_create_table defaults false). PostgreSQL members are HA peers without
// Raft membership or snapshots; binding release follows terminal runtime deletion.
package openbao
