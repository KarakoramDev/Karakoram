# 0012 — Encrypted secrets, split system users and TLS defaults

Status: Accepted

## Context

Karakoram stores database passwords, [S3](https://aws.amazon.com/s3/) credentials, agent tokens and CA/private-key material. Database system privileges should not be collapsed into a root credential used for every operation. Internal authentication, TLS and state-store encryption protect different boundaries.

## Decision

Encrypt each secret value at rest with HKDF-derived AES-256-GCM encryption rooted in a strong operator-held `KARAKORAM_MASTER_KEY` supplied through secret environment, never stored in [SQLite](https://sqlite.org). This is per-value encryption, not envelope encryption with wrapped data-encryption keys. Use HKDF domain separation for derived keys, fresh nonces and authenticated context binding. Store password/API-token verifier hashes where recovery is unnecessary; the agent token also needs an encrypted copy for control-plane-to-agent calls. Preserve the master key separately for recovery (ADR 0003).

Seal additional namespace project tokens and relay client keys with namespace-bound authenticated identities, and the dedicated relay CA/hub keys as installation settings (ADR 0018). Deliver relay enrollment through service env secrets. Non-default storage secrets use a distinct authenticated-data domain to prevent aliasing with legacy default names; validate public identifiers before record lookup. Existing default-namespace secret scopes/AAD remain unchanged during migration; no re-sealing is needed.

Generate distinct fixed system users: `clusterAdmin`, `userAdmin`, `clusterMonitor`, `backup` and `databaseAdmin`, with purpose-specific [Percona](https://www.percona.com)-compatible roles. Use these only for corresponding management, monitoring and [PBM](https://docs.percona.com/percona-backup-mongodb/) work; application users/roles are separate. Rotate system credentials as durable operations and supply agents only locally required credentials.

Generate a random cluster keyfile for member authentication. Default database TLS to `requireTLS` with a self-generated cluster CA and hostname-valid member certificates, allowing password-authenticated clients without client certificates. Support user-provided material and explicit `preferTLS`/`disabled` modes. `preferTLS` permits plaintext client connections: it is not equivalent to mandatory TLS. Keep the control-plane/agent internal CA trust boundary distinct (ADR 0008). Manage renewal/rotation explicitly, without assuming [cert-manager](https://cert-manager.io) exists.

Never return secret values in ordinary status, default URIs or audit events. Only explicit credential reveal can return authorized connection credentials under `read` on `cluster-credentials`; default URIs contain no password. [Zerops](https://zerops.io) tokens stay server-side. Render secret files/config caches with restrictive permissions. Application logs have a bootstrap exception: when `KARAKORAM_ADMIN_PASSWORD` is unset, the generated initial administrator password is logged once. Supply that password through protected service secrets to avoid this fallback; treat bootstrap logs as sensitive.

## Consequences

A copied state database does not expose plaintext encrypted secret values, but this is not whole-database/disk encryption and does not protect against control-plane compromise or a leaked master key. Master-key loss prevents recovery. Least-privilege separation limits routine credential exposure; coordinated rotation and restore authentication preservation remain required. Database data-file encryption is a separate [PSMDB](https://www.percona.com/mongodb/software/percona-server-for-mongodb) capability, not a claim implied by state-store encryption.

## Alternatives considered

- **Plaintext secrets/base64:** no protection for copied state/backups.
- **One root system account:** simpler bootstrap, unnecessarily broad routine authority.
- **preferTLS by default:** accepts plaintext clients for compatibility, but does not enforce transport encryption.
