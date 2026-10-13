<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# 0018 — Multi-project namespaces with outbound relays

Status: Accepted

## Context

Zerops projects have isolated private networks. A project-scoped token permits infrastructure operations but does not make private MongoDB or node-agent endpoints reachable from another project. Zerops has no public container-exec API. One self-hosted control plane must manage additional existing projects without publishing database or agent ports.

## Decision

Represent each project as a **namespace**, with its own project-scoped Full access token and Backend instance. The control plane remains deployed in its own project (ADR 0001), represented by the reserved `default` namespace: private connections remain direct and its token remains environment-managed. Additional namespace tokens are write-only and sealed in SQLite. One project can belong to only one namespace; cluster and backup-storage names are unique within a namespace.

Deploy one outbound relay in each additional project. It runs `karakoram relay`, a subcommand of the same executable, uploaded and deployed like member artifacts. Its private hostname normally defaults to `<control-plane hostname>relay`; names longer than 25 characters use the first sixteen hostname characters, four hexadecimal SHA-256 characters and `relay`. `KARAKORAM_RELAY_HOSTNAME` overrides the default for new enrollments; existing namespaces retain their recorded hostname/SAN. It has one Ubuntu runtime container, private TCP 9443 and no public access. Ownership secrets prevent adoption/deletion of unrelated services. The reconciler redeploys an executable whose digest differs from the control plane's.

The relay dials the control plane's HTTPS public origin at root `/relay/v1/connect` using HTTP/1.1 WebSocket Upgrade through the L7 balancer. Binary WebSocket messages carry **inner mutually authenticated TLS 1.3**, then **smux protocol v2** multiplexes bidirectional TCP streams. Outer HTTPS verifies the public origin; inner TLS protects tunnel plaintext even across L7 termination and its HTTP forwarding hop. A dedicated installation relay CA issues a hub leaf and namespace-identified client leaves. Admission verifies the CA, namespace identity and enrolled certificate serial; one active session per namespace replaces an older session. Enrollment certificate/key/CA are delivered through Zerops service **env secrets**, never pipeline envVariables or API responses. Client renewal starts within thirty days of expiry.

Control-plane-to-relay streams dial only current namespace member/router targets published by their engine provider, on a fixed set of engine client/administration ports (agent 9443, MongoDB 27017, RabbitMQ 5671/5672/15671/15672, Elasticsearch stack 9200/5601/9600/8200/5044/8080); cluster-internal ports such as Erlang distribution are never relayed. The relay enforces the pushed allowlist, bare hostname requirement, reserved-host exclusion and DNS-address checks; it dials checked numeric addresses to avoid DNS rebinding. Policy is pushed on change/reconnect and refreshed every thirty seconds. The agent-facing private listener forwards agent config pulls back to the existing control-plane internal TLS server. Agent-to-control-plane TLS remains **end-to-end** and member-token authentication is unchanged; the internal server leaf includes the relay hostname SAN (ADR 0008).

Use smux keepalive interval/timeout of 15/45 seconds and WebSocket pings every 20 seconds with a five-second timeout. Reconnect uses exponential backoff with jitter, capped at thirty seconds. Pre-authentication admission has thirty-two concurrent pending slots and a ten-second TLS/introduction deadline; policy acknowledgment has its own bounded interval, not a ten-second total admission lifetime.

Restore a **1-MiB per-WebSocket-message read limit** after constructing the byte-stream adapter, and cap active data streams at **256 per session**, excluding the control stream. These limits bound framing/admission, not total streamed payload. Runtime hub/client leaf renewal starts within thirty days of expiry.

Namespace-qualified RBAC objects and an explicit source `backups/export` permission protect cross-namespace restore/clone (ADR 0013). Backup storage belongs to a namespace; cross-namespace recovery uses the source backup's recorded storage. **Backup data transfers directly between members and S3**, not through the relay.

## Consequences

The control plane's public HTTPS origin becomes a management dependency for remote namespaces. Each relay is a single-container, per-project single point for management traffic, but **not** for database replication or already-running member-to-S3 backup transfers. Cached member configuration preserves existing process startup when management is unavailable; first configuration still requires connectivity. Namespace failures do not require stopping unrelated namespace reconciliation.

Streams share an ordered transport and failure domain. TCP half-close is not propagated: EOF closes both sides. This is adequate for administration/PBM control, not a general-purpose VPN or bulk-backup transport. Public endpoint denial of service remains a risk despite bounded handshakes. A compromised control plane has authority across all enrolled namespaces; namespace project administrators remain trusted infrastructure operators. Removal is guarded by clusters, storages, retained backup records and orphaned volumes, and does not remove RBAC policy rules automatically.

Admission is fenced/rechecked at session publication, and deletion revokes both active sessions and pending admission. Hub/client leaves renew during runtime; message and active-stream bounds complement pending-handshake limits. These controls do not eliminate public DoS or compromise of a trusted endpoint. Project DNS/network remain trusted, and end-to-end MongoDB encryption still depends on database TLS. See the [relay threat model](../security.md#relay-threat-model).

## Alternatives considered

- **Public database/agent ports with IP allowlists:** expands exposure and depends on public addresses/discovery; allowlists are not authentication.
- **VPN/WireGuard:** provides general private connectivity but adds network administration, keys and deployment dependencies beyond the bounded management path.
- **Zerops-native cross-project private networking:** would simplify routing, but is not an available platform capability.
- **One control plane per project:** retains direct networking but duplicates state, upgrades, identities and operations.
- **HTTP/2 CONNECT:** plausible transport, but ordinary WebSocket Upgrade has measured L7 compatibility; do not depend on unverified proxy CONNECT behavior.
- **yamux (MPL-2.0) instead of smux (MIT):** both multiplex streams; choose smux's permissive MIT license and the exercised protocol implementation. This is a dependency choice, not a claim that MPL-2.0 is incompatible with Apache-2.0.

## Evidence

Measured in a test deployment using WebSocket, inner TLS 1.3 and smux v2 through the public L7 origin:

| Probe | Observation |
| --- | --- |
| Admission | HTTP/1.1 Upgrade and inner TLS 1.3 established; unrelated-CA clients and trusted-CA clients with a wrong pinned leaf were rejected without disturbing the legitimate session |
| Idle/longevity | One unchanged session lasted at least **31 minutes 23.882 seconds**, including three application-idle intervals of about **605 seconds** each, with zero reconnects |
| Multiplexing | **20/20** concurrent streams byte-verified 1 MB each; initial aggregate payload rate **21.488 MB/s**, RTT p50/p99 **4.460/12.031 ms** |
| Throughput | One 50 MB echo transaction: **12.358 MB/s**, RTT p50/p99 **0.746/3.075 ms**; eight concurrent transactions: **47.974 MB/s**, RTT **0.938/3.710 ms** |
| Restart recovery | First observed recovery after relay restart **8.663 seconds**; after hub restart **15.737 seconds** |
| Direction/policy | Reverse-initiated streams passed before and after the idle window; a disallowed target was refused before dialing |

Rates count original payload once and include stream establishment and small-message phases; they are test transaction rates, not saturated one-way limits. The test establishes transport suitability for administrative/PBM control, not MongoDB/PBM integration or a deployment SLA. It did not characterize multi-hour/day L7 behavior, actual idle timeout or maximum WebSocket message size. Production serial-bound enrollment differs from the test's explicit leaf pins; local regression tests and the environment-gated multi-namespace scenario are described in [verification](../verification.md).

See [architecture](../architecture.md), [namespace lifecycle](../user-guide/namespaces.md) and [security](../security.md#relay-threat-model).
