<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# 0068 — Temporal on Zerops private IPv6

Status: Accepted

## Context

ADR 0056 runs Temporal as stateless combined or split roles on bound PostgreSQL.
The initial agent selected its replica address by opening an unconnected-to-data
UDP route to the writer with `udp4`, required a private IPv4 local address and
bound all roles and the UI to `0.0.0.0`. Commit
`22349adb692ff1674c092461a0b9877c512bb8b6` (`temporal: reject backends without
private IPv4 networking`) added a shared admission guard before intent is
persisted. Zerops declares private IPv6, so creates, updates and dry runs were
rejected. This was a Karakoram agent networking limitation, not a requirement
that Temporal itself use IPv4.

The pinned Temporal versions 1.31.4 and 1.32.1 support numeric IPv6 broadcast
addresses and bracketed endpoints: their membership factory parses broadcast
addresses with `net.ParseIP`, joins listener addresses with `net.JoinHostPort`,
and listens on `tcp`; membership bootstrap and service resolution use
`net.SplitHostPort`/`net.JoinHostPort`. PostgreSQL `cluster_membership.rpc_address`
is `VARCHAR(128)`, not an IPv4-sized field. These are the official native
artifacts already in the lock; no replacement image or patched binary is needed.

Zerops already implements the other requirements through `infra.Backend`:
stateless Ubuntu runtimes, per-role TCP ports, horizontal replicas, private
service DNS, native-artifact preparation and executable readiness. Temporal's
durable state stays in its bound PostgreSQL databases; the Temporal roles need
no volume. Frontend publication is deliberately private. Optional public UI
routing uses the existing HTTP/subdomain capability and OIDC policy.

## Decision

Admit known private IPv4 and IPv6 layouts. Publish an optional `ipv6` boolean in
`tspec.Spec`, derived only from `Layout.PrivateAddressFamily`. For IPv6, agents
select `udp6` when discovering the local writer route, require a private IPv6
local address, advertise that replica-specific address and bind server roles and
the UI to `::`. There is no cross-family fallback and no shared service hostname
substitution for membership. The UI and SDK clients still dial private frontend
service DNS. Local frontend HTTP/gRPC administration and UI readiness use `::1`;
metrics remain IPv4 loopback-only. The existing renderer already brackets SQL
and advertised endpoints with `net.JoinHostPort`.

Keep the server/visibility databases, SQL driver settings, TLS identities, role
layout, ports, health checks, artifacts and provider interface unchanged.
Dokploy and Nomad retain IPv4 behavior.

### Rolling compatibility

New agents advertise `temporal-ipv6` on every authenticated configuration pull,
including the first. New control planes withhold the entire IPv6 configuration
from agents lacking that request-scoped capability. Internal reconciliation reads
the saved envelope independently of pull negotiation, so an old agent cannot
block publication or lose its transport through a partially downgraded payload.
New control plane + old agent therefore remains unavailable until agent rollout,
not silently misconfigured. Configuration pulls return a safe HTTP 409 resource
conflict classified as `AgentUpgradeRequired`, naming `temporal-ipv6` and the
member-agent upgrade action; no part of the saved envelope is returned.
The internal endpoint includes that safe diagnostic in the legacy `error`
string decoded by v0.4.3 agents, alongside the structured error fields.
IPv4 envelopes retain the old schema because `false`
is omitted. Old control plane + new agent keeps the default IPv4 policy; old
control planes continue rejecting Temporal on Zerops. No agent HTTP endpoint,
shared infrastructure type or public API changes.

## Consequences

Temporal can be created on Zerops with combined or split roles. Each replica,
peer, bound source writer and application worker must have the documented
private-family reachability. A public project IPv4 address is not private IPv4
membership and does not replace that requirement. This does not introduce public
frontend routing, an overlay, an external PostgreSQL workaround or source-owned
storage changes.

Regression coverage includes backend and HTTP admission, IPv6/IPv4 rendering,
private route selection, capability gating, and fake Zerops-layout reconciliation
of both topologies and both locked server versions through the real Zerops YAML
renderer. This is offline implementation evidence, not live deployment
acceptance. Live workflow execution, task routing across replicas, source
failover/recovery and optional UI OIDC still require provider qualification.

## Alternatives

- Keep the IPv4-only guard: safe but unnecessarily excludes a supported native
  membership address family.
- Infer the family from DNS/default routes: dual-family DNS can select an address
  that does not match the backend's private reachability. The existing layout is
  authoritative and does not need another discovery policy.
- Add IPv4 overlays or public addresses: changes the platform/trust boundary and
  is unnecessary when the pinned Temporal runtime accepts IPv6 directly.

## Source references

- [Pinned Temporal 1.32.1 membership factory](https://github.com/temporalio/temporal/blob/af08a5e5f2dd40f545f2afac7780daaa6987001e/common/membership/ringpop/factory.go)
- [Pinned Temporal 1.31.4 membership factory](https://github.com/temporalio/temporal/blob/9a9a1f0fa8a5a4edf29a90c4069352da094920fc/common/membership/ringpop/factory.go)
- [Membership bootstrap address parsing](https://github.com/temporalio/temporal/blob/af08a5e5f2dd40f545f2afac7780daaa6987001e/common/membership/ringpop/monitor.go)
- [Membership broadcast/service endpoint construction](https://github.com/temporalio/temporal/blob/af08a5e5f2dd40f545f2afac7780daaa6987001e/common/membership/ringpop/service_resolver.go)
- [PostgreSQL membership schema](https://github.com/temporalio/temporal/blob/af08a5e5f2dd40f545f2afac7780daaa6987001e/schema/postgresql/v12/temporal/schema.sql)
- [Pinned Ringpop bootstrap host parsing](https://github.com/temporalio/ringpop-go/blob/c317effcdc8bc09ed1c11f00994782bda9685d4b/util/util.go)
- [ADR 0056](0056-temporal-service.md), [Zerops provider](../zerops.md), [Temporal](../temporal/README.md)
