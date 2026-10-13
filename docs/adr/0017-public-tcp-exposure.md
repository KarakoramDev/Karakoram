# 0017 — Optional Zerops public TCP routes and discovery-safe endpoints

Status: Accepted

## Context

Private project hostnames are not externally resolvable/reachable. Zerops supports runtime public TCP routing with firewall ranges over project IPv6 or dedicated IPv4; shared IPv4 and the HTTP L7 subdomain are not MongoDB TCP endpoints. Replica-set clients discover member addresses beyond the initial seed URI.

## Decision

Keep databases private by default. When explicitly enabled, reconcile Zerops public-port routes to internal TCP 27017 with the requested CIDR allowlist. Default to IPv6; dedicated IPv4 is an explicit project capability/cost prerequisite, not a substitute using shared IPv4. Allocate collision-free public ports on the selected project IP and persist the route-to-member mapping. De-exposure/delete removes owned routes without affecting unrelated project routing.

For a replica set, publish a reachable endpoint for every member required by external discovery, not just the primary or one load-balanced seed. Coordinate external DNS, port mappings, replica-set advertised addresses and certificate SANs. Keep private addresses usable by members and the control plane. Where split horizons are used, external DNS names and TLS/SNI-capable clients must select the external horizon; bare IP seeds, plaintext clients under preferTLS or an unchanged private `hello` response cannot be assumed to work. Do not report a usable public connection until discovered endpoints are actually externally reachable. Show missing DNS/certificate/horizon prerequisites and the difference between routing provisioned and connection validated.

For sharded clusters, expose client-facing mongos routes only; keep config servers and shard members private. Mongos supplies the application routing boundary, so clients do not need public discovery of every shard member. Endpoint details/URIs contain no passwords unless the separate reveal permission is exercised (ADR 0012).

## Consequences

Public exposure expands the attack surface and requires explicit firewall/TLS planning. IPv6-only clients need IPv6 connectivity; dedicated IPv4 has platform cost. A TCP route alone does not make replica-set failover usable. Keep the default `requireTLS`; opting into `preferTLS` permits plaintext client connections. Changing exposure/DNS can require coordinated topology/certificate reconciliation. Public connectivity is a separate acceptance test from private cluster health.

## Alternatives considered

- **Private network/VPN only:** safest default, but does not meet optional public-access requirements.
- **HTTP subdomain/shared IPv4:** unsuitable for MongoDB wire protocol.
- **Single primary route or generic load balancer for a replica set:** breaks normal discovered-member/failover semantics.
