# 0016 — Honest placement limits and best-effort co-location detection

Status: Accepted

## Context

Zerops exposes no member anti-affinity/zone-spread guarantee or authoritative physical-host identifier through the container API. Separate Local Storage volumes do not force distinct hosts. Three healthy processes therefore do not prove three independent failure domains.

## Decision

Do not claim host-failure tolerance from replica count or promise placement control Zerops does not provide. Preserve one service/volume per member, but describe Local Storage as single-host and non-HA. Keep replica-set quorum health distinct from physical-placement confidence.

Have agents report a best-effort host fingerprint derived only from available non-secret host-level observations. Group matching fingerprints in cluster placement status and show detected co-location; report unknown placement when a dependable signal is unavailable. Signals such as shared host-global counters are heuristic evidence, not authoritative host IDs, and may change across restarts or platform updates.

Display warnings in creation/topology guidance and the cluster detail UI, and document them in architecture/limitations/operations guidance. Even an apparently spread observation is not a placement guarantee or proof of rack/zone separation. Do not repeatedly destroy/recreate members hoping to force placement or block correct database operation merely because placement is unknown.

## Consequences

Users can distinguish database replication from infrastructure resilience. Co-location can remove multiple voters during one host failure; PBM recovery limits data-loss exposure but does not supply availability. Heuristics can have false matches, missing signals or stale groups, so placement status must show uncertainty rather than certify HA. Guaranteed anti-affinity would require a new platform capability and a revised backend/acceptance contract.

## Alternatives considered

- **Assume separate service IDs mean separate hosts:** service identities do not establish physical placement.
- **Automatic placement retries:** expensive and disruptive without a contractual placement lever.
- **Require dedicated CPU as anti-affinity:** CPU isolation is not evidence of host/rack separation.
- **Omit detection entirely:** avoids heuristic ambiguity but hides useful diagnostic information; warnings remain mandatory either way.
