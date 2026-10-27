# 0009 — Compact stable member names within Zerops constraints

Status: Accepted

## Context

Service hostnames identify members over the private network and must remain stable through redeploys and retained-volume resume. Replacement identities must not accidentally inherit an old member's state. Compact lowercase alphanumeric names leave headroom for component and member suffixes.

## Decision

Require cluster names to match `^[a-z][a-z0-9]{1,11}$` (2–12 characters). For cluster `c`, allocate decimal indices and generate:

| Resource | Hostname |
|---|---|
| Replica-set member n | `{c}m{n}` |
| Shard i, member n | `{c}s{i}m{n}` |
| Config-server member n | `{c}c{n}` |
| Router service | `{c}r` |
| Member X's volume | `{X}v` |

Indices start at zero and are monotonic within each set; allocate a new index on member replacement and never reuse a retired one. Persist identity-to-service/volume mappings; names are not a substitute for ownership records. Use `rs0` for a standalone replica set, `rs0`, `rs1`, … for shards and `cfg` for the config-server replica set. Control-plane resources are `karakoram` and `karakoramdata`.

Validate every complete generated runtime/volume hostname against the confirmed platform rules and detect project-wide collisions before import. The short cluster limit leaves suffix headroom but does not make unbounded indices safe. Reject an unrepresentable allocation rather than truncate/hash it silently or overwrite an unrelated service. Retained-volume resume reuses its persisted identity; new replacement allocation does not.

## Consequences

Connection hosts and volume links are inspectable, compact and deterministic. Membership count is not a contiguous ordinal range after replacement. Hostname length/collision checks are an API/provider responsibility, and existing unrelated project services can block a requested name. A naming change would require an explicit migration because replica-set configuration contains these addresses.

Index allocation also skips indices whose runtime or volume hostname is still held by an orphaned volume record or an existing [Zerops](https://zerops.io) service, so re-creating a same-named cluster never collides.

## Alternatives considered

- **UUID-only hostnames:** reduce collisions but obscure operational identity.
- **Reused ordinal slots:** compact but confuse replacement state with the departed member.
- **Human names with hyphens:** more readable, but incompatible with the generated hostname syntax.
