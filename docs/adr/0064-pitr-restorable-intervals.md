# 0064 — Restorable PITR interval lists

Status: Accepted

## Context

The PITR endpoint reported only the latest-ending merged interval. A new full
backup could hide an earlier valid range even though restore admission accepted
it. The restore picker consequently rejected valid recovery targets.

## Decision

Add optional `intervals` to the public PITR response, each with `start`, `end`,
and optional `backup` naming its anchoring full. Preserve legacy `available`,
`start`, `end`, and `message` values. Sort entries by start, end, then backup.
Merge coverage within a full using existing engine rules, never across fulls.

An optional engine interface returns legacy merged coverage and attributed
intervals from one observation. MySQL/PXC provide live per-full coverage;
remembered PXC windows retain unknown attribution. Other engines use their
existing intervals without invented backup names. Admission and cluster status
continue using existing merged coverage. CLI, MCP, and UI expose all intervals;
the picker accepts membership in any interval but rejects gaps.

## Consequences

The API change is additive, with no agent protocol or backup migration. Overlap
between distinct full backups is intentional. Legacy summary consumers still
see one interval and must adopt `intervals` to discover earlier ranges.

## Alternatives considered

Replacing legacy start/end would change existing consumers' recovery summaries.
Merging across fulls would discard useful provenance. A second independent
coverage observation could produce inconsistent summary and interval results.
