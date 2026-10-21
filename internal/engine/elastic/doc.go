// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package elastic reconciles Elasticsearch nodes and their Kibana, Logstash and
// APM component services. Ownership is established by recorded service IDs,
// retained-volume IDs, identities returned by the member's own import, a
// service-bound non-sensitive KARAKORAM_OWNER marker, or readable legacy agent
// token secrets. Sensitive variables are masked for tokens without sudo mode;
// masked values and service hostnames alone never establish ownership.
//
// An unrecorded volume service may also be verified by a runtime whose
// marker or legacy secret proves ownership and whose data directory names that
// exact volume mount. Markers are backfilled only after every existing service
// of the member is verified. Expired import processes are reconciled against
// actual services: missing services are imported again, while existing services
// still require ownership evidence.
//
// RuntimeSpec and the shared engine service builders describe deployments and
// imports using the backend's layout, without rendering platform manifests.
// Optional stack services are declared in an engine.ComponentSet, which supplies
// defaults, validation and topology information. Its stateless lifecycle helper
// preserves the import, deploy, readiness and scale stages; Elasticsearch keeps
// ownership checks, durable operation tracking and database safety decisions.
//
// Agent connectivity means a successful status response, independently of child
// health. Node and pinned component observations retain paused/error diagnostics
// as Process conditions without losing native roles. Every live component must
// pass process, configuration and application readiness checks. Paused children
// keep the member unhealthy and the cluster nonReady; investigate the stop before
// manually sending SIGCONT to a verified process. Reconciliation never resumes
// or kills a child in response to these diagnostics.
//
// A dataSource target remains unready until its durable initial snapshot restore
// completion marker is saved, independently of desired generation and process
// health. Failed or cancelled initial restores are terminal and never replayed
// implicitly. Native restore claim fences conflicting mutations; target deletion
// remains permitted.
// Snapshot submission waits for an explicit successful native shard result and
// persists that completion receipt before advancing. Accepted-only legacy
// receipts and interrupted submissions remain fail-closed without replay.
// Historical superseded creates without restore evidence remain nonReady and
// never enqueue a replacement restore. Their ordinary runtime, access, backup
// storage and exposure reconciliation continues without the old source dependency.
//
// Data MCP uses native, scoped read-only and opt-in document-write identities.
// Their roles exclude restricted indices, but wildcard grants still match
// non-restricted dot indices: the native gateway must independently exclude all
// dot indices and prevent alias or wildcard expansion from reaching them.
// Agent rollouts freeze their target build. If the control-plane build changes,
// no new deployment is admitted or retried for that rollout. An already stopped
// component is started on its existing artifact; an accepted deployment consumes
// operator-corrected configuration at its existing engine version and waits for
// service readiness. Elasticsearch allocation and shard health recover before
// the rolling journal is retired and the operation fails. Recovery never reports
// the changed artifact as satisfying the old target.
package elastic
