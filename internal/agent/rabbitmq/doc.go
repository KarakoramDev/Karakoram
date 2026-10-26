// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package rabbitmq renders and supervises a RabbitMQ member using the shared
// agent.Engine protocol. Managed files live below DATA_DIR/karakoram; broker
// storage lives below DATA_DIR/rabbitmq. Cookies and keys are owner-only files.
// Broker and CLI environments exclude control-plane tokens and unrelated
// service credentials. Scheduler, dirty-scheduler and async thread counts are
// bounded by cgroup CPU quota (one to four schedulers), including discovery peers.
//
// Planned discovery peers and the initial quorum replica count are written for
// the next boot without restarting an existing broker. Other configuration,
// version, plugin, cookie and TLS changes require a restart. Maintenance and
// durable operator stop holds remain authoritative. The RabbitMQ stop route
// drains and stops without creating a durable hold: configuration reloads do
// not undo that stop, but an explicit start or a fresh supervisor can resume it.
// stop-app stops the RabbitMQ application while preserving the Erlang runtime
// for Khepri membership changes; start resumes the application in that runtime.
// Stream listeners follow TLS mode. InterNodeTLS also configures explicit
// mutually authenticated Osiris replication TLS in advanced.config.
//
// RabbitMQ mutation routes are fixed, authenticated by the shared transport,
// serialized with configuration and generic process controls, and deadline-bounded.
// The read-only observe route snapshots configuration without holding that action
// lock across live loopback Management HTTP probes. It never returns cached health
// and does not launch an Erlang CLI VM. The status route remains the CLI protocol
// for formation preconditions and older control planes.
// Local in-service/local-alarms checks and loopback Prometheus build/identity
// samples establish member health with independent two-second HTTP budgets.
// Missing or mismatched local identity withdraws health, never falls back to
// desired/cached data. /api/nodes?disable_stats=true has a separate one-second
// budget: its failure returns membershipError and no membership/partition view,
// without discarding fresh local health. The control plane still withholds
// cluster readiness and change gates until full membership is observed.
// The native display name is explicitly configured from cluster metadata:
// RabbitMQ's otherwise-unused cluster_status name falls back to a host FQDN
// lookup, so a DNS miss can delay even statistics-free membership on every read.
// A parallel /api/whoami probe uses the desired reserved admin credential:
// one fresh HTTP 401 proves drift; a complete 200 identifying admin verifies
// recovery. Network failures and other responses are inconclusive, not drift.
// The optional observation signal lets newer control planes withdraw readiness
// until acceptance is verified without changing legacy-agent behavior.
// The optional probe has its own one-second deadline, leaving headroom inside
// the member observation budget. Its configuration epoch binds rejection to
// the applied desired password: staged rotations and pre-commit peer snapshots
// are inconclusive until the committed credential is loaded.
// CLI arguments are not caller-controlled command lines. Password updates read
// cached system credentials and supply passwords exclusively on standard input.
// Before bootstrap launches cookie-authenticated repair commands, bounded fresh
// local HTTP reads prove whether both desired credentials, administrator tags,
// full permissions on / and guest absence already match. Only a complete proof
// skips the no-op repair; drift, malformed data or unavailable HTTP retains the
// existing CLI repair path. Quiet periodic reconciliation therefore does not
// launch eight Erlang VMs and block the control plane's readiness lease.
// KARAKORAM_RABBITMQ_BIN selects the CLI directory; its default is the pinned
// distribution's /usr/lib/rabbitmq/bin directory.
//
// Metrics scrape the local aggregated Prometheus endpoint and translate only
// rmqspec.MetricNames, without forwarding broker labels. Running member count
// includes the local node and established distribution links to planned peers;
// transient CLI distribution connections are excluded.
package rabbitmq
