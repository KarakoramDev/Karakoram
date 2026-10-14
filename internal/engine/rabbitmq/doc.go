// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package rabbitmq manages RabbitMQ 4.3 singleton brokers with one persistent
// Local Storage volume per broker. Names use monotonically increasing member
// indexes; hostname matches alone never establish infrastructure ownership.
//
// Sync checkpoints imports, deployments, joining and membership removal. Rolling
// changes gate on online quorum and quorum-critical checks, then drain and stop
// the broker before any disruptive platform action. Durable maintenance holds
// prevent background configuration pulls from starting a stopped broker while a
// platform operation is pending. A member must acknowledge its configuration,
// run, rejoin and revive before another member is changed. Resource changes are
// applied through the backend and followed by a gated rolling configuration
// update. Pending platform failures retain their exact action and target with
// durable exponential backoff; CPU-mode cooldowns do not hot-loop.
//
// Removal grows quorum queue replicas on every remaining broker, migrates stream
// queue replicas (independently of the optional stream protocol plugin), and
// shrinks the leaving broker. Its application is drained and stopped while the
// Erlang VM remains a live Khepri voter; a survivor forgets it before full process
// shutdown and infrastructure deletion. Durable classic queues hosted on that
// member block automatic scale-down and replacement: the reported queues must
// be explicitly migrated or deleted by the operator before retrying.
//
// Replacing a lost node uses healthy survivors to join a new, monotonically named
// member and migrate replicated queues. An unreachable departure skips local
// drain/stop commands and is forgotten only with a Khepri majority among survivors.
// This does not recover classic queue contents or enable recovery without a
// surviving metadata majority. Down remaining brokers prevent automatic removal.
// Cleanup gracefully stops brokers and may retain volumes and credentials.
// Resume starts every retained broker before waiting for individual readiness,
// allowing the retained Khepri majority to recover. Persistently mounted volumes
// are handed to orphan-volume cleanup only after their runtime is absent and
// their ownership is verified again.
// Member versions come from the local broker's cluster_status observation, not
// the agent's startup-cached version. A missing observed broker version blocks
// readiness, including for a one-member definitions clone.
//
// Broker credentials, Erlang cookies, CA keys, leaf certificates and pending
// password rotations exist only in SecretStore. Public AMQP requires an explicit
// address allowlist; optional public management uses backend HTTPS subdomains.
// preferTLS supports encrypted and plaintext clients; requireTLS also encrypts
// Erlang distribution. Distribution transport is fixed at creation: switching
// across the requireTLS boundary requires a new cluster and definitions restore.
// disabled and preferTLS can transition through gated rolling changes. Custom
// rabbitmq.conf overrides cannot change managed listeners, discovery, credentials,
// memory/disk limits or logging.
//
// Backups contain gzip-compressed RabbitMQ definitions in S3-compatible storage.
// They do not contain messages and provide no point-in-time recovery. Restore
// imports merge definitions into the target and preserve its reserved system
// users; a clone imports only after the target cluster is ready.
package rabbitmq
