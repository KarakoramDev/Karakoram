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
// replicas, shrinks the leaving broker and drains/stops it before forgetting its
// cluster identity and deleting infrastructure. Down survivors prevent removal.
// Cleanup gracefully stops brokers and may retain volumes and their credentials.
// Persistently mounted volumes are handed to the orphan-volume cleanup workflow
// only after their runtime is absent and their ownership is verified again.
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
