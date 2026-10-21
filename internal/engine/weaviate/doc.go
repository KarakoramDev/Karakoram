// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package weaviate reconciles native community-profile Weaviate members through
// the backend-neutral agent runtime seam. Runtime ownership is bound to recorded
// service IDs or HMAC ownership markers; a matching hostname is never adoption
// evidence. Import batches are persisted before submission, and platform handles
// are drained before another lifecycle owner acts. Platform readiness is the
// peer-independent agent startup probe, not native metadata quorum.
//
// state.go separates cluster identity and fixed voters, member active intent,
// infrastructure pending/retry receipts, and rolling ownership. SecretStore holds
// credentials, PKI and activated/target configurations; none are serialized into
// state, status, events or operation checkpoints. Native listeners load TLS at
// startup, so leaf changes participate in the durable rolling plan. Internal
// gossip/Raft/data transports require a suitably isolated private backend network.
//
// Scale-down retires highest-index non-voters, moving native replicas to healthy
// least-loaded peers before stopping, authenticated Raft removal, and ordered
// runtime/volume deletion. Two empty sharding scans and verbose node statistics
// gate retirement. Fixed voters are never removed. Failed feasibility or drain
// receives bounded retries and a durable blocked condition. Scale-back-up cancels
// outstanding movement until the stop boundary. Proven never-placed runtimes use
// infrastructure-only retirement. Scale-out does not rebalance existing shards.
//
// Root-key rotation rolls old+new keys, verifies the new CP credential on every
// member, then rolls away the old key. Upstream internal basic authentication
// accepts one password for inbound and outbound data calls, so its rotation
// requires a coordinated full restart with brief unavailability, after a healthy
// synchronized quorum gate. TLS rotation reissues leaves under the existing CA
// and uses the durable non-voter-first rolling plan; startup-loaded REST/gRPC
// certificates require restarts. Rotation timestamps are non-secret state.
//
// Native full backups and restores are durably polled; retained artifacts are
// deleted only under their exact owned prefix. Initial DataSource restore and
// the native access interface share the same lifecycle dispatcher. Independently
// owned state fields survive save through statejson.ReplaceOwned.
//
// Lost-volume replacement requires a surviving voting quorum and explicit native
// collection/shard replica evidence; omitted shard or replica evidence fails
// closed with original-volume/backup recovery guidance. Never-placed scale-back
// supersedes obsolete deployment waits before infrastructure-only retirement.
// Public listener allocations precede publication, survive uncertain replies,
// and are reconstructed from current durable membership after retirement.
package weaviate
