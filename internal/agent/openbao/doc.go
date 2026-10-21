// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// The member configuration is cached in plaintext by the shared agent. OpenBao
// initialization shares and root tokens therefore never enter MemberConfig:
// init encrypts the raw response to the caller's X25519 key and fsyncs the
// encrypted receipt before returning it. A missing receipt on an initialized
// server is a conflict, not permission to initialize again. Only a matching
// acknowledgement deletes a receipt. Members do not implement an unseal route;
// the control plane submits shares directly to the native TLS API.
//
// Configuration is JSON, so all string fields retain their literal values.
// Native files live below karakoram/openbao with owner-only permissions; Raft
// databases live below openbao/raft. The supervisor runs the installed binary
// directly with its clean environment. Extra database CAs are supplied through
// SSL_CERT_DIR, never through a secret-bearing environment variable.
//
// ProcessGeneration and BinaryPath are the only Apply-driven restart triggers.
// Other rewritten files, including listener certificates and retry_join lists,
// receive SIGHUP instead. Startup readiness checks only the local listening
// socket: sealing, leadership and quorum are control-plane health concerns.
package openbao
