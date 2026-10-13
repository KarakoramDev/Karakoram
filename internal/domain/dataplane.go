// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import "time"

// ConnectionCredential is one credential-bearing way to connect to a cluster.
// It is only ever returned to callers holding the cluster-credentials read
// permission and must never be logged or cached.
type ConnectionCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	URI      string `json:"uri"`
}

// ConnectionDetails is the sensitive connection view of a cluster: the
// password-free summary, the credentials and the CA certificate clients need
// to verify the cluster's TLS certificates.
type ConnectionDetails struct {
	Connection    Connection             `json:"connection"`
	Credentials   []ConnectionCredential `json:"credentials"`
	CACertificate string                 `json:"caCertificate,omitempty"`
}

// BalancerStatus reports the sharded-cluster balancer state.
type BalancerStatus struct {
	Enabled bool   `json:"enabled"`
	Running bool   `json:"running"`
	Mode    string `json:"mode,omitempty"`
}

// MemberLogLine is one log line of a process running in a cluster member.
type MemberLogLine struct {
	Timestamp time.Time `json:"timestamp"`
	Member    string    `json:"member"`
	Process   string    `json:"process"`
	Stream    string    `json:"stream"`
	Message   string    `json:"message"`
}
