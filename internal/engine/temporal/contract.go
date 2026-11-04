// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Persistence uses the generic PostgreSQL binding service (ADR 0045, package
// pgbinding). A nil binding service makes admission reject Temporal clusters;
// backup and restore additionally require it to implement
// pgbinding.BackupBinder.

// ClientCertificate is the retained, non-secret record of an issued client
// certificate. Private keys are never stored.
type ClientCertificate struct {
	Serial            string    `json:"serial"`
	CommonName        string    `json:"commonName"`
	FingerprintSHA256 string    `json:"fingerprintSha256"`
	IssuedAt          time.Time `json:"issuedAt"`
	NotAfter          time.Time `json:"notAfter"`
	// CAFingerprintSHA256 identifies the issuing CA; certificates of a rotated CA are invalid.
	CAFingerprintSHA256 string `json:"caFingerprintSha256"`
	// CurrentCA is derived on read, never persisted. During rotate-tls, current
	// means the CA used to issue new client certificates now: the old CA during
	// trust staging, then the new CA once the signing CA switches, even while
	// both CAs remain trusted during the rolling transition.
	CurrentCA bool `json:"currentCa"`
}

// IssuedClientCertificate is returned exactly once by IssueClientCertificate.
type IssuedClientCertificate struct {
	ClientCertificate
	CertificatePEM string `json:"certificatePem"`
	PrivateKeyPEM  string `json:"privateKeyPem"`
	CAPEM          string `json:"caPem"`
	// FrontendAddress and ServerName are what an SDK client needs to connect.
	FrontendAddress string `json:"frontendAddress"`
	ServerName      string `json:"serverName"`
}

// Namespace is an observed Temporal namespace as exposed by the API.
type Namespace struct {
	Name        string `json:"name"`
	Retention   string `json:"retention"`
	Description string `json:"description,omitempty"`
	State       string `json:"state"`
	// Managed is true when engineOptions.namespaces declares it (or it is "default").
	Managed bool `json:"managed"`
}

// Access is the Temporal-specific API surface the HTTP server calls on the
// provider (internal/server/temporal.go).
type Access interface {
	// IssueClientCertificate signs a client certificate (CN = commonName, ttl
	// 1h–397d) from the cluster CA and records only its non-secret metadata.
	IssueClientCertificate(ctx context.Context, rc *engine.Context, commonName string, ttl time.Duration) (IssuedClientCertificate, error)
	ListClientCertificates(ctx context.Context, rc *engine.Context) ([]ClientCertificate, error)
	// ListNamespaces reads namespaces through a Ready frontend member.
	ListNamespaces(ctx context.Context, rc *engine.Context) ([]Namespace, error)
	// DeleteNamespace requests asynchronous deletion; "default" and namespaces
	// still declared in engineOptions.namespaces are refused (conflict).
	DeleteNamespace(ctx context.Context, rc *engine.Context, name string) error
	// SetUIOIDCClientSecret seals the write-only Web UI OIDC client secret.
	SetUIOIDCClientSecret(ctx context.Context, rc *engine.Context, secret string) error
}
