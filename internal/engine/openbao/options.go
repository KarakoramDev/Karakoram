// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

// Options is the OpenBao `spec.engineOptions` document. Unknown fields are
// rejected. Seal, storage and unseal share settings are immutable after
// creation. Write-only credentials (transit token, AWS keys) are accepted on
// create, moved into cluster secrets before the cluster is persisted and never
// echoed; the persisted spec keeps only their non-secret siblings.
type Options struct {
	Unseal  UnsealOptions  `json:"unseal,omitzero"`
	Seal    SealOptions    `json:"seal,omitzero"`
	Storage StorageOptions `json:"storage,omitzero"`
	// LogLevel is trace, debug, info (default), warn or error.
	LogLevel string `json:"logLevel,omitempty"`
}

// UnsealOptions sizes the Shamir split (or recovery split for auto-unseal).
// Defaults: 5 shares, threshold 3; 1 <= threshold <= shares <= 10, and
// threshold >= 2 when shares >= 2.
type UnsealOptions struct {
	Shares    int `json:"shares,omitempty"`
	Threshold int `json:"threshold,omitempty"`
}

// SealOptions selects shamir (default), transit or awskms.
type SealOptions struct {
	Type    string          `json:"type,omitempty"`
	Transit *TransitOptions `json:"transit,omitempty"`
	AWSKMS  *AWSKMSOptions  `json:"awskms,omitempty"`
}

// TransitOptions configures transit auto-unseal against an external server.
type TransitOptions struct {
	// Address is the https:// URL of the external OpenBao/Vault.
	Address   string `json:"address"`
	MountPath string `json:"mountPath,omitempty"` // default "transit/"
	KeyName   string `json:"keyName"`
	// CAPEM verifies Address; empty uses the system roots.
	CAPEM         string `json:"caPEM,omitempty"`
	TLSServerName string `json:"tlsServerName,omitempty"`
	// Token is write-only.
	Token string `json:"token,omitempty"`
}

// AWSKMSOptions configures AWS KMS auto-unseal through the KMS plugin.
type AWSKMSOptions struct {
	Region   string `json:"region"`
	KMSKeyID string `json:"kmsKeyId"`
	// Endpoint optionally overrides the KMS endpoint (https://).
	Endpoint string `json:"endpoint,omitempty"`
	// AccessKeyID and SecretAccessKey are write-only.
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
}

// StorageOptions selects raft (default) or postgresql.
type StorageOptions struct {
	Type string `json:"type,omitempty"`
	// PostgreSQL binds to a Karakoram PostgreSQL cluster in the same namespace.
	PostgreSQL *PostgreSQLStorageOptions `json:"postgresql,omitempty"`
}

// PostgreSQLStorageOptions references the PostgreSQL source cluster.
type PostgreSQLStorageOptions struct {
	Cluster string `json:"cluster"`
}

// Cluster secret keys owned by the OpenBao provider. Values are JSON or raw
// bytes sealed by the control-plane secret store.
const (
	// SecretUnseal holds the stored initialization material (shares,
	// threshold, seal type, initial root token, initialized time).
	SecretUnseal = "openbao/unseal"
	// SecretManagementToken holds the periodic karakoram-manager token.
	SecretManagementToken = "openbao/management-token"
	// SecretInitKey holds the in-flight receipt id and X25519 private key.
	SecretInitKey = "openbao/init-key"
	// Write-only seal credentials.
	SecretTransitToken       = "openbao/seal/transit-token"
	SecretAWSAccessKeyID     = "openbao/seal/aws-access-key-id"
	SecretAWSSecretAccessKey = "openbao/seal/aws-secret-access-key"
	// SecretConnections holds the secret-free database-connection index;
	// SecretConnectionPrefix+<name> holds that connection's target identity.
	SecretConnections      = "openbao/connections"
	SecretConnectionPrefix = "openbao/connections/"
)
