// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package baospec defines the OpenBao member specification and the bounded
// control-plane ↔ member-agent protocol. See docs/adr/0047-openbao-engine.md.
//
// Unseal shares, recovery shares and root tokens never appear in MemberSpec:
// the agent caches the whole member configuration on its volume. Initialization
// output leaves the member only as an Envelope encrypted to a control-plane key.
package baospec

// Engine is the provider and agent engine name; Process is the supervised process.
const (
	Engine  = "openbao"
	Process = "openbao"
)

// Fixed member ports.
const (
	// APIPort serves the OpenBao HTTP API over TLS on every interface.
	APIPort = 8200
	// ClusterPort carries Raft and request forwarding (OpenBao-managed TLS).
	ClusterPort = 8201
	// RecoveryPort is private control-plane-only mTLS for shares-based recovery.
	RecoveryPort = 8202
	// MetricsPort is a loopback-only TLS listener with unauthenticated
	// Prometheus metrics, scraped by the member agent.
	MetricsPort = 8210
)

// Storage types.
const (
	StorageRaft       = "raft"
	StoragePostgreSQL = "postgresql"
)

// Seal types.
const (
	SealShamir  = "shamir"
	SealTransit = "transit"
	SealAWSKMS  = "awskms"
)

// Plugin types used in declarative plugin stanzas.
const (
	PluginTypeDatabase = "database"
	PluginTypeKMS      = "kms"
)

// Agent actions, called as POST /v1/openbao/<action>.
const (
	ActionStatus  = "status"
	ActionInit    = "init"
	ActionInitAck = "init-ack"
)

// MemberSpec is the OpenBao EngineSpec. The agent renders it into the server
// configuration under DATA_DIR/karakoram/openbao and supervises `bao server`.
// The API listener certificate, key and cluster CA come from MemberConfig.TLS.
type MemberSpec struct {
	// NodeID is the Raft node_id; it equals the member hostname.
	NodeID  string `json:"nodeId"`
	Version string `json:"version"`
	// BinaryPath is the absolute path of the verified `bao` binary installed by
	// the runtime preparation (for example /opt/karakoram/openbao-2.7.1/bao).
	BinaryPath string `json:"binaryPath"`
	// PluginDirectory is the absolute plugin_directory; empty when no plugins.
	PluginDirectory string   `json:"pluginDirectory,omitempty"`
	Plugins         []Plugin `json:"plugins,omitempty"`
	// ListenAddress is the API listener bind host, "::" (dual stack) or "0.0.0.0".
	ListenAddress string `json:"listenAddress"`
	// APIAddr and ClusterAddr are the advertised https://<member>:<port> URLs.
	APIAddr     string  `json:"apiAddr"`
	ClusterAddr string  `json:"clusterAddr"`
	Storage     Storage `json:"storage"`
	// APIPort, ClusterPort and MetricsPort override the fixed ports when
	// non-zero (native tests run several members on one host).
	APIPort      int `json:"apiPort,omitempty"`
	ClusterPort  int `json:"clusterPort,omitempty"`
	MetricsPort  int `json:"metricsPort,omitempty"`
	RecoveryPort int `json:"recoveryPort,omitempty"`
	// RecoveryCAPEM trusts only the control plane's dedicated recovery client
	// CA. Its signing key and client private key never enter member config.
	RecoveryCAPEM string `json:"recoveryCaPEM,omitempty"`
	Seal          Seal   `json:"seal"`
	// TrustCAPEMs are extra PEM CA bundles (database-connection targets) added
	// to the process trust next to the system roots. Applied on next restart.
	TrustCAPEMs []string `json:"trustCAPEMs,omitempty"`
	// ProcessGeneration is the restart epoch. The agent restarts a running
	// server only when this value changes (or the binary path changes); every
	// other configuration change is written to disk and signalled with SIGHUP,
	// because a restarted server comes back sealed. The control plane bumps it
	// one member at a time.
	ProcessGeneration int64 `json:"processGeneration"`
	// LogLevel is trace, debug, info, warn or error; empty means info.
	LogLevel string `json:"logLevel,omitempty"`
}

// Plugin is one declarative `plugin "<type>" "<name>"` stanza.
type Plugin struct {
	Type string `json:"type"`
	Name string `json:"name"`
	// Command is the executable file name inside PluginDirectory.
	Command string `json:"command"`
	Version string `json:"version,omitempty"`
}

// Storage selects the storage backend.
type Storage struct {
	Type string `json:"type"`
	// RetryJoin lists every other desired member (Raft only).
	RetryJoin  []RetryJoin        `json:"retryJoin,omitempty"`
	PostgreSQL *PostgreSQLStorage `json:"postgresql,omitempty"`
}

// RetryJoin is one Raft retry_join target, verified with the cluster CA.
type RetryJoin struct {
	// LeaderAPIAddr is https://<member>:8200.
	LeaderAPIAddr string `json:"leaderApiAddr"`
	// TLSServerName overrides SNI/verification name; empty uses the URL host.
	TLSServerName string `json:"tlsServerName,omitempty"`
}

// PostgreSQLStorage configures `storage "postgresql"` with HA enabled.
type PostgreSQLStorage struct {
	// ConnectionURL is a libpq URL including the binding role password.
	ConnectionURL string `json:"connectionUrl"`
	Table         string `json:"table"`
	HATable       string `json:"haTable"`
	// CAPEM verifies the PostgreSQL server; the agent writes it to a file and
	// references it with sslrootcert.
	CAPEM string `json:"caPEM,omitempty"`
}

// Seal selects Shamir (default) or an auto-unseal mechanism.
type Seal struct {
	Type    string       `json:"type"`
	Transit *TransitSeal `json:"transit,omitempty"`
	AWSKMS  *AWSKMSSeal  `json:"awskms,omitempty"`
}

// TransitSeal configures `seal "transit"` against an external OpenBao/Vault.
type TransitSeal struct {
	Address   string `json:"address"`
	MountPath string `json:"mountPath"`
	KeyName   string `json:"keyName"`
	Token     string `json:"token"`
	// CAPEM verifies Address; empty uses the system roots.
	CAPEM         string `json:"caPEM,omitempty"`
	TLSServerName string `json:"tlsServerName,omitempty"`
}

// AWSKMSSeal configures `seal "awskms"` through the KMS plugin.
type AWSKMSSeal struct {
	Region    string `json:"region"`
	KMSKeyID  string `json:"kmsKeyId"`
	Endpoint  string `json:"endpoint,omitempty"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

// StatusResponse is the native member observation, from the local
// unauthenticated OpenBao endpoints (seal status, health, leader).
type StatusResponse struct {
	NodeID       string `json:"nodeId"`
	Version      string `json:"version"`
	Initialized  bool   `json:"initialized"`
	Sealed       bool   `json:"sealed"`
	SealType     string `json:"sealType,omitempty"`
	RecoverySeal bool   `json:"recoverySeal,omitempty"`
	// Threshold and Progress are the unseal threshold and submitted share count.
	Threshold int  `json:"threshold,omitempty"`
	Progress  int  `json:"progress,omitempty"`
	Standby   bool `json:"standby"`
	// Active reports that this member is the active (leader) node.
	Active             bool   `json:"active"`
	HAEnabled          bool   `json:"haEnabled"`
	LeaderAddress      string `json:"leaderAddress,omitempty"`
	ClusterID          string `json:"clusterId,omitempty"`
	RaftCommittedIndex uint64 `json:"raftCommittedIndex,omitempty"`
	RaftAppliedIndex   uint64 `json:"raftAppliedIndex,omitempty"`
	// ProcessGeneration is the generation the running process was started with.
	ProcessGeneration int64 `json:"processGeneration"`
	// InitReceipt reports an unacknowledged encrypted init receipt on disk.
	InitReceipt bool `json:"initReceipt,omitempty"`
	// Error is a bounded, secret-free description when the server is unreachable.
	Error string `json:"error,omitempty"`
}

// InitRequest asks the agent to initialize the local server exactly once.
// The control plane persists ReceiptID and the private key before calling and
// reuses both on retry until it acknowledges the receipt.
type InitRequest struct {
	ReceiptID string `json:"receiptId"`
	// RecipientPublicKey is the control plane's X25519 public key (32 bytes).
	RecipientPublicKey []byte `json:"recipientPublicKey"`
	// SecretShares/SecretThreshold apply to Shamir seals; Recovery* to auto-unseal.
	SecretShares      int `json:"secretShares,omitempty"`
	SecretThreshold   int `json:"secretThreshold,omitempty"`
	RecoveryShares    int `json:"recoveryShares,omitempty"`
	RecoveryThreshold int `json:"recoveryThreshold,omitempty"`
}

// InitResponse returns the encrypted receipt. When the server was already
// initialized and a receipt exists, the stored receipt is returned unchanged
// (its ReceiptID may differ from the request). When the server is initialized
// and no receipt exists the agent answers HTTP 409 with ErrorInitializedWithoutReceipt.
type InitResponse struct {
	Envelope Envelope `json:"envelope"`
}

// ErrorInitializedWithoutReceipt is the error text of the 409 init answer.
const ErrorInitializedWithoutReceipt = "openbao is initialized and no initialization receipt exists"

// InitAckRequest deletes the receipt once the control plane stored the material.
// Acknowledging a missing receipt succeeds; a different ReceiptID is rejected.
type InitAckRequest struct {
	ReceiptID string `json:"receiptId"`
}

// InitMaterial is the decrypted receipt plaintext: the sys/init response.
type InitMaterial struct {
	Keys               []string `json:"keys,omitempty"`
	KeysBase64         []string `json:"keys_base64,omitempty"`
	RecoveryKeys       []string `json:"recovery_keys,omitempty"`
	RecoveryKeysBase64 []string `json:"recovery_keys_base64,omitempty"`
	RootToken          string   `json:"root_token"`
}

// MetricNames is the fixed agent exposition vocabulary, in catalog order.
var MetricNames = []string{
	"karakoram_openbao_up",
	"karakoram_openbao_initialized",
	"karakoram_openbao_sealed",
	"karakoram_openbao_active",
	"karakoram_openbao_raft_committed_index",
	"karakoram_openbao_raft_applied_index",
	"karakoram_openbao_raft_follower_last_heartbeat_seconds",
	"karakoram_openbao_autopilot_healthy",
	"karakoram_openbao_autopilot_failure_tolerance",
	"karakoram_openbao_requests_total",
	"karakoram_openbao_leases",
	"karakoram_openbao_memory_bytes",
}
