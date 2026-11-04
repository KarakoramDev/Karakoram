// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package neo4jspec defines the Neo4j Community member payload carried in
// agentapi.MemberConfig.EngineSpec and the bounded agent action protocol
// shared by the control-plane provider (internal/engine/neo4j) and the member
// runner (internal/agent/neo4j). It must stay dependency-free. The payload and
// several requests carry secrets; agents cache the payload owner-only and never
// persist request secrets in receipts.
package neo4jspec

// Engine is the provider name, MemberConfig.Engine and KARAKORAM_ENGINE value.
const Engine = "neo4j"

// Process is the single supervised native process (the Neo4j server JVM).
const Process = "neo4j"

// TopologySingle is the only supported topology: Neo4j Community cannot cluster.
const TopologySingle = "single"

// Well-known ports. Bolt and HTTPS always require TLS; plain HTTP is disabled.
// Recovery ports are bound to 127.0.0.1 only, by the loopback-only credential
// recovery start used after a restore or to repair a lost admin password.
const (
	PortBolt          = 7687
	PortHTTPS         = 7473
	RecoveryPortBolt  = 17687
	RecoveryPortHTTPS = 17473
)

// AdminUser is the managed administrator. Community has no RBAC: every user,
// including application users, has full administrative privileges.
const AdminUser = "neo4j"

// Databases dumped by a backup, in load order. Community serves exactly one
// user database ("neo4j") next to the "system" database that stores users.
var Databases = []string{"system", "neo4j"}

// BackupTypeDump is the only backup type: an offline neo4j-admin dump.
const BackupTypeDump = "dump"

// ManifestFormat versions the backup manifest object.
const ManifestFormat = "karakoram-neo4j-dump/v1"

// Object names below a backup's object prefix. The manifest is written last;
// a prefix without a manifest is an incomplete backup.
const (
	ManifestObject = "manifest.json"
	DumpSuffix     = ".dump" // "<database>.dump"
)

// Agent actions, served as POST /v1/neo4j/<action> and called through
// agentapi.Client.Engine(ctx, Engine, Action..., request, &response).
const (
	ActionReadiness     = "readiness"      // {} -> Readiness
	ActionUsers         = "users"          // UserRequest -> UserResponse
	ActionAdminPassword = "admin-password" // AdminPasswordRequest -> {}
	ActionRecoverAdmin  = "recover-admin"  // RecoverAdminRequest -> JobResponse
	ActionBackup        = "backup"         // BackupRequest -> JobResponse
	ActionRestore       = "restore"        // RestoreRequest -> JobResponse
)

// Certificate is a PEM leaf certificate and its private key.
type Certificate struct {
	CertPEM string `json:"certPEM"`
	KeyPEM  string `json:"keyPEM"`
}

// Install locates the prepared native artifacts on the member runtime.
type Install struct {
	// Neo4jHome is the extracted official tarball root (contains bin/neo4j).
	Neo4jHome string `json:"neo4jHome"`
	// JavaHome is the extracted pinned JRE root (contains bin/java).
	JavaHome string `json:"javaHome"`
}

// Spec is the Neo4j member payload in MemberConfig.EngineSpec.
type Spec struct {
	// Version is the desired Neo4j version (exact calendar version).
	Version string  `json:"version"`
	Install Install `json:"install"`
	// HeapMB and PageCacheMB are rendered as server.memory.heap.{initial,max}_size
	// and server.memory.pagecache.size. Both are positive.
	HeapMB      int `json:"heapMB"`
	PageCacheMB int `json:"pageCacheMB"`
	// CAPEM is the cluster CA; Server is the Bolt and HTTPS leaf.
	CAPEM  string      `json:"caPEM"`
	Server Certificate `json:"server"`
	// AdvertisedHost is the private host name advertised for Bolt/HTTPS.
	AdvertisedHost string `json:"advertisedHost"`
	// AdminPassword is the current managed password of AdminUser.
	AdminPassword string `json:"adminPassword"`
	// Settings are validated user settings from an allowlist; managed keys win
	// and are never overridable.
	Settings map[string]string `json:"settings,omitempty"`
	// RestartToken changes whenever a running server must restart to apply the
	// payload (version, memory, settings, certificate rotation). The agent
	// restarts a running server only when the applied token differs.
	RestartToken string `json:"restartToken"`
}

// Readiness is the response of POST /v1/neo4j/readiness.
type Readiness struct {
	Ready bool `json:"ready"`
	// Message explains a false Ready without secrets.
	Message string `json:"message,omitempty"`
	// ConfigurationError is set when the payload cannot be applied.
	ConfigurationError string `json:"configurationError,omitempty"`
	ConfigVersion      int64  `json:"configVersion"`
	// Version and Edition are observed from the running server.
	Version string `json:"version,omitempty"`
	Edition string `json:"edition,omitempty"`
	// AppliedRestartToken is the token of the running server.
	AppliedRestartToken string `json:"appliedRestartToken,omitempty"`
	// CredentialMismatch reports that AdminPassword is rejected by the server.
	CredentialMismatch bool `json:"credentialMismatch,omitempty"`
	// StoreBytes is the on-disk size of all database directories.
	StoreBytes int64 `json:"storeBytes,omitempty"`
}

// User operations for POST /v1/neo4j/users.
const (
	UserList     = "list"
	UserGet      = "get"
	UserCreate   = "create"
	UserPassword = "password"
	UserDelete   = "delete"
)

// UserNamePattern is the shared public application-user name syntax (1–64 ASCII
// characters). Reserved administrator names are rejected separately.
const UserNamePattern = `^[A-Za-z][A-Za-z0-9_.-]{0,63}$`

// UserRequest administers native users. Name must not be AdminUser.
type UserRequest struct {
	Op       string `json:"op"`
	Name     string `json:"name,omitempty"`
	Password string `json:"password,omitempty"`
}

// User is a native Neo4j user (no password).
type User struct {
	Name                   string `json:"name"`
	PasswordChangeRequired bool   `json:"passwordChangeRequired"`
}

// UserResponse answers a UserRequest.
type UserResponse struct {
	Users []User `json:"users,omitempty"`
	User  *User  `json:"user,omitempty"`
}

// AdminPasswordRequest changes the managed admin password idempotently:
// if Next already authenticates the request succeeds without change;
// otherwise Current must authenticate and the password changes to Next.
type AdminPasswordRequest struct {
	Receipt string `json:"receipt"`
	Current string `json:"current"`
	Next    string `json:"next"`
}

// S3 is an object-store location for dump objects. Credentials are request
// scoped and never persisted by the agent.
type S3 struct {
	Endpoint  string `json:"endpoint"` // HTTP(S) origin
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix"` // object prefix of this backup, no trailing slash
	PathStyle bool   `json:"pathStyle"`
	VerifyTLS bool   `json:"verifyTls"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	// EndpointAllowlist is the control plane's private-endpoint allowlist,
	// enforced by the agent with the same dial-time policy.
	EndpointAllowlist string `json:"endpointAllowlist,omitempty"`
}

// DumpObject describes one database dump object.
type DumpObject struct {
	Database string `json:"database"`
	Key      string `json:"key"` // object key relative to the bucket
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}

// Manifest is the JSON object written last below a backup prefix.
type Manifest struct {
	Format       string       `json:"format"`
	Neo4jVersion string       `json:"neo4jVersion"`
	Edition      string       `json:"edition"`
	CreatedAt    string       `json:"createdAt"` // RFC 3339 UTC
	Databases    []DumpObject `json:"databases"`
}

// Job states.
const (
	JobRunning = "running"
	JobDone    = "done"
	JobFailed  = "failed"
)

// Job phases reported while running (informational).
const (
	PhaseStopping   = "stopping"
	PhaseDumping    = "dumping"
	PhaseUploading  = "uploading"
	PhaseFetching   = "fetching"
	PhaseLoading    = "loading"
	PhaseRecovering = "recovering-credentials"
	PhaseStarting   = "starting"
)

// BackupRequest starts or polls (same Receipt and request) an offline dump:
// stop the server under a durable stop hold, dump every database in Databases
// and stream it to Target, write the manifest last, start the server again.
type BackupRequest struct {
	Receipt string `json:"receipt"`
	Target  S3     `json:"target"`
}

// RestoreRequest starts or polls (same Receipt and request) an offline load:
// stop under a durable stop hold, fetch and verify every dump in Manifest,
// load into a staging data directory, swap it in, reset AdminUser's password
// to AdminPassword through a loopback-only authentication-disabled start, then
// start the server normally.
type RestoreRequest struct {
	Receipt       string   `json:"receipt"`
	Source        S3       `json:"source"`
	Manifest      Manifest `json:"manifest"`
	AdminPassword string   `json:"adminPassword"`
}

// JobResponse reports a backup or restore receipt.
type JobResponse struct {
	State    string    `json:"state"`
	Phase    string    `json:"phase,omitempty"`
	Message  string    `json:"message,omitempty"`
	Manifest *Manifest `json:"manifest,omitempty"`
}

// RecoverAdminRequest starts or polls (same Receipt and request) an offline
// repair of AdminUser's password: stop under a durable stop hold, start once
// with authentication disabled and every connector on 127.0.0.1 recovery
// ports, set AdminPassword, stop, then start normally. The agent uses the same
// procedure internally to initialize a fresh store before its first network
// start.
type RecoverAdminRequest struct {
	Receipt       string `json:"receipt"`
	AdminPassword string `json:"adminPassword"`
}
