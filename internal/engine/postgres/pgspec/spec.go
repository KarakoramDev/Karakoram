// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package pgspec defines the PostgreSQL member payload carried in
// agentapi.MemberConfig.EngineSpec and the bounded agent action protocol
// shared by the control-plane provider (internal/engine/postgres) and the
// member runner (internal/agent/postgres). It must stay dependency-free. The
// payload contains secrets; agents cache it with owner-only permissions.
package pgspec

// Engine is the MemberConfig.Engine and KARAKORAM_ENGINE value.
const Engine = "postgres"

// Member roles.
const (
	RoleData  = "data"
	RoleProxy = "proxy"
)

// Topologies.
const (
	TopologySingle      = "single"
	TopologyHA          = "ha"
	TopologyDistributed = "distributed"
)

// Flavours select engine-specific additions on data members.
const (
	FlavourPostgres = "postgres"
	FlavourSupabase = "supabase"
)

// Well-known ports.
const (
	PortPostgres     = 5432
	PortPatroni      = 8008
	PortEtcdClient   = 2379
	PortEtcdPeer     = 2380
	PortProxy        = 5432
	PortProxyReader  = 5433
	PortProxyAdmin   = 6132
	ProxyWriterGroup = 10
	ProxyReaderGroup = 20
)

// Synchronous replication modes.
const (
	SyncOff    = "off"
	SyncOn     = "on"
	SyncStrict = "strict"
)

// Certificate is a PEM leaf certificate and its private key.
type Certificate struct {
	CertPEM string `json:"certPEM"`
	KeyPEM  string `json:"keyPEM"`
}

// Credentials is a username/password pair.
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Peer is one data member of the cluster as seen by every member.
type Peer struct {
	Name string `json:"name"`
	// Host is the private DNS name used for PostgreSQL, Patroni and etcd.
	Host string `json:"host"`
	// Etcd reports this peer's desired etcd membership: "voter", "learner" or
	// "" when the peer does not (yet) run etcd.
	Etcd string `json:"etcd,omitempty"`
}

// Etcd configures a local DCS member or a client-only Patroni member.
type Etcd struct {
	// InitialCluster is the name=https://host:2380 list for first start.
	InitialCluster string `json:"initialCluster"`
	// State is "new" for bootstrap members or "existing" for members added at runtime.
	State string `json:"state"`
	Token string `json:"token"`
	// Endpoints are host:2379 addresses; a nonempty list selects client-only
	// mode, so this member runs no local etcd and ignores bootstrap fields.
	Endpoints []string `json:"endpoints,omitempty"`
}

// Citus identifies a Patroni-managed distributed PostgreSQL group.
type Citus struct {
	Group    int    `json:"group"`
	Database string `json:"database"`
}

// Postgres holds server settings rendered by the agent.
type Postgres struct {
	// Parameters are validated GUCs (engine-managed keys win over user keys).
	Parameters map[string]string `json:"parameters,omitempty"`
	// SharedPreloadLibraries is rendered as a proper list (never one quoted value).
	SharedPreloadLibraries []string `json:"sharedPreloadLibraries,omitempty"`
	// RequireTLS selects hostssl-only client HBA rules.
	RequireTLS bool `json:"requireTls,omitempty"`
	// ClientCIDRs are the private address ranges allowed by HBA.
	ClientCIDRs []string `json:"clientCidrs,omitempty"`
	// RecoveryClientsHeld keeps remote clients fenced until source credentials
	// have been removed from every member's durable configuration.
	RecoveryClientsHeld bool `json:"recoveryClientsHeld,omitempty"`
	// Synchronous is off, on or strict (ha only).
	Synchronous string `json:"synchronous,omitempty"`
	// PgBackRestReplicas enables pgBackRest as the first replica creation method.
	PgBackRestReplicas bool `json:"pgBackRestReplicas,omitempty"`
}

// S3 is the pgBackRest repository target.
type S3 struct {
	Endpoint  string `json:"endpoint"`
	Port      int    `json:"port,omitempty"`
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	PathStyle bool   `json:"pathStyle"`
	VerifyTLS bool   `json:"verifyTls"`
	CAPEM     string `json:"caPEM,omitempty"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}

// Repository identifies one pgBackRest repository/stanza.
type Repository struct {
	S3     S3     `json:"s3"`
	Path   string `json:"path"`
	Stanza string `json:"stanza"`
}

// Backup configures WAL archiving and backups on data members.
type Backup struct {
	Repository Repository `json:"repository"`
	// AdditionalRepositories receive WAL alongside the default repository so
	// schedule/manual storage overrides retain a consistent recovery chain.
	AdditionalRepositories []Repository `json:"additionalRepositories,omitempty"`
	// ArchiveTimeoutSeconds bounds the age of the newest archived WAL (RPO).
	ArchiveTimeoutSeconds int `json:"archiveTimeoutSeconds"`
}

// Bootstrap selects how an empty data directory is initialised.
type Bootstrap struct {
	// Method is "initdb" (default) or "restore".
	Method string `json:"method"`
	// AllowInitDB authorizes only the first cluster initialization. Formed
	// clusters may clone surviving data or restore, never bootstrap empty.
	AllowInitDB bool `json:"allowInitDB,omitempty"`
	// SeedMember restricts DCS initialization during single-to-HA adoption.
	// Other members may stream but must not initialize an empty cluster.
	SeedMember string `json:"seedMember,omitempty"`
	// Restore is set for Method "restore" (in-place restore or clone).
	Restore *Restore `json:"restore,omitempty"`
	// RecoverySuperuser authenticates only the fenced restored database until
	// its passwords are replaced with this cluster's own credentials.
	RecoverySuperuser *Credentials `json:"recoverySuperuser,omitempty"`
	// Generation changes whenever a new bootstrap must be performed, so agents
	// never repeat a completed restore after a restart.
	Generation int `json:"generation,omitempty"`
}

// Restore describes a pgBackRest restore source and target.
type Restore struct {
	Source Repository `json:"source"`
	// Set restores a specific backup label; empty means latest.
	Set string `json:"set,omitempty"`
	// TargetTime is an RFC 3339 PITR target; empty restores to the end of the set/WAL.
	TargetTime string `json:"targetTime,omitempty"`
	// TargetName is a coordinated restore point, mutually exclusive with TargetTime.
	TargetName string `json:"targetName,omitempty"`
}

// ProxyUser is a login role routed by ProxySQL.
type ProxyUser struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// RequireTLS forces TLS on the client leg for this user.
	RequireTLS bool `json:"requireTls,omitempty"`
}

// Proxy configures a ProxySQL member.
type Proxy struct {
	// Servers are data member hosts (port 5432).
	Servers        []string    `json:"servers"`
	Users          []ProxyUser `json:"users"`
	Monitor        Credentials `json:"monitor"`
	Admin          Credentials `json:"admin"`
	ReadWriteSplit bool        `json:"readWriteSplit,omitempty"`
	ReaderPort     bool        `json:"readerPort,omitempty"`
	Threads        int         `json:"threads,omitempty"`
	MaxConnections int         `json:"maxConnections,omitempty"`
	// ServerCert is the frontend certificate (clients) when TLS is enabled.
	ServerCert *Certificate `json:"serverCert,omitempty"`
	// BackendTLS enables TLS from ProxySQL to PostgreSQL.
	BackendTLS bool `json:"backendTls,omitempty"`
}

// Spec is the PostgreSQL engine payload for one member.
type Spec struct {
	DataAccess *DataAccessRoles `json:"dataAccess,omitempty"`
	Role       string           `json:"role"`
	Topology   string           `json:"topology"`
	Flavour    string           `json:"flavour,omitempty"`
	Profiles   []string         `json:"profiles,omitempty"`
	// Scope is the Patroni scope / etcd key namespace (cluster identity).
	Scope string `json:"scope"`
	// Major is the PostgreSQL major version; BinDir its binaries.
	Major  int    `json:"major"`
	BinDir string `json:"binDir"`
	// Version is the full catalog version reported to the control plane.
	Version string `json:"version"`
	Member  string `json:"member"`
	Peers   []Peer `json:"peers,omitempty"`
	// ListenHost selects the backend's private address family for listeners.
	// Empty retains the IPv4 default; supported values are 0.0.0.0 and ::.
	ListenHost string `json:"listenHost,omitempty"`
	// Retiring permanently excludes this identity from Patroni elections.
	Retiring bool `json:"retiring,omitempty"`

	Etcd      *Etcd      `json:"etcd,omitempty"`
	Citus     *Citus     `json:"citus,omitempty"`
	Postgres  *Postgres  `json:"postgres,omitempty"`
	Backup    *Backup    `json:"backup,omitempty"`
	Bootstrap *Bootstrap `json:"bootstrap,omitempty"`
	Proxy     *Proxy     `json:"proxy,omitempty"`

	// System credentials (data members).
	Superuser   Credentials `json:"superuser"`
	Replication Credentials `json:"replication"`
	Rewind      Credentials `json:"rewind"`
	Monitor     Credentials `json:"monitor"`
	PatroniREST Credentials `json:"patroniRest"`

	// TLS material issued by the cluster CA. ServerCert serves PostgreSQL,
	// Patroni REST and etcd (server+client usages); CAPEM is the trust anchor.
	CAPEM      string       `json:"caPEM"`
	ServerCert *Certificate `json:"serverCert,omitempty"`
}

// Action names under /v1/postgres/<action>.
const (
	ActionStatus               = "status"
	ActionBackup               = "backup"
	ActionBackupInfo           = "backup-info"
	ActionExpire               = "expire"
	ActionStanzaCreate         = "stanza-create"
	ActionRestore              = "restore"
	ActionDCSReset             = "dcs-reset"
	ActionSwitchover           = "switchover"
	ActionReinit               = "reinit"
	ActionRestart              = "restart"
	ActionRetireStop           = "retire-stop"
	ActionRetireFence          = "retire-fence"
	ActionEtcdMember           = "etcd-member"
	ActionRoles                = "roles"
	ActionDatabases            = "databases"
	ActionGrants               = "grants"
	ActionSQLBootstrap         = "sql-bootstrap"
	ActionProfileBootstrap     = "profile-bootstrap"
	ActionUpgradeCheck         = "upgrade-check"
	ActionUpgradeRun           = "upgrade-run"
	ActionCitusStatus          = "citus-status"
	ActionCitusRebalanceStart  = "citus-rebalance-start"
	ActionCitusRebalanceStatus = "citus-rebalance-status"
	ActionCitusRebalanceStop   = "citus-rebalance-stop"
	ActionCitusDrainNode       = "citus-drain-node"
	ActionCitusRemoveNode      = "citus-remove-node"
	ActionCitusRestorePoint    = "citus-restore-point"
	ActionWALSwitch            = "wal-switch"
	ActionArchiveCovers        = "archive-covers"
)

// StatusResponse is the live member state.
type StatusResponse struct {
	Citus              *Citus `json:"citus,omitempty"`
	CitusIsCoordinator bool   `json:"citusIsCoordinator,omitempty"`
	// HealthCapabilities distinguishes unsupported observations in older
	// member protocols from failed probes in a capable implementation.
	HealthCapabilities *HealthCapabilities `json:"healthCapabilities,omitempty"`
	// CredentialReceipts advertises durable credential mutation replay.
	CredentialReceipts bool   `json:"credentialReceipts,omitempty"`
	Role               string `json:"role"` // primary, replica, standalone, proxy, starting, stopped
	// Running reports whether PostgreSQL (or ProxySQL) accepts connections.
	Running bool `json:"running"`
	// DataRecoveryRequired refuses empty initialization after data loss.
	DataRecoveryRequired bool `json:"dataRecoveryRequired,omitempty"`
	// DataMissing is a successful filesystem observation, not a failed SQL
	// probe. Individual HA replicas may still safely clone from a live leader.
	DataMissing bool   `json:"dataMissing,omitempty"`
	Leader      string `json:"leader,omitempty"`
	Timeline    int    `json:"timeline,omitempty"`
	// LagBytes is local receive-minus-replay backlog on replicas, or the
	// maximum downstream replay distance on a primary. Cluster readiness
	// compares WALPosition across matching system identifiers and timelines.
	LagBytes  int64  `json:"lagBytes,omitempty"`
	Streaming bool   `json:"streaming,omitempty"`
	SyncState string `json:"syncState,omitempty"`
	Version   string `json:"version,omitempty"`
	// SystemID is the PostgreSQL system identifier.
	SystemID string `json:"systemId,omitempty"`
	// Locale is template1's locale, preserved when initializing pg_upgrade's target.
	Locale string `json:"locale,omitempty"`
	// Bootstrap generation applied by this member.
	BootstrapGeneration int `json:"bootstrapGeneration,omitempty"`
	// EtcdMemberID is the local etcd member ID (hex) when etcd runs.
	EtcdMemberID string `json:"etcdMemberId,omitempty"`
	// EtcdHealthy means a linearizable local request reached a DCS quorum.
	EtcdHealthy bool `json:"etcdHealthy,omitempty"`
	// PendingRestart is Patroni's observed PostgreSQL restart requirement.
	PendingRestart bool `json:"pendingRestart,omitempty"`
	// SharedMemoryParameters are the running, not merely configured, GUC values.
	SharedMemoryParameters map[string]int64 `json:"sharedMemoryParameters,omitempty"`
	// WALPosition is the primary's current LSN or a replica's replay LSN.
	WALPosition string `json:"walPosition,omitempty"`
	// Paused reports Patroni maintenance mode.
	Paused bool `json:"paused,omitempty"`
	// FailoverFenced confirms the local member's retirement tags in /cluster.
	FailoverFenced bool `json:"failoverFenced,omitempty"`
	// RetirementTagsCleared confirms /cluster observed no local election exclusion.
	RetirementTagsCleared bool `json:"retirementTagsCleared,omitempty"`
	// RetirementStopped confirms a durable stop hold and no running database.
	RetirementStopped bool `json:"retirementStopped,omitempty"`
	// LastArchivedAt is pg_stat_archiver.last_archived_time (primary).
	LastArchivedAt     string `json:"lastArchivedAt,omitempty"`
	ArchiveFailing     bool   `json:"archiveFailing,omitempty"`
	ArchiveStatusKnown bool   `json:"archiveStatusKnown,omitempty"`
	// WALStatusKnown distinguishes a failed sample from an empty WAL directory.
	WALStatusKnown   bool              `json:"walStatusKnown,omitempty"`
	WALBytes         int64             `json:"walBytes,omitempty"`
	ArchivePending   int64             `json:"archivePending,omitempty"`
	SlotStatusKnown  bool              `json:"slotStatusKnown,omitempty"`
	ReplicationSlots []ReplicationSlot `json:"replicationSlots,omitempty"`
	// RecoverableUntil bounds WAL content, not archive upload wall time.
	RecoverableUntil string `json:"recoverableUntil,omitempty"`
	// ProxyBackends reports ProxySQL runtime server states (proxy members).
	ProxyBackends []ProxyBackend `json:"proxyBackends,omitempty"`
}

// HealthCapabilities announces implemented observations, independently of
// whether an individual sample succeeded. Absent capabilities are informational
// and must not introduce new readiness requirements for existing members.
type HealthCapabilities struct {
	LocalEtcd           bool `json:"localEtcd,omitempty"`
	ReplicationSlots    bool `json:"replicationSlots,omitempty"`
	WALArchive          bool `json:"walArchive,omitempty"`
	ReplicationDistance bool `json:"replicationDistance,omitempty"`
	PatroniPause        bool `json:"patroniPause,omitempty"`
}

// ReplicationSlot reports retention without changing or dropping the consumer.
type ReplicationSlot struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Active        bool   `json:"active"`
	RetainedBytes int64  `json:"retainedBytes"`
	WALStatus     string `json:"walStatus"`
	SafeWALBytes  *int64 `json:"safeWalBytes,omitempty"`
}

// ProxyBackend is one runtime pgsql_servers row.
type ProxyBackend struct {
	Host      string `json:"host"`
	Hostgroup int    `json:"hostgroup"`
	Status    string `json:"status"`
	// Diagnostics is optional for mixed-version agents. It contains only
	// allowlisted error classes, never native messages or connection strings.
	Diagnostics *ProxyDiagnostics `json:"diagnostics,omitempty"`
}

// ProxyMonitorError identifies the most recent failed native monitor check.
type ProxyMonitorError struct {
	Class         string `json:"class"`
	TimeUnixMicro int64  `json:"timeUnixMicro"`
}

// ProxyDiagnostics attributes routing failures without exposing native errors.
type ProxyDiagnostics struct {
	Connect          *ProxyMonitorError `json:"connect,omitempty"`
	Ping             *ProxyMonitorError `json:"ping,omitempty"`
	ReadOnly         *ProxyMonitorError `json:"readOnly,omitempty"`
	ConnectionErrors uint64             `json:"connectionErrors"`
	PoolObserved     bool               `json:"poolObserved"`
}

// BackupRequest starts (or polls, by Receipt) a pgBackRest backup.
type BackupRequest struct {
	// Receipt is a durable idempotency key (operation ID).
	Receipt    string      `json:"receipt"`
	Type       string      `json:"type"` // full, diff, incr
	Repository *Repository `json:"repository,omitempty"`
	// ObserveOnly polls an existing receipt without starting an absent backup.
	ObserveOnly bool `json:"observeOnly,omitempty"`
}

// BackupResponse reports a backup started/finished under a receipt.
type BackupResponse struct {
	State   string `json:"state"` // running, done, failed
	Label   string `json:"label,omitempty"`
	Message string `json:"message,omitempty"`
}

// BackupInfo is a projection of `pgbackrest info --output=json`.
type BackupInfo struct {
	Label      string            `json:"label"`
	Type       string            `json:"type"`
	Prior      string            `json:"prior,omitempty"`
	StartedAt  string            `json:"startedAt"`
	StoppedAt  string            `json:"stoppedAt"`
	WALStart   string            `json:"walStart,omitempty"`
	WALStop    string            `json:"walStop,omitempty"`
	SizeBytes  int64             `json:"sizeBytes"`
	RepoBytes  int64             `json:"repoBytes"`
	References []string          `json:"references,omitempty"`
	SystemID   string            `json:"systemId,omitempty"`
	Version    string            `json:"version,omitempty"`
	Annotation map[string]string `json:"annotation,omitempty"`
}

// BackupInfoResponse lists repository backups and archive bounds.
type BackupInfoResponse struct {
	Backups    []BackupInfo `json:"backups"`
	ArchiveMin string       `json:"archiveMin,omitempty"`
	ArchiveMax string       `json:"archiveMax,omitempty"`
	SystemID   string       `json:"systemId,omitempty"`
	Version    string       `json:"version,omitempty"`
}

// ExpireRequest removes a backup set and its dependents.
type ExpireRequest struct {
	Set        string      `json:"set"`
	Repository *Repository `json:"repository,omitempty"`
}

// RepositoryRequest selects an override repository without changing WAL archiving.
type RepositoryRequest struct {
	Repository *Repository `json:"repository,omitempty"`
}

// PrepareRestoreRequest fences a data member and optionally empties its PGDATA.
// The receipt prevents a resumed wipe from destroying a newly rebuilt replica.
type PrepareRestoreRequest struct {
	Receipt string `json:"receipt"`
	Wipe    bool   `json:"wipe"`
}

// RestoreRequest identifies a durable offline restore.
type RestoreRequest struct {
	Receipt    string  `json:"receipt"`
	Restore    Restore `json:"restore"`
	Generation int     `json:"generation"`
}

// SQLBootstrapRequest selects the curated flavour bootstrap plan. Passwords are
// secret-bearing and must never be persisted in operation checkpoints.
type SQLBootstrapRequest struct {
	Components []string          `json:"components"`
	Extensions []string          `json:"extensions"`
	Passwords  map[string]string `json:"passwords"`
}

// ProfileBootstrapRequest can select only profiles authorized by member config.
// SQL bytes are supplied exclusively by the immutable profile registry.
type ProfileBootstrapRequest struct {
	Profiles []string `json:"profiles"`
}

// SwitchoverRequest moves leadership (ha).
type SwitchoverRequest struct {
	Candidate string `json:"candidate,omitempty"`
}

// EtcdMemberRequest mutates etcd membership.
type EtcdMemberRequest struct {
	Op       string `json:"op"` // list, add-learner, promote, remove
	Name     string `json:"name,omitempty"`
	PeerURL  string `json:"peerUrl,omitempty"`
	MemberID string `json:"memberId,omitempty"`
}

// EtcdMember is one etcd member.
type EtcdMember struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	PeerURL []string `json:"peerUrls"`
	Learner bool     `json:"learner"`
}

// EtcdMemberResponse returns membership after the operation.
type EtcdMemberResponse struct {
	Members []EtcdMember `json:"members"`
	// InitialCluster is returned by add-learner for the new member's first start.
	InitialCluster string `json:"initialCluster,omitempty"`
}

// ActionResponse is a generic acknowledgement.
type ActionResponse struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// UpgradeRequest identifies a resumable, offline adjacent-major upgrade.
// Replica retires the old data directory so Patroni can rebuild it from the
// upgraded primary; the primary instead performs pg_upgrade --check/--link.
type UpgradeRequest struct {
	Receipt  string `json:"receipt"`
	OldMajor int    `json:"oldMajor"`
	NewMajor int    `json:"newMajor"`
	Locale   string `json:"locale"`
	Replica  bool   `json:"replica,omitempty"`
}

// UpgradeResponse acknowledges a durable upgrade checkpoint.
type UpgradeResponse struct {
	Checked bool `json:"checked"`
	Done    bool `json:"done"`
}

// CitusNode is a pg_dist_node catalog row.
type CitusNode struct {
	NodeID  int    `json:"nodeId"`
	GroupID int    `json:"groupId"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Role    string `json:"role"`
	Active  bool   `json:"active"`
}

type CitusStatusResponse struct {
	Group         int                     `json:"group"`
	IsCoordinator bool                    `json:"isCoordinator"`
	Nodes         []CitusNode             `json:"nodes"`
	Rebalance     *CitusRebalanceResponse `json:"rebalance,omitempty"`
}

type CitusRebalanceStartRequest struct {
	Strategy string `json:"strategy"`
	Receipt  string `json:"receipt"`
}

type CitusRebalanceRequest struct {
	JobID int64 `json:"jobId"`
}

type CitusRebalanceResponse struct {
	JobID int64  `json:"jobId"`
	State string `json:"state"`
}

type CitusNodeRequest struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type CitusRestorePointRequest struct {
	Name string `json:"name"`
}

type CitusRestorePoint struct {
	NodeID int    `json:"nodeId"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	// LSN is the exact coordinator marker position, or a conservative
	// post-marker WAL insertion upper bound on a worker primary.
	LSN string `json:"lsn"`
}

type CitusRestorePointResponse struct {
	Nodes []CitusRestorePoint `json:"nodes"`
}

type WALSwitchResponse struct {
	LSN string `json:"lsn"`
}

type ArchiveCoversRequest struct {
	LSN string `json:"lsn"`
}

type ArchiveCoversResponse struct {
	Covers bool `json:"covers"`
}

// DCSResetRequest defaults to a single Patroni group. Formation must be
// explicitly authorized to erase the complete distributed DCS namespace.
type DCSResetRequest struct {
	Formation bool `json:"formation,omitempty"`
}
