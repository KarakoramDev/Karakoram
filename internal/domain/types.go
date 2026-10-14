// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package domain

import (
	"encoding/json"
	"time"
)

// Phase identifies a resource lifecycle state.
type Phase string

const (
	PhaseCreating     Phase = "Creating"
	PhaseInitializing Phase = "Initializing"
	PhaseReady        Phase = "Ready"
	PhaseUpdating     Phase = "Updating"
	PhaseScaling      Phase = "Scaling"
	PhaseUpgrading    Phase = "Upgrading"
	PhaseRestoring    Phase = "Restoring"
	PhaseDegraded     Phase = "Degraded"
	PhaseError        Phase = "Error"
	PhaseDeleting     Phase = "Deleting"
	PhaseSuspended    Phase = "Suspended"
	PhasePending      Phase = "Pending"
	PhaseRunning      Phase = "Running"
	PhaseSucceeded    Phase = "Succeeded"
	PhaseFailed       Phase = "Failed"
)
const (
	OperationBackup           = "backup"
	OperationDeleteBackup     = "delete-backup"
	OperationRestore          = "restore"
	OperationRaiseFCV         = "raise-fcv"
	OperationRestart          = "restart"
	OperationReplaceMember    = "replace-member"
	OperationRotateSystemUser = "rotate-system-user"
	OperationRotateTLS        = "rotate-tls"
)

// Cluster holds desired state, observed state, and provider-owned durable state.
type Cluster struct {
	Namespace  string          `json:"namespace,omitempty"`
	Name       string          `json:"name"`
	Spec       ClusterSpec     `json:"spec"`
	Status     ClusterStatus   `json:"status"`
	Internal   json.RawMessage `json:"-"`
	Generation int64           `json:"generation"`
	// RequestedBy belongs to Generation, not to observed-state updates.
	// It remains empty for generations whose historical requester is unknown.
	RequestedBy   string        `json:"-"`
	Deleting      bool          `json:"deleting"`
	DeleteOptions DeleteOptions `json:"deleteOptions,omitempty"`
	CreatedAt     time.Time     `json:"createdAt"`
	UpdatedAt     time.Time     `json:"updatedAt"`
}

// ClusterSpec declares the desired engine, topology, and operational configuration.
type ClusterSpec struct {
	Provider     string           `json:"provider"`
	Version      string           `json:"version"`
	Topology     Topology         `json:"topology"`
	Components   Components       `json:"components"`
	EngineConfig string           `json:"engineConfig,omitempty"`
	Backup       BackupConfig     `json:"backup"`
	TLS          TLSConfig        `json:"tls"`
	Exposure     Exposure         `json:"exposure"`
	Monitoring   MonitoringConfig `json:"monitoring"`
	DataSource   *DataSource      `json:"dataSource,omitempty"`
}

// Topology selects replica-set or sharded deployment and shard count.
type Topology struct {
	Type   string `json:"type"`
	Shards int    `json:"shards,omitempty"`
}

// Components groups replica and resource settings for each component.
type Components struct {
	Engine       ComponentSpec `json:"engine"`
	ConfigServer ComponentSpec `json:"configServer"`
	Router       ComponentSpec `json:"router"`
}

// ComponentSpec defines replica count and infrastructure sizing.
type ComponentSpec struct {
	Replicas  int       `json:"replicas"`
	Resources Resources `json:"resources"`
}

// Resources expresses Zerops CPU, RAM, and disk autoscaling bounds.
type Resources struct {
	CPUMode  string   `json:"cpuMode"`
	MinCPU   float64  `json:"minCpu"`
	MaxCPU   float64  `json:"maxCpu"`
	MinRAM   float64  `json:"minRam"`
	MaxRAM   float64  `json:"maxRam"`
	MinDisk  float64  `json:"minDisk"`
	MaxDisk  float64  `json:"maxDisk"`
	StartCPU *float64 `json:"startCpu,omitempty"`
}

// BackupConfig configures storage, schedules, and point-in-time recovery.
type BackupConfig struct {
	Storage   string           `json:"storage,omitempty"`
	Schedules []BackupSchedule `json:"schedules"`
	PITR      PITRConfig       `json:"pitr"`
}

// BackupSchedule defines a named cron backup and count-based retention policy.
type BackupSchedule struct {
	Name        string `json:"name"`
	Cron        string `json:"cron"`
	Type        string `json:"type"`
	Storage     string `json:"storage"`
	RetainCount int    `json:"retainCount"`
	Enabled     bool   `json:"enabled"`
}

// PITRConfig controls oplog capture and slicing.
type PITRConfig struct {
	Enabled          bool `json:"enabled"`
	OplogSpanMinutes int  `json:"oplogSpanMinutes"`
}

// TLSConfig configures transport encryption and certificate material.
type TLSConfig struct {
	Mode string `json:"mode"`
	CA   string `json:"ca"`
}

// Exposure defines optional public TCP access and firewall rules.
type Exposure struct {
	Public    bool     `json:"public"`
	IPType    string   `json:"ipType"`
	Allowlist []string `json:"allowlist"`
}

// MonitoringConfig controls engine monitoring.
type MonitoringConfig struct {
	Enabled bool `json:"enabled"`
}

// DataSource identifies the source of a new cluster restore.
type DataSource struct {
	Namespace string     `json:"namespace,omitempty"`
	Cluster   string     `json:"cluster,omitempty"`
	Backup    string     `json:"backup,omitempty"`
	PITRTime  *time.Time `json:"pitrTime,omitempty"`
}

// DeleteOptions controls preservation of volumes and backup objects.
type DeleteOptions struct {
	KeepVolumes bool `json:"keepVolumes"`
	KeepBackups bool `json:"keepBackups"`
}

// ClusterStatus captures the latest observed cluster health and connectivity.
type ClusterStatus struct {
	Phase              Phase          `json:"phase"`
	Message            string         `json:"message,omitempty"`
	Conditions         []Condition    `json:"conditions"`
	ObservedGeneration int64          `json:"observedGeneration"`
	Version            string         `json:"version,omitempty"`
	FCV                string         `json:"fcv,omitempty"`
	Members            []MemberStatus `json:"members"`
	Connection         Connection     `json:"connection"`
	Backup             BackupStatus   `json:"backup"`
	Placement          Placement      `json:"placement"`
}

// Condition describes one observed aspect of resource health.
type Condition struct {
	Type               string    `json:"type"`
	Status             string    `json:"status"`
	Reason             string    `json:"reason,omitempty"`
	Message            string    `json:"message,omitempty"`
	LastTransitionTime time.Time `json:"lastTransitionTime"`
}

// MemberStatus reports the state and resource usage of one database member.
type MemberStatus struct {
	Name           string      `json:"name"`
	Set            string      `json:"set"`
	Role           string      `json:"role"`
	State          string      `json:"state"`
	Health         bool        `json:"health"`
	Version        string      `json:"version"`
	LagSeconds     float64     `json:"lagSeconds"`
	Optime         time.Time   `json:"optime"`
	Uptime         int64       `json:"uptime"`
	CPUUsage       float64     `json:"cpuUsage"`
	RAMUsage       float64     `json:"ramUsage"`
	DiskUsage      float64     `json:"diskUsage"`
	Agent          AgentStatus `json:"agent"`
	PlacementGroup string      `json:"placementGroup,omitempty"`
	// PublicHost is the member's public "host:port" when exposure is enabled.
	PublicHost string `json:"publicHost,omitempty"`
	// Resources are the infrastructure resources currently allocated to the
	// member's container as reported by the backend.
	Resources *ObservedResources `json:"resources,omitempty"`
	// Autoscaling contains effective runtime bounds read from Zerops service
	// configuration, independently of desired spec and current allocation.
	Autoscaling *Resources `json:"autoscaling,omitempty"`
}

// ObservedResources are resources currently allocated by the infrastructure.
type ObservedResources struct {
	CPUCores float64 `json:"cpuCores"`
	MemoryGB float64 `json:"memoryGB"`
	DiskGB   float64 `json:"diskGB"`
}

// AgentStatus reports agent connectivity and applied configuration.
type AgentStatus struct {
	Healthy       bool      `json:"healthy"`
	ConfigVersion int64     `json:"configVersion"`
	LastSeen      time.Time `json:"lastSeen"`
	Message       string    `json:"message,omitempty"`
}

// Connection contains password-free connection discovery information.
type Connection struct {
	Hosts       []string `json:"hosts"`
	ReplicaSet  string   `json:"replicaSet,omitempty"`
	URI         string   `json:"uri"`
	PublicHosts []string `json:"publicHosts"`
}

// TimeWindow bounds a restorable point-in-time interval.
type TimeWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// BackupStatus summarizes backup recency and PITR availability.
type BackupStatus struct {
	PITRWindow *TimeWindow `json:"pitrWindow,omitempty"`
	LastBackup *time.Time  `json:"lastBackup,omitempty"`
}

// Placement reports best-effort host placement without promising anti-affinity.
type Placement struct {
	Warning string              `json:"warning,omitempty"`
	Groups  map[string][]string `json:"groups"`
}

// ProviderInfo describes a provider catalog and supported capabilities.
type ProviderInfo struct {
	Name         string         `json:"name"`
	DisplayName  string         `json:"displayName"`
	Versions     []VersionInfo  `json:"versions"`
	UpgradePaths []UpgradePath  `json:"upgradePaths"`
	Topologies   []TopologyInfo `json:"topologies"`
	Presets      []Preset       `json:"presets"`
	BackupTypes  []string       `json:"backupTypes"`
}

// VersionInfo describes a supported engine version.
type VersionInfo struct {
	Version string `json:"version"`
	Major   string `json:"major"`
	Default bool   `json:"default"`
	Status  string `json:"status"`
	// PBMVersion is the backup agent version bundled with this engine version.
	PBMVersion string `json:"pbmVersion,omitempty"`
}

// UpgradePath identifies one permitted version transition.
type UpgradePath struct {
	From string `json:"from"`
	To   string `json:"to"`
	// RequiresFCV is the feature compatibility version the cluster must have
	// before this upgrade may start (major upgrades only).
	RequiresFCV string `json:"requiresFCV,omitempty"`
	// RequiresBackup recommends a fresh backup before the upgrade.
	RequiresBackup bool `json:"requiresBackup,omitempty"`
}

// TopologyInfo describes component defaults and limits for a topology.
type TopologyInfo struct {
	Type       string                   `json:"type"`
	Components map[string]ComponentInfo `json:"components"`
	Schema     json.RawMessage          `json:"schema,omitempty"`
}

// ComponentInfo describes replica bounds and form schema for a component.
type ComponentInfo struct {
	Defaults        ComponentSpec   `json:"defaults"`
	MinReplicas     int             `json:"minReplicas"`
	MaxReplicas     int             `json:"maxReplicas"`
	AllowedReplicas []int           `json:"allowedReplicas,omitempty"`
	Schema          json.RawMessage `json:"schema,omitempty"`
}

// Preset provides a named component-sizing configuration.
type Preset struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Components  Components `json:"components"`
	// Topology is the topology the preset deploys (replicaSet or sharded).
	Topology Topology `json:"topology"`
}

// BackupStorage describes an S3-compatible destination; credentials never marshal publicly.
type BackupStorage struct {
	Namespace      string             `json:"namespace,omitempty"`
	Name           string             `json:"name"`
	Type           string             `json:"type"`
	EndpointURL    string             `json:"endpointURL"`
	Region         string             `json:"region"`
	Bucket         string             `json:"bucket"`
	Prefix         string             `json:"prefix"`
	ForcePathStyle bool               `json:"forcePathStyle"`
	VerifyTLS      bool               `json:"verifyTLS"`
	Credentials    StorageCredentials `json:"-"`
	Description    string             `json:"description"`
}

// StorageCredentials contains write-only S3 authentication material.
type StorageCredentials struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
}

// Backup tracks one durable backup inventory entry.
type Backup struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
	Cluster   string `json:"cluster"`
	Storage   string `json:"storage"`
	Type      string `json:"type"`
	Trigger   string `json:"trigger"`
	Schedule  string `json:"schedule,omitempty"`
	Phase     Phase  `json:"phase"`
	PBMName   string `json:"pbmName,omitempty"`
	// Provenance survives deletion of the source cluster. PBMPrefix is the
	// complete destination prefix at snapshot time, not the mutable storage prefix.
	PBMPrefix      string   `json:"pbmPrefix,omitempty"`
	SourceTopology Topology `json:"sourceTopology,omitempty"`
	SourceVersion  string   `json:"sourceVersion,omitempty"`
	// RecoveryIdentity refers to an encrypted snapshot-time secret scope.
	RecoveryIdentity string     `json:"recoveryIdentity,omitempty"`
	StartedAt        *time.Time `json:"startedAt,omitempty"`
	CompletedAt      *time.Time `json:"completedAt,omitempty"`
	LastWriteTime    *time.Time `json:"lastWriteTime,omitempty"`
	Size             int64      `json:"size"`
	Error            string     `json:"error,omitempty"`
	CreatedAt        time.Time  `json:"createdAt"`
}

// Restore tracks a restore into its target cluster.
type Restore struct {
	Namespace       string        `json:"namespace,omitempty"`
	SourceNamespace string        `json:"sourceNamespace,omitempty"`
	Name            string        `json:"name"`
	Cluster         string        `json:"cluster"`
	Source          RestoreSource `json:"source"`
	Phase           Phase         `json:"phase"`
	StartedAt       *time.Time    `json:"startedAt,omitempty"`
	CompletedAt     *time.Time    `json:"completedAt,omitempty"`
	Error           string        `json:"error,omitempty"`
	CreatedAt       time.Time     `json:"createdAt"`
}

// RestoreSource selects a backup or point in time from a source cluster.
type RestoreSource struct {
	Backup        string     `json:"backup,omitempty"`
	PITRTime      *time.Time `json:"pitrTime,omitempty"`
	SourceCluster string     `json:"sourceCluster,omitempty"`
}

// Operation tracks a durable, checkpointed imperative action.
type Operation struct {
	Namespace   string          `json:"namespace,omitempty"`
	ID          string          `json:"id"`
	Cluster     string          `json:"cluster"`
	Kind        string          `json:"kind"`
	Phase       Phase           `json:"phase"`
	Step        string          `json:"step,omitempty"`
	Message     string          `json:"message,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	Checkpoint  json.RawMessage `json:"checkpoint,omitempty"`
	Attempts    int             `json:"attempts"`
	CreatedAt   time.Time       `json:"createdAt"`
	UpdatedAt   time.Time       `json:"updatedAt"`
	StartedAt   *time.Time      `json:"startedAt,omitempty"`
	CompletedAt *time.Time      `json:"completedAt,omitempty"`
	Actor       string          `json:"actor"`
}

// Event is a durable audit and lifecycle notification.
type Event struct {
	Namespace string    `json:"namespace,omitempty"`
	ID        int64     `json:"id"`
	Cluster   string    `json:"cluster,omitempty"`
	Resource  string    `json:"resource,omitempty"`
	Target    string    `json:"target,omitempty"`
	Operation string    `json:"operation,omitempty"`
	Type      string    `json:"type"`
	Reason    string    `json:"reason"`
	Message   string    `json:"message"`
	Actor     string    `json:"actor,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

// Account describes a local identity without exposing its password hash.
type Account struct {
	Name               string    `json:"name"`
	DisplayName        string    `json:"displayName"`
	PasswordHash       string    `json:"-"`
	Roles              []string  `json:"roles"`
	MustChangePassword bool      `json:"mustChangePassword"`
	Disabled           bool      `json:"disabled"`
	CreatedAt          time.Time `json:"createdAt"`
	PasswordChangedAt  time.Time `json:"passwordChangedAt,omitempty"`
	OIDC               bool      `json:"oidc"`
	OIDCIssuer         string    `json:"oidcIssuer,omitempty"`
	OIDCSubject        string    `json:"oidcSubject,omitempty"`
}

// ApiToken stores API token ownership and lifecycle metadata.
type ApiToken struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Hash       string     `json:"-"`
	Owner      string     `json:"owner"`
	ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
	LastUsedAt *time.Time `json:"lastUsedAt,omitempty"`
	CreatedAt  time.Time  `json:"createdAt"`
}

// Session stores a rotating refresh-token session.
type Session struct {
	ID                string    `json:"id"`
	Owner             string    `json:"owner"`
	RefreshHash       string    `json:"-"`
	ExpiresAt         time.Time `json:"expiresAt"`
	CreatedAt         time.Time `json:"createdAt"`
	CSRF              string    `json:"csrf"`
	Subjects          []string  `json:"subjects,omitempty"`
	Via               string    `json:"via"`
	UsedRefreshHashes []string  `json:"usedRefreshHashes,omitempty"`
	Revoked           bool      `json:"revoked"`
}

// PolicyRule is one ordered Casbin policy or grouping rule.
type PolicyRule struct {
	PType  string   `json:"ptype"`
	Values []string `json:"values"`
}

// AgentToken binds a member identity to hashed and encrypted authentication material.
type AgentToken struct {
	Namespace string `json:"namespace,omitempty"`
	Cluster   string `json:"cluster"`
	Member    string `json:"member"`
	Hash      string `json:"-"`
	Token     string `json:"-"`
}

// MetricSample is one timestamped member metric with optional labels.
type MetricSample struct {
	Namespace string            `json:"namespace,omitempty"`
	Cluster   string            `json:"cluster"`
	Member    string            `json:"member"`
	Metric    string            `json:"metric"`
	Timestamp time.Time         `json:"timestamp"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// RetainedVolume preserves runtime-to-volume mapping for cluster resumption.
type RetainedVolume struct {
	Namespace  string    `json:"namespace,omitempty"`
	Cluster    string    `json:"cluster"`
	Member     string    `json:"member"`
	Hostname   string    `json:"hostname"`
	ServiceID  string    `json:"serviceId"`
	Set        string    `json:"set"`
	Role       string    `json:"role"`
	Index      int       `json:"index"`
	RetainedAt time.Time `json:"retainedAt"`
}

// DatabaseUser describes a MongoDB application user.
type DatabaseUser struct {
	Name     string    `json:"name"`
	Database string    `json:"database"`
	Roles    []RoleRef `json:"roles"`
	Password string    `json:"-"`
}

// RoleRef references a role in a MongoDB database.
type RoleRef struct {
	Role string `json:"role"`
	DB   string `json:"db"`
}

// DatabaseRole describes a MongoDB application role.
type DatabaseRole struct {
	Name       string      `json:"name"`
	Database   string      `json:"database"`
	Roles      []RoleRef   `json:"roles"`
	Privileges []Privilege `json:"privileges"`
}

// Privilege associates MongoDB resources with allowed actions.
type Privilege struct {
	Resource json.RawMessage `json:"resource"`
	Actions  []string        `json:"actions"`
}

// SystemUser describes a fixed-purpose engine account.
type SystemUser struct {
	Name          string     `json:"name"`
	Roles         []RoleRef  `json:"roles"`
	LastRotatedAt *time.Time `json:"lastRotatedAt,omitempty"`
}
