// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"encoding/json"
	"time"
)

// DefaultPort is the private HTTPS port used by node agents.
const DefaultPort = 9443
const (
	RoleReplicaSet   = "mongod-rs"
	RoleConfigServer = "mongod-cfg"
	RoleRouter       = "mongos"
)

// MemberConfig is the secret-bearing desired configuration pulled by a node agent.
type MemberConfig struct {
	Cluster               string                 `json:"cluster"`
	Member                string                 `json:"member"`
	Volume                *VolumeIdentity        `json:"volume,omitempty"`
	Engine                string                 `json:"engine,omitempty"`
	EngineSpec            json.RawMessage        `json:"engineSpec,omitempty"`
	Role                  string                 `json:"role"`
	ReplSetName           string                 `json:"replSetName"`
	Port                  int                    `json:"port"`
	ConfigDB              string                 `json:"configDB,omitempty"`
	ShardServer           bool                   `json:"shardServer,omitempty"`
	WiredTigerCacheSizeGB float64                `json:"wiredTigerCacheSizeGB,omitempty"`
	SlowOpThresholdMS     int64                  `json:"slowOpThresholdMS,omitempty"`
	EngineVersion         string                 `json:"engineVersion"`
	EngineConfig          string                 `json:"engineConfig"`
	Keyfile               string                 `json:"keyfile"`
	TLS                   TLSConfig              `json:"tls"`
	AgentTLS              Certificate            `json:"agentTLS"`
	SystemUsers           map[string]Credentials `json:"systemUsers"`
	PBMEnabled            bool                   `json:"pbmEnabled"`
	// PBMGoMemoryLimitBytes is the CP-derived Go runtime soft limit for the
	// next PBM child launch. Zero means an old/unknown budget, not unlimited RAM.
	PBMGoMemoryLimitBytes uint64   `json:"pbmGoMemoryLimitBytes,omitempty"`
	DesiredProcesses      []string `json:"desiredProcesses"`
	Maintenance           bool     `json:"maintenance"`
	ConfigVersion         int64    `json:"configVersion"`
	// CertificateReloadOnly requests identity publication without engine rollout.
	// The CP must first observe the matching agent capability.
	CertificateReloadOnly bool `json:"certificateReloadOnly,omitempty"`
}

// Certificate contains a PEM certificate and its private key.
type Certificate struct {
	CertPEM string `json:"certPEM"`
	KeyPEM  string `json:"keyPEM"`
}

// TLSConfig configures transport encryption and certificate material.
type TLSConfig struct {
	Mode    string `json:"mode"`
	CAPEM   string `json:"caPEM"`
	CertPEM string `json:"certPEM"`
	KeyPEM  string `json:"keyPEM"`
}

// Credentials contains local system-user authentication material.
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

// Status reports process, disk, and applied agent configuration state.
type Status struct {
	Agent           AgentBuild               `json:"agent"`
	Processes       map[string]ProcessStatus `json:"processes"`
	Volume          *VolumeStatus            `json:"volume,omitempty"`
	EngineVersion   string                   `json:"engineVersion"`
	ConfigVersion   int64                    `json:"configVersion"`
	Disk            DiskUsage                `json:"disk"`
	HostFingerprint string                   `json:"hostFingerprint"`
	Maintenance     bool                     `json:"maintenance"`
	UptimeSeconds   int64                    `json:"uptimeSeconds"`
	// PBMRestoreFence advertises supervised stop/wait/start confirmation.
	PBMRestoreFence bool `json:"pbmRestoreFence,omitempty"`
	// PBMRestoreIdentity advertises restore-scoped PBM authentication support.
	PBMRestoreIdentity bool `json:"pbmRestoreIdentity,omitempty"`
	// CertificateReloadOnly advertises reload-only native certificate application.
	CertificateReloadOnly bool `json:"certificateReloadOnly,omitempty"`
	// Error explains a blocked local recovery without making status unreachable.
	Error string `json:"error,omitempty"`
	// MongoReady observes authenticated router access to config-server metadata.
	// Nil means this agent did not observe router application readiness.
	MongoReady *bool  `json:"mongoReady,omitempty"`
	MongoError string `json:"mongoError,omitempty"`
	// Resources is what the container can currently use, read at request
	// time. Nil from agents that predate the field or could read nothing.
	Resources *ContainerResources `json:"resources,omitempty"`
}

// AgentBuild is the running Karakoram executable, not the database version.
type AgentBuild struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Build   string `json:"build"`
}

// ContainerResources reports the CPU, memory, and root disk a member
// container currently has. Zero means the value could not be read.
type ContainerResources struct {
	// CPUCores are usable cores: the effective cgroup cpuset size, capped by
	// a cpu.max quota when one is set.
	CPUCores float64 `json:"cpuCores"`
	// MemoryBytes is the cgroup memory limit, or MemTotal without one.
	MemoryBytes uint64 `json:"memoryBytes"`
	// RootDiskBytes is the size of the filesystem mounted at /.
	RootDiskBytes uint64 `json:"rootDiskBytes"`
}

// ProcessStatus describes one supervised child process.
type ProcessStatus struct {
	State     string     `json:"state"`
	PID       int        `json:"pid"`
	Restarts  int        `json:"restarts"`
	StartedAt *time.Time `json:"startedAt,omitempty"`
	ExitCode  *int       `json:"exitCode,omitempty"`
	Error     string     `json:"error,omitempty"`
	// SupervisorSession and Launch identify actual successful child launches.
	// A higher Launch in the same session proves the previous child was reaped;
	// unlike StartedAt, these identities do not compare wall clocks.
	SupervisorSession string `json:"supervisorSession,omitempty"`
	Launch            uint64 `json:"launch,omitempty"`
	// GoMemoryLimitBytes reports the limit configured at this child's launch;
	// changing desired configuration alone does not change a running process.
	GoMemoryLimitBytes uint64 `json:"goMemoryLimitBytes,omitempty"`
}

// DiskUsage reports data-directory filesystem capacity.
type DiskUsage struct {
	TotalBytes uint64 `json:"totalBytes"`
	UsedBytes  uint64 `json:"usedBytes"`
	FreeBytes  uint64 `json:"freeBytes"`
}

// ProcessAction selects a whitelisted process and supervisor action.
type ProcessAction struct {
	Process string `json:"process"`
	Action  string `json:"action"`
	// RequestID is stable across retries of one logical restart.
	RequestID string `json:"requestID,omitempty"`
	// RestoreCommandTS requests a PBM-only active fence with a member clock guard.
	RestoreCommandTS int64 `json:"restoreCommandTS,omitempty"`
}

// Maintenance controls automatic database process restart suppression.
type Maintenance struct {
	Enabled bool `json:"enabled"`
}

// BootstrapRequest describes idempotent localhost-exception replica-set initialization.
type BootstrapRequest struct {
	ReplSetName  string            `json:"replSetName"`
	Members      []BootstrapMember `json:"members"`
	UserAdmin    Credentials       `json:"userAdmin"`
	ConfigServer bool              `json:"configServer"`
}

// BootstrapMember defines one initial replica-set member.
type BootstrapMember struct {
	ID       int      `json:"id"`
	Host     string   `json:"host"`
	Priority *float64 `json:"priority,omitempty"`
	Votes    *int     `json:"votes,omitempty"`
}

// BootstrapResponse reports replica-set and user bootstrap outcomes.
type BootstrapResponse struct {
	Initialized bool `json:"initialized"`
	UserCreated bool `json:"userCreated"`
	// Primary redirects localhost-exception creation to the elected member.
	// Empty means credentials were created or verified on this invocation.
	Primary string `json:"primary,omitempty"`
}

// PBMRequest selects a whitelisted PBM command and arguments.
type PBMRequest struct {
	Command   string          `json:"command"`
	RequestID string          `json:"requestID,omitempty"`
	Args      []string        `json:"args,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`
}

// PBMResponse contains structured PBM output and exit status.
type PBMResponse struct {
	Output   json.RawMessage `json:"output,omitempty"`
	Stderr   string          `json:"stderr,omitempty"`
	ExitCode int             `json:"exitCode"`
}

// LogLine is a timestamped process or container log entry.
type LogLine struct {
	Timestamp time.Time `json:"timestamp"`
	Process   string    `json:"process"`
	Stream    string    `json:"stream"`
	Message   string    `json:"message"`
	// Launch identity scopes retained process output to one supervised child.
	SupervisorSession string `json:"supervisorSession,omitempty"`
	Launch            uint64 `json:"launch,omitempty"`
	// ExitCode is populated in retained logs only after this launch is reaped.
	ExitCode *int `json:"exitCode,omitempty"`
}

// LogsResponse contains bounded agent process logs.
type LogsResponse struct {
	Lines []LogLine `json:"lines"`
}

// ActionResponse acknowledges an agent control request.
type ActionResponse struct {
	Success  bool             `json:"success"`
	Message  string           `json:"message,omitempty"`
	PBMFence *PBMRestartFence `json:"pbmFence,omitempty"`
}

// PBMRestartFence confirms active process replacement, not inferred history.
type PBMRestartFence struct {
	OldExited    bool      `json:"oldExited"`
	PID          int       `json:"pid"`
	StartedAfter time.Time `json:"startedAfter"`
	MemberTime   time.Time `json:"memberTime"`
}

// PBMRestoreFenceClockMargin keeps the old command behind a new PBM listener.
const PBMRestoreFenceClockMargin = 30 * time.Second

// ErrorResponse describes an agent protocol error.
type ErrorResponse struct {
	Error string `json:"error"`
}
