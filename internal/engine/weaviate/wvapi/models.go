// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package wvapi

import "encoding/json"

// Models mirror the v1.40.1 OpenAPI property spelling. BackupConfig deliberately
// excludes enterprise deduplication controls and the obsolete ChunkSize field.
// Meta is the native Meta model.
type Meta struct {
	Hostname           string          `json:"hostname,omitempty"`
	Version            string          `json:"version,omitempty"`
	Modules            json.RawMessage `json:"modules,omitempty"`
	GRPCMaxMessageSize int64           `json:"grpcMaxMessageSize,omitempty"`
	License            MetaLicense     `json:"license,omitempty"`
}

// Node is the native Node model.
type Node struct {
	Name            string            `json:"name,omitempty"`
	Status          string            `json:"status,omitempty"`
	Version         string            `json:"version,omitempty"`
	GitHash         string            `json:"gitHash,omitempty"`
	Stats           NodeStats         `json:"stats,omitempty"`
	BatchStats      BatchStats        `json:"batchStats,omitempty"`
	Shards          []NodeShardStatus `json:"shards,omitempty"`
	OperationalMode string            `json:"operationalMode,omitempty"`
}

// ClusterStatistics is the native ClusterStatistics model.
type ClusterStatistics struct {
	Statistics   []Statistics `json:"statistics,omitempty"`
	Synchronized bool         `json:"synchronized"`
}

// ShardStatus is the native ShardStatus model.
type ShardStatus struct {
	Name            string            `json:"name,omitempty"`
	Status          string            `json:"status,omitempty"`
	PerNodeStatus   map[string]string `json:"per_node_status,omitempty"`
	VectorQueueSize int64             `json:"vectorQueueSize"`
}

// ReplicateRequest is the native ReplicateRequest model.
type ReplicateRequest struct {
	SourceNode string      `json:"sourceNode"`
	TargetNode string      `json:"targetNode"`
	Collection string      `json:"collection"`
	Shard      string      `json:"shard"`
	Type       ReplicaType `json:"type,omitempty"`
}

// ReplicateResponse is the native ReplicateResponse model.
type ReplicateResponse struct {
	ID string `json:"id"`
}

// Replication is the native Replication model.
type Replication struct {
	ID                 string              `json:"id"`
	Shard              string              `json:"shard"`
	Collection         string              `json:"collection"`
	SourceNode         string              `json:"sourceNode"`
	TargetNode         string              `json:"targetNode"`
	Type               ReplicaType         `json:"type"`
	Uncancelable       bool                `json:"uncancelable,omitempty"`
	ScheduledForCancel bool                `json:"scheduledForCancel,omitempty"`
	ScheduledForDelete bool                `json:"scheduledForDelete,omitempty"`
	Status             ReplicationStatus   `json:"status"`
	StatusHistory      []ReplicationStatus `json:"statusHistory,omitempty"`
	WhenStartedUnixMs  int64               `json:"whenStartedUnixMs,omitempty"`
}

// ShardingStateResponse is the native ShardingStateResponse model.
type ShardingStateResponse struct {
	ShardingState ShardingState `json:"shardingState,omitempty"`
}

// DBUser is the native DBUser model.
type DBUser struct {
	Roles              []string `json:"roles"`
	UserID             string   `json:"userId"`
	DBUserType         string   `json:"dbUserType"`
	Active             bool     `json:"active"`
	CreatedAt          *string  `json:"createdAt,omitempty"`
	APIKeyFirstLetters *string  `json:"apiKeyFirstLetters,omitempty"`
	LastUsedAt         *string  `json:"lastUsedAt,omitempty"`
	Namespace          string   `json:"namespace,omitempty"`
}

// APIKey is the native APIKey model.
type APIKey struct {
	APIKey string `json:"apikey"`
}

// Role is the native Role model.
type Role struct {
	Name        string       `json:"name"`
	Permissions []Permission `json:"permissions"`
}

// BackupRequest is the native BackupRequest model.
type BackupRequest struct {
	ID           string       `json:"id,omitempty"`
	Config       BackupConfig `json:"config,omitempty"`
	Include      []string     `json:"include,omitempty"`
	Exclude      []string     `json:"exclude,omitempty"`
	IncludeUsers *[]string    `json:"includeUsers,omitempty"`
	IncludeRoles *[]string    `json:"includeRoles,omitempty"`
}

// Backup is the native Backup model.
type Backup struct {
	ID      string      `json:"id,omitempty"`
	Classes []string    `json:"classes,omitempty"`
	Backend string      `json:"backend,omitempty"`
	Bucket  string      `json:"bucket,omitempty"`
	Path    string      `json:"path,omitempty"`
	Error   string      `json:"error,omitempty"`
	Status  BackupState `json:"status,omitempty"`
}

// BackupStatus is the native BackupStatus model.
type BackupStatus struct {
	ID                      string      `json:"id,omitempty"`
	Backend                 string      `json:"backend,omitempty"`
	Path                    string      `json:"path,omitempty"`
	Error                   string      `json:"error,omitempty"`
	Status                  BackupState `json:"status,omitempty"`
	StartedAt               string      `json:"startedAt,omitempty"`
	CompletedAt             string      `json:"completedAt,omitempty"`
	Size                    float64     `json:"size,omitempty"`
	IncrementalBaseBackupId string      `json:"incremental_base_backup_id,omitempty"`
}

// RestoreRequest is the native RestoreRequest model.
type RestoreRequest struct {
	Config         RestoreConfig     `json:"config,omitempty"`
	Include        []string          `json:"include,omitempty"`
	Exclude        []string          `json:"exclude,omitempty"`
	NodeMapping    map[string]string `json:"node_mapping,omitempty"`
	OverwriteAlias bool              `json:"overwriteAlias,omitempty"`
}

// Restore is the native Restore model.
type Restore struct {
	ID      string      `json:"id,omitempty"`
	Classes []string    `json:"classes,omitempty"`
	Backend string      `json:"backend,omitempty"`
	Path    string      `json:"path,omitempty"`
	Error   string      `json:"error,omitempty"`
	Status  BackupState `json:"status,omitempty"`
}

// RestoreStatus is the native RestoreStatus model.
type RestoreStatus struct {
	ID      string      `json:"id,omitempty"`
	Backend string      `json:"backend,omitempty"`
	Path    string      `json:"path,omitempty"`
	Error   string      `json:"error,omitempty"`
	Status  BackupState `json:"status,omitempty"`
}

// MetaLicense is the native MetaLicense model.
type MetaLicense struct {
	Edition           string  `json:"edition,omitempty"`
	DocumentationHref string  `json:"documentationHref,omitempty"`
	Status            string  `json:"status,omitempty"`
	LicenseId         string  `json:"licenseId,omitempty"`
	ExpiresAt         *string `json:"expiresAt,omitempty"`
	LastCheckedAt     *string `json:"lastCheckedAt,omitempty"`
	GracePeriodEndsAt *string `json:"gracePeriodEndsAt,omitempty"`
	Enforcing         bool    `json:"enforcing,omitempty"`
	ClusterMismatch   bool    `json:"clusterMismatch,omitempty"`
}

// NodeStats is the native NodeStats model.
type NodeStats struct {
	ShardCount  int   `json:"shardCount"`
	ObjectCount int64 `json:"objectCount"`
}

// BatchStats is the native BatchStats model.
type BatchStats struct {
	QueueLength   *int `json:"queueLength,omitempty"`
	RatePerSecond int  `json:"ratePerSecond"`
}

// NodeShardStatus is the native NodeShardStatus model.
type NodeShardStatus struct {
	Name                   string                   `json:"name"`
	Class                  string                   `json:"class"`
	ObjectCount            int64                    `json:"objectCount"`
	VectorIndexingStatus   string                   `json:"vectorIndexingStatus"`
	Compressed             bool                     `json:"compressed"`
	VectorQueueLength      int64                    `json:"vectorQueueLength"`
	Loaded                 bool                     `json:"loaded"`
	AsyncReplicationStatus []AsyncReplicationStatus `json:"asyncReplicationStatus,omitempty"`
	NumberOfReplicas       *int64                   `json:"numberOfReplicas,omitempty"`
	ReplicationFactor      *int64                   `json:"replicationFactor,omitempty"`
}

// Statistics is the native Statistics model.
type Statistics struct {
	Name                    string          `json:"name,omitempty"`
	Status                  string          `json:"status,omitempty"`
	Bootstrapped            bool            `json:"bootstrapped,omitempty"`
	DBLoaded                bool            `json:"dbLoaded,omitempty"`
	InitialLastAppliedIndex uint64          `json:"initialLastAppliedIndex,omitempty"`
	LastAppliedIndex        float64         `json:"lastAppliedIndex,omitempty"`
	IsVoter                 bool            `json:"isVoter,omitempty"`
	LeaderID                json.RawMessage `json:"leaderId,omitempty"`
	LeaderAddress           json.RawMessage `json:"leaderAddress,omitempty"`
	Open                    bool            `json:"open,omitempty"`
	Ready                   bool            `json:"ready,omitempty"`
	Candidates              json.RawMessage `json:"candidates,omitempty"`
	Raft                    RaftStatistics  `json:"raft,omitempty"`
}

// ReplicationStatus is the native ReplicationStatus model.
type ReplicationStatus struct {
	State             ReplicaState                                    `json:"state,omitempty"`
	WhenStartedUnixMs int64                                           `json:"whenStartedUnixMs,omitempty"`
	Errors            []ReplicationReplicateDetailsReplicaStatusError `json:"errors,omitempty"`
}

// ShardingState is the native ShardingState model.
type ShardingState struct {
	Collection string          `json:"collection,omitempty"`
	Shards     []ShardReplicas `json:"shards,omitempty"`
}

// Permission is the native Permission model.
type Permission struct {
	Backups     *PermissionBackups         `json:"backups,omitempty"`
	Data        *PermissionData            `json:"data,omitempty"`
	Nodes       *PermissionNodes           `json:"nodes,omitempty"`
	Users       *PermissionUsers           `json:"users,omitempty"`
	Groups      *PermissionGroups          `json:"groups,omitempty"`
	Tenants     *PermissionTenants         `json:"tenants,omitempty"`
	Roles       *PermissionRoles           `json:"roles,omitempty"`
	Collections *PermissionCollections     `json:"collections,omitempty"`
	Replicate   *PermissionReplicate       `json:"replicate,omitempty"`
	Aliases     *PermissionAliases         `json:"aliases,omitempty"`
	Namespaces  *PermissionNamespaces      `json:"namespaces,omitempty"`
	Action      string                     `json:"action"`
	Extra       map[string]json.RawMessage `json:"-"`
}

// BackupConfig is the native BackupConfig model.
// Endpoint mirrors OpenAPI, but the S3 backend uses its startup endpoint; only
// Bucket and Path provide effective per-operation location overrides.
type BackupConfig struct {
	Endpoint         string `json:"Endpoint,omitempty"`
	Bucket           string `json:"Bucket,omitempty"`
	Path             string `json:"Path,omitempty"`
	CPUPercentage    int64  `json:"CPUPercentage,omitempty"`
	CompressionLevel string `json:"CompressionLevel,omitempty"`
}

// RestoreConfig is the native RestoreConfig model.
// Endpoint mirrors OpenAPI, but does not change the S3 backend's startup endpoint.
type RestoreConfig struct {
	Endpoint      string         `json:"Endpoint,omitempty"`
	Bucket        string         `json:"Bucket,omitempty"`
	Path          string         `json:"Path,omitempty"`
	CPUPercentage int64          `json:"CPUPercentage,omitempty"`
	RolesOptions  RestoreOptions `json:"rolesOptions,omitempty"`
	UsersOptions  RestoreOptions `json:"usersOptions,omitempty"`
}

// AsyncReplicationStatus is the native AsyncReplicationStatus model.
type AsyncReplicationStatus struct {
	ObjectsPropagated       uint64 `json:"objectsPropagated,omitempty"`
	StartDiffTimeUnixMillis int64  `json:"startDiffTimeUnixMillis,omitempty"`
	TargetNode              string `json:"targetNode,omitempty"`
}

// RaftStatistics is the native RaftStatistics model.
type RaftStatistics struct {
	AppliedIndex             string          `json:"appliedIndex,omitempty"`
	CommitIndex              string          `json:"commitIndex,omitempty"`
	FsmPending               string          `json:"fsmPending,omitempty"`
	LastContact              string          `json:"lastContact,omitempty"`
	LastLogIndex             string          `json:"lastLogIndex,omitempty"`
	LastLogTerm              string          `json:"lastLogTerm,omitempty"`
	LastSnapshotIndex        string          `json:"lastSnapshotIndex,omitempty"`
	LastSnapshotTerm         string          `json:"lastSnapshotTerm,omitempty"`
	LatestConfiguration      json.RawMessage `json:"latestConfiguration,omitempty"`
	LatestConfigurationIndex string          `json:"latestConfigurationIndex,omitempty"`
	NumPeers                 string          `json:"numPeers,omitempty"`
	ProtocolVersion          string          `json:"protocolVersion,omitempty"`
	ProtocolVersionMax       string          `json:"protocolVersionMax,omitempty"`
	ProtocolVersionMin       string          `json:"protocolVersionMin,omitempty"`
	SnapshotVersionMax       string          `json:"snapshotVersionMax,omitempty"`
	SnapshotVersionMin       string          `json:"snapshotVersionMin,omitempty"`
	State                    string          `json:"state,omitempty"`
	Term                     string          `json:"term,omitempty"`
}

// ReplicationReplicateDetailsReplicaStatusError is the native ReplicationReplicateDetailsReplicaStatusError model.
type ReplicationReplicateDetailsReplicaStatusError struct {
	WhenErroredUnixMs int64  `json:"whenErroredUnixMs,omitempty"`
	Message           string `json:"message,omitempty"`
}

// ShardReplicas is the native ShardReplicas model.
type ShardReplicas struct {
	Shard    string   `json:"shard,omitempty"`
	Replicas []string `json:"replicas,omitempty"`
}

// PermissionBackups is the native PermissionBackups model.
type PermissionBackups struct {
	Collection string                     `json:"collection,omitempty"`
	Extra      map[string]json.RawMessage `json:"-"`
}

// PermissionData is the native PermissionData model.
type PermissionData struct {
	Collection string                     `json:"collection,omitempty"`
	Tenant     string                     `json:"tenant,omitempty"`
	Object     string                     `json:"object,omitempty"`
	Extra      map[string]json.RawMessage `json:"-"`
}

// PermissionNodes is the native PermissionNodes model.
type PermissionNodes struct {
	Verbosity  string                     `json:"verbosity,omitempty"`
	Collection string                     `json:"collection,omitempty"`
	Extra      map[string]json.RawMessage `json:"-"`
}

// PermissionUsers is the native PermissionUsers model.
type PermissionUsers struct {
	Users string                     `json:"users,omitempty"`
	Extra map[string]json.RawMessage `json:"-"`
}

// PermissionGroups is the native PermissionGroups model.
type PermissionGroups struct {
	Group     string                     `json:"group,omitempty"`
	GroupType GroupType                  `json:"groupType,omitempty"`
	Extra     map[string]json.RawMessage `json:"-"`
}

// PermissionTenants is the native PermissionTenants model.
type PermissionTenants struct {
	Collection string                     `json:"collection,omitempty"`
	Tenant     string                     `json:"tenant,omitempty"`
	Extra      map[string]json.RawMessage `json:"-"`
}

// PermissionRoles is the native PermissionRoles model.
type PermissionRoles struct {
	Role  string                     `json:"role,omitempty"`
	Scope string                     `json:"scope,omitempty"`
	Extra map[string]json.RawMessage `json:"-"`
}

// PermissionCollections is the native PermissionCollections model.
type PermissionCollections struct {
	Collection string                     `json:"collection,omitempty"`
	Extra      map[string]json.RawMessage `json:"-"`
}

// PermissionReplicate is the native PermissionReplicate model.
type PermissionReplicate struct {
	Collection string                     `json:"collection,omitempty"`
	Shard      string                     `json:"shard,omitempty"`
	Extra      map[string]json.RawMessage `json:"-"`
}

// PermissionAliases is the native PermissionAliases model.
type PermissionAliases struct {
	Collection string                     `json:"collection,omitempty"`
	Alias      string                     `json:"alias,omitempty"`
	Extra      map[string]json.RawMessage `json:"-"`
}

// PermissionNamespaces is the native PermissionNamespaces model.
type PermissionNamespaces struct {
	Namespace string                     `json:"namespace,omitempty"`
	Extra     map[string]json.RawMessage `json:"-"`
}

// GroupType is the native GroupType model.
type GroupType string

// BackupState is the native backup or restore phase.
type BackupState string

const (
	BackupStarted      BackupState = "STARTED"
	BackupTransferring BackupState = "TRANSFERRING"
	BackupTransferred  BackupState = "TRANSFERRED"
	BackupFinalizing   BackupState = "FINALIZING"
	BackupSuccess      BackupState = "SUCCESS"
	BackupFailed       BackupState = "FAILED"
	BackupCancelling   BackupState = "CANCELLING"
	BackupCanceled     BackupState = "CANCELED"
)

// ReplicaType selects whether the source replica remains after transfer.
type ReplicaType string

const (
	ReplicaMove ReplicaType = "MOVE"
	ReplicaCopy ReplicaType = "COPY"
)

// ReplicaState is the native replica movement phase.
type ReplicaState string

const (
	ReplicaRegistered  ReplicaState = "REGISTERED"
	ReplicaHydrating   ReplicaState = "HYDRATING"
	ReplicaFinalizing  ReplicaState = "FINALIZING"
	ReplicaIntegrating ReplicaState = "INTEGRATING"
	ReplicaDehydrating ReplicaState = "DEHYDRATING"
	ReplicaReady       ReplicaState = "READY"
	ReplicaCancelled   ReplicaState = "CANCELLED"
)

// RestoreOptions controls identity restoration.
type RestoreOptions string

const (
	RestoreAll  RestoreOptions = "all"
	RestoreNone RestoreOptions = "noRestore"
)

func (v *Permission) UnmarshalJSON(data []byte) error {
	type plain Permission
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "backups")
	delete(extra, "data")
	delete(extra, "nodes")
	delete(extra, "users")
	delete(extra, "groups")
	delete(extra, "tenants")
	delete(extra, "roles")
	delete(extra, "collections")
	delete(extra, "replicate")
	delete(extra, "aliases")
	delete(extra, "namespaces")
	delete(extra, "action")
	p.Extra = extra
	*v = Permission(p)
	return nil
}
func (v Permission) MarshalJSON() ([]byte, error) {
	type plain Permission
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "backups", "data", "nodes", "users", "groups", "tenants", "roles", "collections", "replicate", "aliases", "namespaces", "action":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionBackups) UnmarshalJSON(data []byte) error {
	type plain PermissionBackups
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "collection")
	p.Extra = extra
	*v = PermissionBackups(p)
	return nil
}
func (v PermissionBackups) MarshalJSON() ([]byte, error) {
	type plain PermissionBackups
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "collection":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionData) UnmarshalJSON(data []byte) error {
	type plain PermissionData
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "collection")
	delete(extra, "tenant")
	delete(extra, "object")
	p.Extra = extra
	*v = PermissionData(p)
	return nil
}
func (v PermissionData) MarshalJSON() ([]byte, error) {
	type plain PermissionData
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "collection", "tenant", "object":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionNodes) UnmarshalJSON(data []byte) error {
	type plain PermissionNodes
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "verbosity")
	delete(extra, "collection")
	p.Extra = extra
	*v = PermissionNodes(p)
	return nil
}
func (v PermissionNodes) MarshalJSON() ([]byte, error) {
	type plain PermissionNodes
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "verbosity", "collection":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionUsers) UnmarshalJSON(data []byte) error {
	type plain PermissionUsers
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "users")
	p.Extra = extra
	*v = PermissionUsers(p)
	return nil
}
func (v PermissionUsers) MarshalJSON() ([]byte, error) {
	type plain PermissionUsers
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "users":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionGroups) UnmarshalJSON(data []byte) error {
	type plain PermissionGroups
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "group")
	delete(extra, "groupType")
	p.Extra = extra
	*v = PermissionGroups(p)
	return nil
}
func (v PermissionGroups) MarshalJSON() ([]byte, error) {
	type plain PermissionGroups
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "group", "groupType":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionTenants) UnmarshalJSON(data []byte) error {
	type plain PermissionTenants
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "collection")
	delete(extra, "tenant")
	p.Extra = extra
	*v = PermissionTenants(p)
	return nil
}
func (v PermissionTenants) MarshalJSON() ([]byte, error) {
	type plain PermissionTenants
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "collection", "tenant":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionRoles) UnmarshalJSON(data []byte) error {
	type plain PermissionRoles
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "role")
	delete(extra, "scope")
	p.Extra = extra
	*v = PermissionRoles(p)
	return nil
}
func (v PermissionRoles) MarshalJSON() ([]byte, error) {
	type plain PermissionRoles
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "role", "scope":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionCollections) UnmarshalJSON(data []byte) error {
	type plain PermissionCollections
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "collection")
	p.Extra = extra
	*v = PermissionCollections(p)
	return nil
}
func (v PermissionCollections) MarshalJSON() ([]byte, error) {
	type plain PermissionCollections
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "collection":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionReplicate) UnmarshalJSON(data []byte) error {
	type plain PermissionReplicate
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "collection")
	delete(extra, "shard")
	p.Extra = extra
	*v = PermissionReplicate(p)
	return nil
}
func (v PermissionReplicate) MarshalJSON() ([]byte, error) {
	type plain PermissionReplicate
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "collection", "shard":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionAliases) UnmarshalJSON(data []byte) error {
	type plain PermissionAliases
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "collection")
	delete(extra, "alias")
	p.Extra = extra
	*v = PermissionAliases(p)
	return nil
}
func (v PermissionAliases) MarshalJSON() ([]byte, error) {
	type plain PermissionAliases
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "collection", "alias":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}

func (v *PermissionNamespaces) UnmarshalJSON(data []byte) error {
	type plain PermissionNamespaces
	var p plain
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	var extra map[string]json.RawMessage
	if err := json.Unmarshal(data, &extra); err != nil {
		return err
	}
	delete(extra, "namespace")
	p.Extra = extra
	*v = PermissionNamespaces(p)
	return nil
}
func (v PermissionNamespaces) MarshalJSON() ([]byte, error) {
	type plain PermissionNamespaces
	data, err := json.Marshal(plain(v))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	for k, value := range v.Extra {
		switch k {
		case "namespace":
			continue
		}
		fields[k] = value
	}
	return json.Marshal(fields)
}
