// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// internalKey is the single Cluster.Internal root key owned by this provider.
// Other root keys (if any) are preserved by save.
const internalKey = "elasticsearch"

// Member kinds.
const (
	kindNode     = "node"     // combined (cluster topology) or data node (dedicatedMasters)
	kindMaster   = "master"   // dedicated master-eligible node
	kindKibana   = "kibana"   // stateless Kibana service (N containers)
	kindLogstash = "logstash" // stateless Logstash service (N containers)
	kindAPM      = "apm"      // stateless APM Server service (N containers)
)

// Member stages.
const (
	stagePlanned  = "planned"  // identity allocated, nothing imported
	stageImported = "imported" // runtime (+volume) imported and ownership verified
	stageDeployed = "deployed" // pipeline deployed; process readiness passed
	stageJoined   = "joined"   // observed as a cluster node (ES) / ready (components)
	stageDraining = "draining" // allocation-excluded, waiting for zero shards
	stageExcluded = "excluded" // voting-excluded (masters) and empty; ready for deletion
	stageDeleting = "deleting" // runtime/volume deletion in progress
)

// state is the secret-free provider-owned durable state stored in
// Cluster.Internal under internalKey. Secrets live in the SecretStore.
type state struct {
	UID   string `json:"uid"`
	Phase string `json:"phase"`
	// ConfigVersion increments whenever any member configuration changes.
	ConfigVersion int64     `json:"configVersion"`
	Members       []*member `json:"members"`
	// NextIndex holds the next never-used index per kind (node, master).
	NextIndex map[string]int `json:"nextIndex"`
	// InitialMasterNodes is chosen once for a brand-new cluster and kept until
	// ClusterUUID is observed; it is never re-populated afterwards.
	InitialMasterNodes []string `json:"initialMasterNodes,omitempty"`
	// ClusterUUID is persisted once formation is observed. Its presence means
	// the cluster is bootstrapped; cluster.initial_master_nodes is never set again.
	ClusterUUID string `json:"clusterUUID,omitempty"`
	// SecurityHash is the digest of the last successfully ensured native
	// security objects (passwords, roles, writer users, Kibana token).
	SecurityHash string `json:"securityHash,omitempty"`
	// SecureSettingsHash tracks the last acknowledged keystore credential set.
	SecureSettingsHash   string `json:"secureSettingsHash,omitempty"`
	SecureSettingsReload bool   `json:"secureSettingsReload,omitempty"`
	// Repositories maps storage name to the registered repository state.
	Repositories map[string]*repository `json:"repositories,omitempty"`
	// Storages is the ordered set of backup storages whose S3 clients are
	// rendered into node settings (static settings: changes are rolled).
	Storages []string `json:"storages,omitempty"`
	// Rolling records one in-flight rolling restart/upgrade/config pass.
	Rolling *rolling `json:"rolling,omitempty"`
	// Pending is the single in-flight platform process.
	Pending *pending `json:"pending,omitempty"`
	// Retry preserves the exact failed action until its backoff expires.
	Retry *pending `json:"retry,omitempty"`
	// DrainBlocked explains why a scale-down cannot safely move shards.
	DrainBlocked string `json:"drainBlocked,omitempty"`
	// PlatformRetries tracks durable backoff per action key.
	PlatformRetries map[string]*platformRetry `json:"platformRetries,omitempty"`
	// InitialRestore tracks spec.dataSource: empty, initial-restore, done, or
	// abandoned when deletion interrupts it; abandoned is never retried implicitly.
	InitialRestore string `json:"initialRestore,omitempty"`
	// RestoreActive suspends component restarts and security re-ensure while a
	// restore runs; retained-volume resume abandons and clears retired intent.
	RestoreActive bool `json:"restoreActive,omitempty"`
	// Suspended is set when a cluster was deleted with retained volumes.
	Suspended bool `json:"suspended,omitempty"`
	// Resume reconciles database settings owned by retired operations after
	// retained members have been reprovisioned and joined.
	ResumeAllocationReset bool     `json:"resumeAllocationReset,omitempty"`
	ResumeExcludeNames    []string `json:"resumeExcludeNames,omitempty"`
	ResumeVotingReset     bool     `json:"resumeVotingReset,omitempty"`
	// ResumeRestoreReset clears the restore-owned destructive setting after
	// retained Elasticsearch nodes rejoin; the retired restore is not resumed.
	ResumeRestoreReset bool `json:"resumeRestoreReset,omitempty"`
	// Cleanup progress.
	CleanupBackups bool `json:"cleanupBackups,omitempty"`
	CleanupSecrets bool `json:"cleanupSecrets,omitempty"`
}

// member is one Zerops service: an Elasticsearch node (with its own volume)
// or a stateless component service.
type member struct {
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	Index     int    `json:"index"`
	Volume    string `json:"volume,omitempty"`
	ServiceID string `json:"serviceId,omitempty"`
	VolumeID  string `json:"volumeId,omitempty"`
	// VolumeOrphaned marks a volume Zerops refused to delete (tracked as an
	// orphaned volume record, as for PSMDB).
	VolumeOrphaned bool   `json:"volumeOrphaned,omitempty"`
	Stage          string `json:"stage"`
	// Version is the Elastic Stack version deployed on the service.
	Version string `json:"version"`
	// ConfigVersion is the last configuration version published to the member.
	ConfigVersion int64 `json:"configVersion"`
	// RestartToken is the restart-relevant digest published to the member.
	RestartToken string `json:"restartToken"`
	// ConfigHash excludes imperative restart nonces and dynamically refreshed credentials.
	ConfigHash string `json:"configHash,omitempty"`
	// ConfigurationWarning suppresses repeated warnings for one applied config/error.
	ConfigurationWarning string `json:"configurationWarning,omitempty"`
	// HeapMB is the last published process heap; stateless resizes do not restart.
	HeapMB int `json:"heapMB,omitempty"`
	// Resources/Disk/Containers are the last applied platform values.
	Resources  domain.Resources `json:"resources"`
	Disk       domain.Resources `json:"disk"`
	Containers int              `json:"containers,omitempty"`
	// Removing marks scale-down or replacement removal in progress.
	Removing bool `json:"removing,omitempty"`
	// AllocationExcluded records managed exclusion intent before its ES write.
	AllocationExcluded bool `json:"allocationExcluded,omitempty"`
	// DrainIdleObservations counts consecutive healthy-primary, idle observations with
	// the same remaining shard count; transient relocation gaps are not blocks.
	DrainIdleObservations int `json:"drainIdleObservations,omitempty"`
	DrainShardCount       int `json:"drainShardCount,omitempty"`
	// DrainWarning survives condition recovery to suppress repeat flap warnings.
	DrainWarning string `json:"drainWarning,omitempty"`
	// Replaces names the member this one replaces (replace-member operation).
	Replaces string `json:"replaces,omitempty"`
	// PublicPort/Subdomain record applied public exposure.
	PublicPort   int    `json:"publicPort,omitempty"`
	Subdomain    bool   `json:"subdomain,omitempty"`
	ExposureHash string `json:"exposureHash,omitempty"`
}

// repository records one registered snapshot repository.
type repository struct {
	Name     string `json:"name"`
	Client   string `json:"client"`
	BasePath string `json:"basePath"`
	ReadOnly bool   `json:"readOnly,omitempty"`
	Hash     string `json:"hash"`
}

// rolling is a durable one-member-at-a-time pass over Elasticsearch nodes
// followed by component services.
type rolling struct {
	// Reason: "upgrade", "config", "resources", "restart".
	Reason string `json:"reason"`
	// Target version for upgrades.
	Version string `json:"version,omitempty"`
	// Order is the fixed member order chosen at start.
	Order []string `json:"order"`
	// Done lists members finished.
	Done []string `json:"done"`
	// Current member and its step: "prepare" (allocation primaries + flush),
	// "apply", "rejoin", "recover".
	Current string `json:"current,omitempty"`
	Step    string `json:"step,omitempty"`
	// OperationID links an imperative restart operation, if any.
	OperationID string    `json:"operationId,omitempty"`
	StartedAt   time.Time `json:"startedAt"`
}

// pending is one in-flight platform process.
type pending struct {
	Op         infra.Op         `json:"op"`
	Action     string           `json:"action"`
	Member     string           `json:"member"`
	Version    string           `json:"version,omitempty"`
	Resources  domain.Resources `json:"resources"`
	Containers int              `json:"containers,omitempty"`
}

// platformRetry is durable backoff for a failed platform action.
type platformRetry struct {
	Failures      int       `json:"failures"`
	RetryAfter    time.Time `json:"retryAfter"`
	Message       string    `json:"message"`
	FirstFailedAt time.Time `json:"firstFailedAt,omitempty"`
	LastProcessID string    `json:"lastProcessId,omitempty"`
	// StillMountedFailures counts "volume still mounted" deletion refusals.
	StillMountedFailures int `json:"stillMountedFailures,omitempty"`
}

// load decodes provider state from Cluster.Internal.
func load(c *domain.Cluster) (*state, error) {
	s := &state{}
	if len(c.Internal) > 0 {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return nil, fmt.Errorf("decode Elasticsearch Internal: %w", err)
		}
		if raw, ok := root[internalKey]; ok {
			if err := json.Unmarshal(raw, s); err != nil {
				return nil, fmt.Errorf("decode Elasticsearch Internal: %w", err)
			}
		}
	}
	if s.NextIndex == nil {
		s.NextIndex = map[string]int{}
	}
	if s.Repositories == nil {
		s.Repositories = map[string]*repository{}
	}
	if s.ConfigVersion == 0 {
		s.ConfigVersion = 1
	}
	return s, nil
}

// save merges provider state into Cluster.Internal, preserving other keys.
func save(c *domain.Cluster, s *state) error {
	root := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return err
		}
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	root[internalKey] = b
	c.Internal, err = json.Marshal(root)
	return err
}

func (s *state) find(name string) *member {
	for _, m := range s.Members {
		if m.Name == name {
			return m
		}
	}
	return nil
}

// members returns non-removed members of the given kinds ordered by kind then index.
func (s *state) members(kinds ...string) []*member {
	out := []*member{}
	for _, m := range s.Members {
		for _, k := range kinds {
			if m.Kind == k {
				out = append(out, m)
				break
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Index < out[j].Index
	})
	return out
}

// esNodes returns Elasticsearch node members (dedicated masters and nodes).
func (s *state) esNodes() []*member { return s.members(kindMaster, kindNode) }
