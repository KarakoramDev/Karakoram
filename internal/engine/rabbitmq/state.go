// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/statejson"
)

// internalState checkpoints each observed lifecycle transition. Unknown fields belong to other durable workflows.
type internalState struct {
	UID             string                    `json:"uid"`
	Phase           string                    `json:"phase"`
	Members         []*member                 `json:"members"`
	NextIndex       int                       `json:"nextIndex"`
	ConfigVersion   int64                     `json:"configVersion"`
	Pending         *pending                  `json:"pending,omitempty"`
	Retry           *pending                  `json:"retry,omitempty"`
	PlatformRetries map[string]*platformRetry `json:"platformRetries,omitempty"`
	CloneImported   bool                      `json:"cloneImported,omitempty"`
	Bootstrapped    bool                      `json:"bootstrapped"`
	// CreationSeed is elected before peer deployment; capability gates native start.
	CreationSeed            string    `json:"creationSeed,omitempty"`
	CreationGateChecked     bool      `json:"creationGateChecked,omitempty"`
	SystemUsersReconciledAt time.Time `json:"systemUsersReconciledAt"`
	SystemUsersError        string    `json:"systemUsersError"`
	// Rejections from older snapshots are expected during peer reloads.
	AdminCredentialVersion int64      `json:"adminCredentialVersion,omitempty"`
	Rebalance              bool       `json:"rebalance"`
	FeatureFlags           bool       `json:"featureFlags"`
	ReplaceActive          bool       `json:"replaceActive"`
	CleanupBackups         bool       `json:"cleanupBackups"`
	BackupCleanupStarted   *time.Time `json:"backupCleanupStarted,omitempty"`
	RetainedBackups        []string   `json:"retainedBackups,omitempty"`
	CleanupSecrets         bool       `json:"cleanupSecrets"`
	Suspended              bool       `json:"suspended"`
	RotationSecrets        []string   `json:"rotationSecrets,omitempty"`
	InterNodeTLS           bool       `json:"interNodeTLS"`
	DistributionSet        bool       `json:"distributionSet"`
}
type member struct {
	Name               string                     `json:"name"`
	RuntimeRecovery    engine.RuntimeRecovery     `json:"runtimeRecovery,omitempty"`
	UnplacedRetirement *engine.UnplacedRetirement `json:"unplacedRetirement,omitempty"`
	ImportPlan         *engine.ImportPlan         `json:"importPlan,omitempty"`
	Volume             string                     `json:"volume"`
	// ReplacesVolume preserves the replacement placement hint across reconciles.
	ReplacesVolume string `json:"replacesVolume,omitempty"`
	Index          int    `json:"index"`
	Stage          string `json:"stage"`
	ServiceID      string `json:"serviceId,omitempty"`
	VolumeID       string `json:"volumeId,omitempty"`
	VolumeOrphaned bool   `json:"volumeOrphaned,omitempty"`
	Resume         bool   `json:"resume,omitempty"`
	// ResumeDeploy checkpoints non-rolling provisioning, both fresh and retained.
	// The wire name is retained so already accepted receipts survive upgrades.
	ResumeDeploy     *pending `json:"resumeDeploy,omitempty"`
	InitialFormation bool     `json:"initialFormation,omitempty"`
	// FormationVerified records native bootstrap/seed membership, not merely
	// a mounted volume or an exec'd supervisor child.
	FormationVerified       bool                    `json:"formationVerified,omitempty"`
	FormationChecked        bool                    `json:"formationChecked,omitempty"`
	FormationGate           bool                    `json:"formationGate,omitempty"`
	FormationLegacyRelease  bool                    `json:"formationLegacyRelease,omitempty"`
	Version                 string                  `json:"version"`
	DesiredVersion          string                  `json:"desiredVersion"`
	ConfigVersion           int64                   `json:"configVersion"`
	TLSMode                 string                  `json:"tlsMode"`
	ProvidedCertificateHash string                  `json:"providedCertificateHash,omitempty"`
	EngineConfig            string                  `json:"engineConfig"`
	EngineOptions           json.RawMessage         `json:"engineOptions,omitempty"`
	Resources               domain.Resources        `json:"resources"`
	Disk                    domain.Resources        `json:"disk"`
	ConfigResources         domain.Resources        `json:"configResources"`
	ConfigDisk              domain.Resources        `json:"configDisk"`
	CPUModeChangedAt        time.Time               `json:"cpuModeChangedAt,omitempty"`
	Removing                bool                    `json:"removing"`
	PublicPort              int                     `json:"publicPort,omitempty"`
	PublicHash              string                  `json:"publicHash"`
	ManagementPublic        bool                    `json:"managementPublic"`
	RemoveStep              string                  `json:"removeStep,omitempty"`
	CleanupStopReason       string                  `json:"cleanupStopReason,omitempty"`
	GrowIndex               int                     `json:"growIndex"`
	Lost                    bool                    `json:"lost,omitempty"`
	RemovalBlocked          string                  `json:"removalBlocked,omitempty"`
	ChangeAction            string                  `json:"changeAction,omitempty"`
	QuorumBlockers          []rmqspec.QuorumBlocker `json:"quorumBlockers,omitempty"`
	QuorumBlockerCount      int                     `json:"quorumBlockerCount,omitempty"`
	ChangeStep              string                  `json:"changeStep,omitempty"`
	ChangeDisruptedAt       time.Time               `json:"changeDisruptedAt,omitempty"`
	ChangeStartupMessage    string                  `json:"changeStartupMessage,omitempty"`
	TargetVersion           string                  `json:"targetVersion,omitempty"`
	TargetResources         domain.Resources        `json:"targetResources"`
	TargetTLS               string                  `json:"targetTLS,omitempty"`
	TargetConfig            string                  `json:"targetConfig,omitempty"`
	TargetOptions           json.RawMessage         `json:"targetOptions,omitempty"`
}
type pending struct {
	Op        infra.Op         `json:"op"`
	Action    string           `json:"action"`
	Member    string           `json:"member"`
	Version   string           `json:"version,omitempty"`
	Resources domain.Resources `json:"resources"`
}
type platformRetry struct {
	engine.RetrySchedule
	Failures             int       `json:"failures"`
	RetryAfter           time.Time `json:"retryAfter"`
	Message              string    `json:"message"`
	CPUModeCooldown      bool      `json:"cpuModeCooldown,omitempty"`
	StillMountedFailures int       `json:"stillMountedFailures,omitempty"`
	FirstFailedAt        time.Time `json:"firstFailedAt,omitempty"`
	FirstError           string    `json:"firstError,omitempty"`
	LastAttemptAt        time.Time `json:"lastAttemptAt,omitempty"`
	LastProcessID        string    `json:"lastProcessId,omitempty"`
}

func loadState(c *domain.Cluster) (*internalState, error) {
	s := &internalState{}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, s); err != nil {
			return nil, fmt.Errorf("decode RabbitMQ state: %w", err)
		}
	}
	if s.ConfigVersion == 0 {
		s.ConfigVersion = 1
	}
	if !s.DistributionSet {
		s.InterNodeTLS = c.Spec.TLS.Mode == "requireTLS"
		s.DistributionSet = true
	}
	return s, nil
}
func (s *internalState) save(c *domain.Cluster) error {
	raw := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &raw); err != nil {
			return err
		}
	}
	raw, err := statejson.ReplaceOwned(raw, s)
	if err != nil {
		return err
	}
	c.Internal, err = json.Marshal(raw)
	return err
}
func now(rc *engine.Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now().UTC()
}
func (s *internalState) find(name string) *member {
	for _, m := range s.Members {
		if m.Name == name {
			return m
		}
	}
	return nil
}
func ordered(s *internalState) []*member {
	out := append([]*member(nil), s.Members...)
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}
func (s *internalState) runningMembers() []*member {
	out := []*member{}
	for _, m := range ordered(s) {
		if !m.Removing && (m.Stage == "ready" || m.Stage == "joined") {
			out = append(out, m)
		}
	}
	return out
}
func (s *internalState) add(cluster string) *member {
	n := fmt.Sprintf("%sm%d", cluster, s.NextIndex)
	m := &member{Name: n, Volume: n + "v", Index: s.NextIndex, Stage: "planned"}
	s.NextIndex++
	s.Members = append(s.Members, m)
	return m
}
func removeRecord(s *internalState, name string) {
	for i, m := range s.Members {
		if m.Name == name {
			s.Members = append(s.Members[:i], s.Members[i+1:]...)
			return
		}
	}
}
func removingMembers(s *internalState) []*member {
	out := []*member{}
	for _, m := range ordered(s) {
		if m.Removing {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index > out[j].Index })
	return out
}
