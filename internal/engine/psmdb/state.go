// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/statejson"
)

// internalState is persisted after every reconciliation. Stages progress only after observed success.
// Unknown JSON fields are retained so PBM's durable state can share Cluster.Internal.
type internalState struct {
	UID                      string                    `json:"uid"`
	PBMPrefix                string                    `json:"pbmPrefix"`
	Phase                    string                    `json:"phase"`
	ConfigVersion            int64                     `json:"configVersion"`
	Members                  []*member                 `json:"members"`
	Sets                     map[string]*setState      `json:"sets"`
	NextIndex                map[string]int            `json:"nextIndex"`
	Pending                  *pending                  `json:"pending,omitempty"`
	Retry                    *pending                  `json:"retry,omitempty"`
	PlatformRetries          map[string]*platformRetry `json:"platformRetries,omitempty"`
	ConfigHash               string                    `json:"configHash"`
	PBMHash                  string                    `json:"pbmHash"`
	PBMStorage               string                    `json:"pbmStorage"`
	UsedStorages             []string                  `json:"usedStorages,omitempty"`
	CloneQueued              bool                      `json:"cloneQueued"`
	InitialRestore           *initialRestore           `json:"initialRestore,omitempty"`
	BalancerStopped          bool                      `json:"balancerStopped"`
	BalancerWasEnabled       bool                      `json:"balancerWasEnabled"`
	BalancerObserved         bool                      `json:"balancerObserved,omitempty"`
	CleanupBackups           bool                      `json:"cleanupBackups"`
	CleanupSecrets           bool                      `json:"cleanupSecrets"`
	CleanedStorages          []string                  `json:"cleanedStorages,omitempty"`
	FinalizedStorages        []string                  `json:"finalizedStorages,omitempty"`
	BackupCleanupStarted     *time.Time                `json:"backupCleanupStarted,omitempty"`
	RetainedStorages         []string                  `json:"retainedStorages,omitempty"`
	RotationSecrets          []string                  `json:"rotationSecrets,omitempty"`
	LastRotatedAt            map[string]*time.Time     `json:"lastRotatedAt,omitempty"`
	ReplaceActive            bool                      `json:"replaceActive"`
	Suspended                bool                      `json:"suspended"`
	TLSMode                  string                    `json:"tlsMode"`
	TargetTLS                string                    `json:"targetTLS,omitempty"`
	ManagedUsersError        string                    `json:"managedUsersError,omitempty"`
	ManagedUsersRetryable    bool                      `json:"managedUsersRetryable"`
	RestoreConsistency       *restoreConsistency       `json:"restoreConsistency,omitempty"`
	RestoreWorkerUnconfirmed bool                      `json:"restoreWorkerUnconfirmed,omitempty"`
}

// restoreConsistency survives process recovery and unrelated operations.
// Availability cannot prove that a failed native restore left data/auth intact.
type restoreConsistency struct {
	NativeName string `json:"nativeName"`
	Failure    string `json:"failure"`
}
type member struct {
	Name               string                     `json:"name"`
	NativeReadiness    *bool                      `json:"nativeReadiness,omitempty"`
	RuntimeRecovery    engine.RuntimeRecovery     `json:"runtimeRecovery,omitempty"`
	UnplacedRetirement *engine.UnplacedRetirement `json:"unplacedRetirement,omitempty"`
	ImportPlan         *engine.ImportPlan         `json:"importPlan,omitempty"`
	Volume             string                     `json:"volume,omitempty"`
	// ReplacesVolume preserves the replacement placement hint across reconciles.
	ReplacesVolume   string           `json:"replacesVolume,omitempty"`
	ServiceID        string           `json:"serviceId,omitempty"`
	VolumeID         string           `json:"volumeId,omitempty"`
	VolumeOrphaned   bool             `json:"volumeOrphaned,omitempty"`
	NeedsAllocation  bool             `json:"needsAllocation,omitempty"`
	Set              string           `json:"set"`
	Role             string           `json:"role"`
	Index            int              `json:"index"`
	Shard            int              `json:"shard"`
	Stage            string           `json:"stage"`
	Version          string           `json:"version"`
	DesiredVersion   string           `json:"desiredVersion"`
	ConfigVersion    int64            `json:"configVersion"`
	TLSMode          string           `json:"tlsMode"`
	EngineConfig     string           `json:"engineConfig"`
	CacheGB          float64          `json:"cacheGB"`
	ConfigDB         string           `json:"configDB,omitempty"`
	ReplacingReady   bool             `json:"replacingReady"`
	Resources        domain.Resources `json:"resources"`
	CPUModeChangedAt time.Time        `json:"cpuModeChangedAt,omitempty"`
	Disk             domain.Resources `json:"disk"`
	Containers       int              `json:"containers"`
	Removing         bool             `json:"removing"`
	PublicPort       int              `json:"publicPort,omitempty"`
	PublicHash       string           `json:"publicHash"`
}
type setState struct {
	Stage string `json:"stage"`
	Added bool   `json:"added"`
}
type pending struct {
	Op         infra.Op         `json:"op"`
	Action     string           `json:"action"`
	Member     string           `json:"member"`
	Version    string           `json:"version,omitempty"`
	Resources  domain.Resources `json:"resources"`
	Containers int              `json:"containers,omitempty"`
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

func load(c *domain.Cluster) (*internalState, error) {
	s := &internalState{}
	if len(c.Internal) > 0 {
		if e := json.Unmarshal(c.Internal, s); e != nil {
			return nil, fmt.Errorf("decode PSMDB Internal: %w", e)
		}
	}
	if s.Sets == nil {
		s.Sets = map[string]*setState{}
	}
	if s.NextIndex == nil {
		s.NextIndex = map[string]int{}
	}
	if s.ConfigVersion == 0 {
		s.ConfigVersion = 1
	}
	if s.TLSMode == "" {
		s.TLSMode = c.Spec.TLS.Mode
	}
	// Active is the pre-journal durable receipt that native initialization
	// completed. Migrate before ownership checks can observe physical loss.
	for _, m := range s.Members {
		if m.Stage == "active" && m.VolumeID != "" {
			m.RuntimeRecovery.Initialized = true
		}
	}
	return s, nil
}

// RelayTargets includes observed and planned database endpoints, including replacements.
func RelayTargets(c *domain.Cluster) []string {
	if c == nil {
		return nil
	}
	targets := make(map[string]struct{})
	add := func(host string) {
		if host != "" {
			targets[host+":27017"] = struct{}{}
			targets[host+":9443"] = struct{}{}
		}
	}
	var state internalState
	_ = json.Unmarshal(c.Internal, &state)
	for _, m := range state.Members {
		if m != nil {
			add(m.Name)
		}
	}
	for _, m := range c.Status.Members {
		add(m.Name)
	}
	result := make([]string, 0, len(targets))
	for target := range targets {
		result = append(result, target)
	}
	sort.Strings(result)
	return result
}
func save(c *domain.Cluster, s *internalState) error {
	raw := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if e := json.Unmarshal(c.Internal, &raw); e != nil {
			return e
		}
	}
	raw, e := statejson.ReplaceOwned(raw, s)
	if e != nil {
		return e
	}
	c.Internal, e = json.Marshal(raw)
	return e
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
func (s *internalState) setMembers(set string) []*member {
	out := []*member{}
	for _, m := range s.Members {
		if m.Set == set && !m.Removing {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}
func memberName(c, set, role string, shard, index int) string {
	if role == agentapi.RoleRouter {
		return c + "r"
	}
	if role == agentapi.RoleConfigServer {
		return fmt.Sprintf("%sc%d", c, index)
	}
	if shard >= 0 {
		return fmt.Sprintf("%ss%dm%d", c, shard, index)
	}
	return fmt.Sprintf("%sm%d", c, index)
}
func (s *internalState) add(c, set, role string, shard int) *member {
	idx := s.NextIndex[set]
	s.NextIndex[set] = idx + 1
	n := memberName(c, set, role, shard, idx)
	m := &member{Name: n, Set: set, Role: role, Index: idx, Shard: shard, Stage: "planned"}
	if role != agentapi.RoleRouter {
		m.Volume = n + "v"
	}
	s.Members = append(s.Members, m)
	if role != agentapi.RoleRouter && s.Sets[set] == nil {
		s.Sets[set] = &setState{Stage: "planned"}
	}
	return m
}
func component(spec domain.ClusterSpec, m *member) domain.ComponentSpec {
	if m.Role == agentapi.RoleConfigServer {
		return spec.Components.ConfigServer
	}
	if m.Role == agentapi.RoleRouter {
		return spec.Components.Router
	}
	return spec.Components.Engine
}
func plan(c *domain.Cluster, s *internalState) bool {
	changed := false
	ensure := func(set, role string, shard, n int) {
		members := s.setMembers(set)
		for len(members) < n {
			members = append(members, s.add(c.Name, set, role, shard))
			changed = true
		}
		if len(members) > n {
			sort.Slice(members, func(i, j int) bool { return members[i].Index > members[j].Index })
			for _, m := range members[:len(members)-n] {
				m.Removing = true
				changed = true
			}
		}
	}
	if c.Spec.Topology.Type == "replicaSet" {
		ensure("rs0", agentapi.RoleReplicaSet, -1, c.Spec.Components.Engine.Replicas)
	} else {
		ensure("cfg", agentapi.RoleConfigServer, -1, c.Spec.Components.ConfigServer.Replicas)
		for i := range c.Spec.Topology.Shards {
			ensure(fmt.Sprintf("rs%d", i), agentapi.RoleReplicaSet, i, c.Spec.Components.Engine.Replicas)
		}
		if s.find(c.Name+"r") == nil {
			s.add(c.Name, "router", agentapi.RoleRouter, -1)
			changed = true
		}
	}
	return changed
}
func ordered(s *internalState) []*member {
	out := append([]*member(nil), s.Members...)
	sort.Slice(out, func(i, j int) bool {
		rank := func(m *member) int {
			if m.Role == agentapi.RoleConfigServer {
				return 0
			}
			if m.Role == agentapi.RoleRouter {
				return 2
			}
			return 1
		}
		a, b := out[i], out[j]
		if rank(a) != rank(b) {
			return rank(a) < rank(b)
		}
		if a.Shard != b.Shard {
			return a.Shard < b.Shard
		}
		return a.Index < b.Index
	})
	return out
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
	out := make([]*member, 0)
	for _, m := range s.Members {
		if m.Removing {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Set != out[j].Set {
			return out[i].Set < out[j].Set
		}
		return out[i].Index > out[j].Index
	})
	return out
}
