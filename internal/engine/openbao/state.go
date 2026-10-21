// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/statejson"
)

// internalState contains observations and receipts, never native credentials.
type internalState struct {
	UID                       string                    `json:"uid"`
	Phase                     string                    `json:"phase"`
	Members                   []*member                 `json:"members"`
	NextIndex                 int                       `json:"nextIndex"`
	ConfigVersion             int64                     `json:"configVersion"`
	Pending                   *pending                  `json:"pending,omitempty"`
	Retry                     *pending                  `json:"retry,omitempty"`
	PlatformRetries           map[string]*platformRetry `json:"platformRetries,omitempty"`
	InitOwner                 string                    `json:"initOwner,omitempty"`
	Initialized               bool                      `json:"initialized"`
	InitializationOutcomeLost string                    `json:"initializationOutcomeLost,omitempty"`
	QuorumBlocked             string                    `json:"quorumBlocked,omitempty"`
	Suspended                 bool                      `json:"suspended,omitempty"`
	CleanupSecrets            bool                      `json:"cleanupSecrets,omitempty"`
	// Unsealing fences the next follower until autopilot observes this voter healthy.
	Unsealing            string `json:"unsealing,omitempty"`
	BindingID            string `json:"bindingId,omitempty"`
	BindingReleased      bool   `json:"bindingReleased,omitempty"`
	CredentialGeneration int64  `json:"credentialGeneration,omitempty"`
	Replacing            bool   `json:"replacing,omitempty"`
	CloneRestoreStep     string `json:"cloneRestoreStep,omitempty"`
}
type member struct {
	Name                 string                     `json:"name"`
	Volume               string                     `json:"volume"`
	Index                int                        `json:"index"`
	Stage                string                     `json:"stage"`
	ServiceID            string                     `json:"serviceId,omitempty"`
	VolumeID             string                     `json:"volumeId,omitempty"`
	VolumeOrphaned       bool                       `json:"volumeOrphaned,omitempty"`
	RuntimeRecovery      engine.RuntimeRecovery     `json:"runtimeRecovery,omitempty"`
	UnplacedRetirement   *engine.UnplacedRetirement `json:"unplacedRetirement,omitempty"`
	ImportPlan           *engine.ImportPlan         `json:"importPlan,omitempty"`
	ResumeDeploy         *pending                   `json:"resumeDeploy,omitempty"`
	Resume               bool                       `json:"resume,omitempty"`
	Version              string                     `json:"version"`
	DesiredVersion       string                     `json:"desiredVersion"`
	ConfigVersion        int64                      `json:"configVersion"`
	AppliedConfigVersion int64                      `json:"appliedConfigVersion,omitempty"`
	ProcessGeneration    int64                      `json:"processGeneration"`
	RestartFingerprint   string                     `json:"restartFingerprint,omitempty"`
	Change               *memberChange              `json:"change,omitempty"`
	EngineOptions        json.RawMessage            `json:"engineOptions,omitempty"`
	EngineConfig         string                     `json:"engineConfig,omitempty"`
	TLSMode              string                     `json:"tlsMode,omitempty"`
	Resources            domain.Resources           `json:"resources"`
	Disk                 domain.Resources           `json:"disk"`
	ConfigResources      domain.Resources           `json:"configResources"`
	ConfigDisk           domain.Resources           `json:"configDisk"`
	PublicPort           int                        `json:"publicPort,omitempty"`
	PublicHash           string                     `json:"publicHash,omitempty"`
	Removing             bool                       `json:"removing,omitempty"`
	RemoveStep           string                     `json:"removeStep,omitempty"`
	CleanupStopReason    string                     `json:"cleanupStopReason,omitempty"`
	Held                 bool                       `json:"held,omitempty"`
	Initialized          bool                       `json:"initialized"`
	Observed             bool                       `json:"observed"`
	Sealed               bool                       `json:"sealed"`
	Active               bool                       `json:"active"`
	Standby              bool                       `json:"standby"`
	Voter                bool                       `json:"voter"`
	Healthy              bool                       `json:"healthy"`
	LeaderAddress        string                     `json:"leaderAddress,omitempty"`
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
			return nil, fmt.Errorf("decode OpenBao state: %w", err)
		}
	}
	if s.ConfigVersion == 0 {
		s.ConfigVersion = 1
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
		if !m.Removing && m.Stage == "ready" {
			out = append(out, m)
		}
	}
	return out
}
func (s *internalState) add(cluster string) *member {
	name := fmt.Sprintf("%sm%d", cluster, s.NextIndex)
	m := &member{Name: name, Volume: name + "v", Index: s.NextIndex, Stage: "planned", ProcessGeneration: 1, Sealed: true}
	s.NextIndex++
	s.Members = append(s.Members, m)
	return m
}
func removeRecord(s *internalState, name string) {
	for i, m := range s.Members {
		if m.Name == name {
			s.Members = append(s.Members[:i], s.Members[i+1:]...)
			if s.InitOwner == name && len(s.Members) > 0 {
				s.InitOwner = ordered(s)[0].Name
			}
			return
		}
	}
}
