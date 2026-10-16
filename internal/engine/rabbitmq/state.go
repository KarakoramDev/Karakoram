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
	Rebalance       bool                      `json:"rebalance"`
	FeatureFlags    bool                      `json:"featureFlags"`
	ReplaceActive   bool                      `json:"replaceActive"`
	CleanupBackups  bool                      `json:"cleanupBackups"`
	CleanupSecrets  bool                      `json:"cleanupSecrets"`
	Suspended       bool                      `json:"suspended"`
	RotationSecrets []string                  `json:"rotationSecrets,omitempty"`
	InterNodeTLS    bool                      `json:"interNodeTLS"`
	DistributionSet bool                      `json:"distributionSet"`
}
type member struct {
	Name   string `json:"name"`
	Volume string `json:"volume"`
	// ReplacesVolume preserves the replacement placement hint across reconciles.
	ReplacesVolume          string                  `json:"replacesVolume,omitempty"`
	Index                   int                     `json:"index"`
	Stage                   string                  `json:"stage"`
	ServiceID               string                  `json:"serviceId,omitempty"`
	VolumeID                string                  `json:"volumeId,omitempty"`
	VolumeOrphaned          bool                    `json:"volumeOrphaned,omitempty"`
	Resume                  bool                    `json:"resume,omitempty"`
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
	GrowIndex               int                     `json:"growIndex"`
	Lost                    bool                    `json:"lost,omitempty"`
	RemovalBlocked          string                  `json:"removalBlocked,omitempty"`
	ChangeAction            string                  `json:"changeAction,omitempty"`
	QuorumBlockers          []rmqspec.QuorumBlocker `json:"quorumBlockers,omitempty"`
	QuorumBlockerCount      int                     `json:"quorumBlockerCount,omitempty"`
	ChangeStep              string                  `json:"changeStep,omitempty"`
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
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	owned := map[string]json.RawMessage{}
	if err = json.Unmarshal(b, &owned); err != nil {
		return err
	}
	for k, v := range owned {
		raw[k] = v
	}
	for _, k := range []string{"pending", "retry", "platformRetries"} {
		if _, ok := owned[k]; !ok {
			delete(raw, k)
		}
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
