// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/statejson"
	"sort"
	"time"
)

// state contains receipts and non-secret intent only. Backup and access workflows
// add separately owned fields; save preserves fields unknown to this slice.
type state struct {
	UID            string               `json:"uid"`
	Phase          string               `json:"phase"`
	Members        []*member            `json:"members"`
	NextIndex      int                  `json:"nextIndex"`
	ConfigVersion  int64                `json:"configVersion"`
	Voters         []string             `json:"voters"`
	Bootstrapped   bool                 `json:"bootstrapped"`
	Pending        *pending             `json:"pending,omitempty"`
	Rolling        *rolling             `json:"rolling,omitempty"`
	Suspended      bool                 `json:"suspended,omitempty"`
	CleanupSecrets bool                 `json:"cleanupSecrets,omitempty"`
	InitialRestore string               `json:"initialRestore,omitempty"`
	ScaleDown      *scaleDown           `json:"scaleDown,omitempty"`
	Rotation       *rotation            `json:"rotation,omitempty"`
	LastRotated    map[string]time.Time `json:"lastRotated,omitempty"`
}

// member stores runtime ownership, active configuration identity and recovery.
// Secret-bearing active configuration is held under config/<member> in SecretStore.
type member struct {
	Name               string                     `json:"name"`
	Volume             string                     `json:"volume"`
	Index              int                        `json:"index"`
	Stage              string                     `json:"stage"`
	ServiceID          string                     `json:"serviceId,omitempty"`
	VolumeID           string                     `json:"volumeId,omitempty"`
	Version            string                     `json:"version"`
	DesiredVersion     string                     `json:"desiredVersion"`
	ConfigVersion      int64                      `json:"configVersion"`
	Resources          domain.Resources           `json:"resources"`
	ConfigHash         string                     `json:"configHash,omitempty"`
	TLSMode            string                     `json:"tlsMode"`
	RuntimeRecovery    engine.RuntimeRecovery     `json:"runtimeRecovery,omitempty"`
	UnplacedRetirement *engine.UnplacedRetirement `json:"unplacedRetirement,omitempty"`
	ImportPlan         *engine.ImportPlan         `json:"importPlan,omitempty"`
	PublicHTTPPort     int                        `json:"publicHttpPort,omitempty"`
	PublicGRPCPort     int                        `json:"publicGrpcPort,omitempty"`
	Removing           bool                       `json:"removing,omitempty"`
	CleanupStep        string                     `json:"cleanupStep,omitempty"`
}

// pending is the single infrastructure mutation owner. Retry provenance is durable.
type pending struct {
	engine.RetrySchedule
	Op      *infra.Op `json:"op,omitempty"`
	Action  string    `json:"action"`
	Member  string    `json:"member"`
	RetryAt time.Time `json:"retryAt,omitempty"`
}

// rolling freezes order and target configs before any disruption. Targets are
// encrypted separately, never embedded in this journal.
type rolling struct {
	Reason      string   `json:"reason"`
	OperationID string   `json:"operationId,omitempty"`
	Order       []string `json:"order"`
	Position    int      `json:"position"`
	Step        string   `json:"step"`
}

func load(c *domain.Cluster) (*state, error) {
	s := &state{ConfigVersion: 1}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, s); err != nil {
			return nil, fmt.Errorf("decode Weaviate state: %w", err)
		}
	}
	return s, nil
}
func (s *state) save(c *domain.Cluster) error {
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
func (s *state) find(name string) *member {
	for _, m := range s.Members {
		if m.Name == name {
			return m
		}
	}
	return nil
}
func ordered(s *state) []*member {
	out := append([]*member(nil), s.Members...)
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}
func now(rc *engine.Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now().UTC()
}
