// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// state contains no credentials. Maintenance is an operation ID, so later
// offline workflows can fence config pulls and ordinary convergence durably.
type state struct {
	UID                string                   `json:"uid"`
	ConfigVersion      int64                    `json:"configVersion"`
	ConfigHash         string                   `json:"configHash,omitempty"`
	ConfigOptions      *EngineOptions           `json:"configOptions,omitempty"`
	Member             *member                  `json:"member,omitempty"`
	ExposureAdmission  engine.ExposureAdmission `json:"exposureAdmission,omitempty"`
	Maintenance        string                   `json:"maintenance,omitempty"`
	RestoreRecovery    *domain.Operation        `json:"restoreRecovery,omitempty"`
	Suspended          bool                     `json:"suspended,omitempty"`
	TLSRotationID      string                   `json:"tlsRotationId,omitempty"`
	PasswordRotationID string                   `json:"passwordRotationId,omitempty"`
	CloneQueued        bool                     `json:"cloneQueued,omitempty"`
	CloneComplete      bool                     `json:"cloneComplete,omitempty"`
}
type member struct {
	Name             string                 `json:"name"`
	Volume           string                 `json:"volume"`
	ServiceID        string                 `json:"serviceId,omitempty"`
	VolumeID         string                 `json:"volumeId,omitempty"`
	Stage            string                 `json:"stage"`
	Version          string                 `json:"version,omitempty"`
	PreviousVersion  string                 `json:"previousVersion,omitempty"`
	Resources        domain.Resources       `json:"resources"`
	Disk             domain.Resources       `json:"disk"`
	ImportBatch      *engine.ImportPlan     `json:"importBatch,omitempty"`
	Pending          *infra.Op              `json:"pending,omitempty"`
	PendingAction    string                 `json:"pendingAction,omitempty"`
	PendingVersion   string                 `json:"pendingVersion,omitempty"`
	PendingResources domain.Resources       `json:"pendingResources"`
	RuntimeRecovery  engine.RuntimeRecovery `json:"runtimeRecovery,omitempty"`
	Offline          bool                   `json:"offline,omitempty"`
	Exposure         domain.Exposure        `json:"exposure"`
	Public           bool                   `json:"public,omitempty"`
}

func load(c *domain.Cluster) (*state, error) {
	s := &state{ConfigVersion: 1}
	if len(c.Internal) > 0 {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return nil, err
		}
		if raw := root["neo4j"]; len(raw) > 0 {
			if err := json.Unmarshal(raw, s); err != nil {
				return nil, err
			}
		}
	}
	return s, nil
}
func save(c *domain.Cluster, s *state) error {
	root := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return err
		}
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	root["neo4j"] = raw
	c.Internal, err = json.Marshal(root)
	return err
}
func (*Provider) CanClaimOperation(c *domain.Cluster) (bool, error) {
	s, err := load(c)
	if err != nil {
		return false, err
	}
	return s.Maintenance == "" && s.RestoreRecovery == nil && s.Member != nil &&
		s.Member.Pending == nil && s.Member.Stage == "ready" && s.Member.Version == c.Spec.Version, nil
}
