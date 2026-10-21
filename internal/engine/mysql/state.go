// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"time"
)

// state contains only durable intent and receipts; credentials are sealed separately.
type state struct {
	UID                  string              `json:"uid"`
	NextIndex            int                 `json:"nextIndex"`
	ConfigVersion        int64               `json:"configVersion"`
	ConfigHash           string              `json:"configHash,omitempty"`
	Topology             string              `json:"topology"`
	Members              []*member           `json:"members"`
	Pending              *pending            `json:"pending,omitempty"`
	Suspended            bool                `json:"suspended,omitempty"`
	Formation            formation           `json:"formation"`
	Rolling              *rollingState       `json:"rolling,omitempty"`
	Replacement          *replacementState   `json:"replacement,omitempty"`
	TLSRotation          *tlsRotationState   `json:"tlsRotation,omitempty"`
	GaleraTLS            *galeraTLSRotation  `json:"galeraTLS,omitempty"`
	LastReplacement      string              `json:"lastReplacement,omitempty"`
	LastTLSRotation      string              `json:"lastTLSRotation,omitempty"`
	GTIDIncidents        []gtidIncident      `json:"gtidIncidents,omitempty"`
	LastGTIDSafeAt       time.Time           `json:"lastGTIDSafeAt,omitempty"`
	GTIDWindows          []domain.TimeWindow `json:"gtidWindows,omitempty"`
	GTIDWindowGeneration int                 `json:"gtidWindowGeneration,omitempty"`
}

// Incidents are retained after a fresh full baseline so old chains can never
// silently cross a previously observed discontinuity.
type gtidIncident struct {
	DetectedAt  time.Time `json:"detectedAt"`
	SafeThrough time.Time `json:"safeThrough,omitempty"`
	Members     []string  `json:"members"`
	BaselineAt  time.Time `json:"baselineAt,omitempty"`
	Backup      string    `json:"backup,omitempty"`
	Audited     bool      `json:"audited,omitempty"`
}
type formation struct {
	GroupName      string       `json:"groupName"`
	FormationEpoch uint64       `json:"formationEpoch"`
	Seed           string       `json:"seed"`
	State          string       `json:"state"`
	Blocked        string       `json:"blocked,omitempty"`
	Reformation    *reformation `json:"reformation,omitempty"`
	Adopting       bool         `json:"adopting,omitempty"`
	StartPosition  string       `json:"startPosition,omitempty"`
}
type reformation struct {
	State              string                                 `json:"state"`
	Observations       map[string]mysqloffline                `json:"observations,omitempty"`
	Blocked            string                                 `json:"blocked,omitempty"`
	GaleraObservations map[string]myspec.WSRepRecoverResponse `json:"galeraObservations,omitempty"`
}
type mysqloffline struct {
	GTID string `json:"gtid"`
	UUID string `json:"uuid"`
}
type member struct {
	Name              string               `json:"name"`
	Role              string               `json:"role"`
	Index             int                  `json:"index"`
	Volume            string               `json:"volume,omitempty"`
	VolumeID          string               `json:"volumeId,omitempty"`
	ServiceID         string               `json:"serviceId,omitempty"`
	Stage             string               `json:"stage"`
	ImportBatch       *engine.ImportPlan   `json:"importBatch,omitempty"`
	DeployOp          *infra.Op            `json:"deployOp,omitempty"`
	NativeReadiness   *bool                `json:"nativeReadiness,omitempty"`
	Version           string               `json:"version"`
	Resources         domain.Resources     `json:"resources"`
	Replicas          int                  `json:"replicas"`
	ConfigVersion     int64                `json:"configVersion"`
	Initialized       bool                 `json:"initialized,omitempty"`
	ServerUUID        string               `json:"serverUUID,omitempty"`
	Offline           bool                 `json:"offline,omitempty"`
	Public            bool                 `json:"public,omitempty"`
	ExposureHash      string               `json:"exposureHash,omitempty"`
	Clone             *myspec.CloneRequest `json:"clone,omitempty"`
	CloneDone         bool                 `json:"cloneDone,omitempty"`
	CloneReplacesData bool                 `json:"cloneReplacesData,omitempty"`
	Retirement        string               `json:"retirement,omitempty"`
	Removing          bool                 `json:"removing,omitempty"`
	SlowShutdown      bool                 `json:"slowShutdown,omitempty"`
	RetirementOp      *infra.Op            `json:"retirementOp,omitempty"`
	Parameters        map[string]string    `json:"restartParameters,omitempty"`
}
type pending struct {
	Op     infra.Op `json:"op"`
	Action string   `json:"action"`
	Member string   `json:"member"`
}

func load(c *domain.Cluster) (*state, error) {
	s := &state{ConfigVersion: 1, Formation: formation{State: "unformed", FormationEpoch: 1}}
	if len(c.Internal) > 0 {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return nil, err
		}
		if raw := root["mysql"]; len(raw) > 0 {
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
	root["mysql"] = raw
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
func (s *state) data() []*member {
	out := []*member{}
	for _, m := range s.Members {
		if m.Role == "data" {
			out = append(out, m)
		}
	}
	return out
}
func (s *state) allocate(c *domain.Cluster) {
	for len(s.data()) < c.Spec.Components.Engine.Replicas {
		i := s.NextIndex
		s.NextIndex++
		name := fmt.Sprintf("%sm%d", c.Name, i)
		s.Members = append(s.Members, &member{Name: name, Role: "data", Index: i, Volume: name + "v", Stage: "planned", ConfigVersion: s.ConfigVersion})
		if s.Formation.Seed == "" {
			s.Formation.Seed = name
		}
	}
	if c.Spec.Components.Extra["proxy"].Replicas > 0 && s.find(c.Name+"mx") == nil {
		s.Members = append(s.Members, &member{Name: c.Name + "mx", Role: "proxy", Stage: "planned", ConfigVersion: s.ConfigVersion})
	}
}
func (*Core) RelayTargets(c *domain.Cluster) []string {
	s, err := load(c)
	if err != nil {
		return nil
	}
	s.allocate(c)
	out := []string{}
	for _, m := range s.Members {
		for _, port := range []string{"3306", "9443"} {
			out = append(out, net.JoinHostPort(m.Name, port))
		}
		if m.Role == "proxy" {
			out = append(out, net.JoinHostPort(m.Name, "3307"))
		}
	}
	return out
}
func (*Core) DataMembers(c *domain.Cluster) []string {
	s, err := load(c)
	if err != nil {
		return nil
	}
	out := []string{}
	for _, m := range s.data() {
		out = append(out, m.Name)
	}
	return out
}

var _ engine.Provider = (*Core)(nil)
