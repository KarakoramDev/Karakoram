// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
	"strconv"
	"time"
)

type state struct {
	UID                          string            `json:"uid"`
	NextIndex                    int               `json:"nextIndex"`
	ConfigVersion                int64             `json:"configVersion"`
	ConfigHash                   string            `json:"configHash,omitempty"`
	BootstrapGeneration          int               `json:"bootstrapGeneration"`
	RepositoryGeneration         int               `json:"repositoryGeneration,omitempty"`
	RetiredRepositoryGenerations []int             `json:"retiredRepositoryGenerations,omitempty"`
	BackupCleanupStarted         *time.Time        `json:"backupCleanupStarted,omitempty"`
	CleanedBackupRepositories    []string          `json:"cleanedBackupRepositories,omitempty"`
	RetainedBackupStorages       []string          `json:"retainedBackupStorages,omitempty"`
	Topology                     string            `json:"topology"`
	Members                      []*member         `json:"members"`
	ConversionSeed               string            `json:"conversionSeed,omitempty"`
	Pending                      *pending          `json:"pending,omitempty"`
	Suspended                    bool              `json:"suspended,omitempty"`
	Formed                       bool              `json:"formed,omitempty"`
	Replacement                  *replacementState `json:"replacement,omitempty"`
	Upgrade                      *upgradeState     `json:"upgrade,omitempty"`
	Rolling                      *rollingState     `json:"rolling,omitempty"`
	RestoreRecovery              *domain.Operation `json:"restoreRecovery,omitempty"`
	SwitchoverBlocked            string            `json:"switchoverBlocked,omitempty"`
}
type member struct {
	Name            string `json:"name"`
	NativeReadiness *bool  `json:"nativeReadiness,omitempty"`
	Role            string `json:"role"`
	Index           int    `json:"index"`
	Volume          string `json:"volume,omitempty"`
	ServiceID       string `json:"serviceId,omitempty"`
	VolumeID        string `json:"volumeId,omitempty"`
	// ImportBatch preserves the exact original subset across ambiguous imports.
	// Resources/Replicas are frozen with it; sensitive Env is rebuilt from the
	// sealed member token, never stored in Cluster.Internal.
	ImportBatch        *engine.ImportPlan             `json:"importBatch,omitempty"`
	Stage              string                         `json:"stage"`
	Version            string                         `json:"version"`
	Resources          domain.Resources               `json:"resources"`
	Disk               domain.Resources               `json:"disk"`
	Replicas           int                            `json:"replicas"`
	ConfigVersion      int64                          `json:"configVersion"`
	EtcdState          string                         `json:"etcdState,omitempty"`
	EtcdID             string                         `json:"etcdId,omitempty"`
	Public             bool                           `json:"public,omitempty"`
	Options            json.RawMessage                `json:"options,omitempty"`
	DeployOp           *infra.Op                      `json:"deployOp,omitempty"`
	Topology           string                         `json:"topology,omitempty"`
	Removing           bool                           `json:"removing,omitempty"`
	ProxyDrainVersion  int64                          `json:"proxyDrainVersion,omitempty"`
	ProxyReady         bool                           `json:"proxyReady,omitempty"`
	Retirement         string                         `json:"retirement,omitempty"`
	RetirementOp       *infra.Op                      `json:"retirementOp,omitempty"`
	EtcdLearner        bool                           `json:"etcdLearner,omitempty"`
	InitialCluster     string                         `json:"initialCluster,omitempty"`
	TLSPending         bool                           `json:"tlsPending,omitempty"`
	TLSReloadPending   bool                           `json:"tlsReloadPending,omitempty"`
	TLSReloadVersion   int64                          `json:"tlsReloadVersion,omitempty"`
	TLSReloadOnly      bool                           `json:"tlsReloadOnly,omitempty"`
	TLSReloadBlocked   string                         `json:"tlsReloadBlocked,omitempty"`
	ProxyClockRecovery map[string]*proxyClockRecovery `json:"proxyClockRecovery,omitempty"`
	Offline            bool                           `json:"offline,omitempty"`
	RuntimeRecovery    engine.RuntimeRecovery         `json:"runtimeRecovery,omitempty"`
	UnplacedRetirement *engine.UnplacedRetirement     `json:"unplacedRetirement,omitempty"`
}
type pending struct {
	Op        infra.Op         `json:"op"`
	Action    string           `json:"action"`
	Member    string           `json:"member"`
	Resources domain.Resources `json:"resources"`
	Replicas  int              `json:"replicas"`
	Version   string           `json:"version,omitempty"`
}

func load(c *domain.Cluster) (*state, error) {
	s := &state{ConfigVersion: 1, BootstrapGeneration: 1}
	if len(c.Internal) > 0 {
		var root map[string]json.RawMessage
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return nil, err
		}
		if raw := root["postgres"]; len(raw) > 0 {
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
	root["postgres"] = raw
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
		if s.Formed {
			for _, m := range s.data() {
				if m.Stage != "joined" || m.EtcdLearner {
					return
				}
			}
		}
		i := s.NextIndex
		s.NextIndex++
		name := fmt.Sprintf("%sp%d", c.Name, i)
		etcdState := "new"
		if s.Formed && s.Topology == "ha" {
			etcdState = "pending"
		}
		s.Members = append(s.Members, &member{Name: name, Role: "data", Index: i, Volume: name + "v", Stage: "planned", ConfigVersion: s.ConfigVersion, EtcdState: etcdState})
		if s.Formed {
			break
		}
	}
	if c.Spec.Components.Extra["proxy"].Replicas > 0 && s.find(c.Name+"px") == nil {
		s.Members = append(s.Members, &member{Name: c.Name + "px", Role: "proxy", Stage: "planned", ConfigVersion: s.ConfigVersion})
	}
}

// newDataMember reserves a monotonic identity before any infrastructure is
// created. Retrying after a crash therefore cannot reuse a retired member.
func (s *state) newDataMember(c *domain.Cluster) *member {
	i := s.NextIndex
	s.NextIndex++
	name := fmt.Sprintf("%sp%d", c.Name, i)
	m := &member{
		Name: name, Role: "data", Index: i, Volume: name + "v",
		Stage: "planned", ConfigVersion: s.ConfigVersion, EtcdState: "pending",
	}
	s.Members = append(s.Members, m)
	return m
}
func (p *Core) RelayTargets(c *domain.Cluster) []string {
	s, err := load(c)
	if err != nil {
		return nil
	}
	s.allocate(c)
	out := []string{}
	for _, m := range s.Members {
		for _, port := range []int{5432, 8008, 2379, 2380, 9443} {
			out = append(out, net.JoinHostPort(m.Name, strconv.Itoa(port)))
		}
	}
	if p.options.Flavour != nil {
		out = append(out, p.options.Flavour.RelayTargets(c)...)
	}
	return out
}

// DataMembers returns active data identities in allocation order. Credentials
// remain available only through the authenticated MemberConfig path.
func (*Core) DataMembers(c *domain.Cluster) []string {
	s, err := load(c)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(s.Members))
	for _, m := range s.data() {
		if !m.Removing {
			names = append(names, m.Name)
		}
	}
	return names
}
