// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type privateFamilyBackend struct {
	infra.Backend
	family string
}

func (b privateFamilyBackend) Layout() infra.Layout {
	v := b.Backend.Layout()
	v.PrivateAddressFamily = b.family
	return v
}

// This is the original strict-decoder wire schema, with no listener-policy
// field. Runtime socket selection must not enter any CP payload or token.
type legacyNeo4jSpec struct {
	Version        string                `json:"version"`
	Install        neo4jspec.Install     `json:"install"`
	HeapMB         int                   `json:"heapMB"`
	PageCacheMB    int                   `json:"pageCacheMB"`
	CAPEM          string                `json:"caPEM"`
	Server         neo4jspec.Certificate `json:"server"`
	AdvertisedHost string                `json:"advertisedHost"`
	AdminPassword  string                `json:"adminPassword"`
	Settings       map[string]string     `json:"settings,omitempty"`
	RestartToken   string                `json:"restartToken"`
}

func TestMemberConfigTokenIndependentOfPullCapabilities(t *testing.T) {
	for _, family := range []string{infra.AddressFamilyIPv4, infra.AddressFamilyIPv6} {
		t.Run(family, func(t *testing.T) {
			p, rc, _, observed := setup(t)
			rc.Backend = privateFamilyBackend{rc.Backend, family}
			converge(t, p, rc)
			s, _ := load(rc.Cluster)
			plain, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
			if err != nil {
				t.Fatal(err)
			}
			pull := *rc
			// Include the historical flag to prove a rolling-upgrade request cannot
			// resurrect the removed request-scoped token input.
			pull.AgentConfigCapabilities = []string{"neo4j-ipv6", agentapi.ConfigCapabilityGaleraListenHost, agentapi.ConfigCapabilityMySQLListenHost}
			agentConfig, err := p.MemberConfig(t.Context(), &pull, s.Member.Name)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(agentConfig.EngineSpec, plain.EngineSpec) {
				t.Fatal("agent pull and capability-free CP paths disagree")
			}
			var legacy legacyNeo4jSpec
			decoder := json.NewDecoder(bytes.NewReader(agentConfig.EngineSpec))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&legacy); err != nil {
				t.Fatal("original strict decoder rejected payload", err)
			}
			encoded, err := json.Marshal(legacy)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, plain.EngineSpec) {
				t.Fatal("CP payload differs from original legacy wire schema")
			}
			originalToken := digest(struct {
				Version     string
				JavaHome    string
				Heap, Page  int
				Settings    map[string]string
				Certificate string
			}{legacy.Version, legacy.Install.JavaHome, legacy.HeapMB, legacy.PageCacheMB, legacy.Settings, digest(legacy.Server.CertPEM)})
			if legacy.RestartToken != originalToken {
				t.Fatal("CP token differs from pinned-runtime token computation")
			}
			observed.config = agentConfig
			if err := json.Unmarshal(agentConfig.EngineSpec, &observed.spec); err != nil {
				t.Fatal(err)
			}
			before := observed.reloads
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			if observed.reloads != before {
				t.Fatal("capability-free reconciliation repeatedly reloads an acknowledged token")
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseReady || len(status.Members) != 1 || !status.Members[0].Health {
				t.Fatal("capability-free status did not accept agent token", status.Phase)
			}
			runOp(t, p, rc, &domain.Operation{ID: "family-restart", Kind: domain.OperationRestart})
			if observed.restarts != 1 {
				t.Fatal("capability-free operation failed to complete exactly once", observed.restarts)
			}
		})
	}
}
