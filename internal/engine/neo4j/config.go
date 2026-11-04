// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func install(version string) (neo4jspec.Install, error) {
	j, err := nativeartifacts.Get("temurin-jre-21")
	if err != nil {
		return neo4jspec.Install{}, err
	}
	n := "neo4j-community-" + version
	return neo4jspec.Install{Neo4jHome: "/opt/karakoram/" + n + "/" + n, JavaHome: "/opt/karakoram/temurin-jre-21/jdk-" + j.Version + "-jre"}, nil
}
func (p *Provider) runtimeSpec(rc *engine.Context, s *state) (infra.RuntimeSpec, error) {
	prepare := []string{"sudo apt-get update && sudo apt-get install -y ca-certificates curl python3 tar gzip"}
	names := []string{"neo4j-community-" + rc.Cluster.Spec.Version, "temurin-jre-21"}
	if s.Member.PreviousVersion != "" && s.Member.PreviousVersion != rc.Cluster.Spec.Version {
		names = append(names, "neo4j-community-"+s.Member.PreviousVersion)
	}
	for _, n := range names {
		a, e := nativeartifacts.Get(n)
		if e != nil {
			return infra.RuntimeSpec{}, e
		}
		u, e := nativeartifacts.ResolveURL(a)
		if e != nil {
			return infra.RuntimeSpec{}, e
		}
		prepare = append(prepare, nativeartifacts.PrepareCommands(a, u, "/opt/karakoram/"+a.Name, rc.Backend.Layout().User)...)
	}
	url := rc.AgentURL
	if url == "" {
		url = p.options.ControlPlaneURL
	}
	return (engine.AgentRuntime{Cluster: rc.Cluster.Name, Member: s.Member.Name, Engine: neo4jspec.Engine, Volume: s.Member.Volume, AgentURL: url, InternalCAPEM: p.options.InternalCAPEM, Ports: []infra.Port{{Port: 7687, Protocol: "tcp"}, {Port: 7473, Protocol: "tcp"}, {Port: 9443, Protocol: "tcp"}}, Prepare: prepare}).Spec(rc.Backend.Layout()), nil
}
func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	m := s.Member
	if m == nil || m.Name != name {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	o := s.ConfigOptions
	if o == nil {
		// Older persisted states have no accepted options snapshot. Do not
		// expose a pending configuration through an autonomous agent pull.
		hash := digest(struct {
			Version   string
			Resources domain.Resources
			Options   any
		}{rc.Cluster.Spec.Version, rc.Cluster.Spec.Components.Engine.Resources, rc.Cluster.Spec.EngineOptions})
		if allowed, _ := rc.DisruptionAllowed(); !allowed && s.ConfigHash != "" && s.ConfigHash != hash {
			return agentapi.MemberConfig{}, engine.ErrUnavailable
		}
		parsed, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
		if err != nil {
			return agentapi.MemberConfig{}, err
		}
		o = &parsed
	}
	h, pc, err := memory(m.Resources.MinRAM, *o)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	get := func(k string) string {
		if err != nil {
			return ""
		}
		var b []byte
		b, err = secret(ctx, rc, k)
		return string(b)
	}
	// Config pulls retain the old release until preparation of the new runtime
	// has completed; a version update must never launch an uninstalled binary.
	version := m.Version
	if version == "" {
		version = m.PendingVersion
	}
	if version == "" {
		version = rc.Cluster.Spec.Version
	}
	inst, e := install(version)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	sp := neo4jspec.Spec{Version: version, Install: inst, HeapMB: h, PageCacheMB: pc, CAPEM: get("tls/ca.crt"), Server: neo4jspec.Certificate{CertPEM: get("tls/servers/" + name + ".crt"), KeyPEM: get("tls/servers/" + name + ".key")}, AdvertisedHost: name, AdminPassword: get("users/neo4j"), Settings: o.Settings}
	sp.RestartToken = digest(struct {
		Version     string
		Heap, Page  int
		Settings    map[string]string
		Certificate string
	}{sp.Version, h, pc, o.Settings, digest(sp.Server.CertPEM)})
	tls := agentapi.Certificate{CertPEM: get("tls/agents/" + name + ".crt"), KeyPEM: get("tls/agents/" + name + ".key")}
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	desired := []string{neo4jspec.Process}
	// Maintenance fences control-plane convergence, not the member process.
	// Offline jobs own their durable stop hold and must be able to restart
	// Neo4j before the control plane observes readiness and releases its fence.
	maintenance := m.Offline
	if maintenance {
		desired = []string{}
	}
	raw, err := json.Marshal(sp)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	var volume *agentapi.VolumeIdentity
	if m.VolumeID != "" {
		volume = &agentapi.VolumeIdentity{ClusterUID: s.UID, Member: m.Name, Volume: m.Volume, VolumeID: m.VolumeID, Initialized: m.RuntimeRecovery.Initialized}
	}
	return agentapi.MemberConfig{Cluster: rc.Cluster.Name, Member: name, Engine: neo4jspec.Engine, EngineSpec: raw, EngineVersion: sp.Version, Role: "single", Port: 7687, ConfigVersion: s.ConfigVersion, DesiredProcesses: desired, Maintenance: maintenance, Volume: volume, AgentTLS: tls}, nil
}
