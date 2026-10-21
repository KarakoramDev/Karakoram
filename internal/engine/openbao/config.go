// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net"
)

// lookupRuntimeArtifact is replaceable by package tests qualifying future catalog entries.
var lookupRuntimeArtifact = nativeartifacts.Get

func (p *Provider) runtimeSpec(rc *engine.Context, m *member, version string) (infra.RuntimeSpec, error) {
	a, err := lookupRuntimeArtifact("openbao")
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	if a.Version != version {
		return infra.RuntimeSpec{}, engine.Invalid("unsupported OpenBao version")
	}
	url, err := nativeartifacts.ResolveURL(a)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	layout := rc.Backend.Layout()
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	options, err := parseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	prepare := nativeartifacts.PrepareCommands(a, url, "/opt/karakoram/openbao-"+version, layout.User)
	plugins, err := pluginPrepare(options, layout.User)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	prepare = append(prepare, plugins...)
	return (engine.AgentRuntime{Cluster: rc.Cluster.Name, Member: m.Name, Engine: baospec.Engine, Volume: m.Volume, AgentURL: agentURL, InternalCAPEM: p.options.InternalCAPEM, Ports: []infra.Port{{Port: 8200, Protocol: "tcp"}, {Port: 8201, Protocol: "tcp"}, {Port: baospec.RecoveryPort, Protocol: "tcp"}}, Prepare: prepare}).Spec(layout), nil
}
func runtimeResources(r domain.Resources) domain.Resources { r.MinDisk = 1; r.MaxDisk = 3; return r }
func activateConfig(c *domain.Cluster, s *internalState, m *member) {
	m.ConfigVersion = s.ConfigVersion
	m.DesiredVersion = c.Spec.Version
	m.EngineOptions = append(json.RawMessage(nil), c.Spec.EngineOptions...)
	m.EngineConfig = c.Spec.EngineConfig
	m.TLSMode = c.Spec.TLS.Mode
	m.ConfigResources = runtimeResources(c.Spec.Components.Engine.Resources)
	m.ConfigDisk = c.Spec.Components.Engine.Resources
}

func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, err := loadState(rc.Cluster)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	m := s.find(name)
	if m == nil {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	version := m.DesiredVersion
	if version == "" {
		version = rc.Cluster.Spec.Version
	}
	v := agentapi.MemberConfig{Engine: baospec.Engine, Cluster: rc.Cluster.Name, Member: name, Role: "openbao", EngineVersion: version, ConfigVersion: m.ConfigVersion, Port: 8200, TLS: agentapi.TLSConfig{Mode: "requireTLS"}, DesiredProcesses: []string{baospec.Process}}
	if m.Held {
		v.DesiredProcesses = []string{}
	}
	// PostgreSQL HA peers hold no local engine data. Preserve volume identity
	// fencing, but do not classify an empty peer disk as lost Raft storage.
	localInitialized := !p.postgresStorage(rc.Cluster) && (m.RuntimeRecovery.Initialized || m.Stage == "ready")
	v.Volume = engine.RuntimeVolumeIdentity(s.UID, m.Name, m.Volume, m.VolumeID, localInitialized)
	ca, err := secret(ctx, rc, "tls/ca.crt")
	if err != nil {
		return v, err
	}
	v.TLS.CAPEM = string(ca)
	cert, err := memberCertificate(ctx, rc, "tls/members/"+name+".pem")
	if err != nil {
		return v, err
	}
	v.TLS.CertPEM = cert.CertPEM
	v.TLS.KeyPEM = cert.KeyPEM
	v.AgentTLS, err = memberCertificate(ctx, rc, "tls/agents/"+name+".pem")
	if err != nil {
		return v, err
	}
	var options Options
	if len(m.EngineOptions) > 0 {
		if err = json.Unmarshal(m.EngineOptions, &options); err != nil {
			return v, engine.Invalid("invalid OpenBao options")
		}
	}
	bind := "::"
	if rc.Backend.Layout().PrivateAddressFamily == infra.AddressFamilyIPv4 {
		bind = "0.0.0.0"
	}
	spec := baospec.MemberSpec{NodeID: name, Version: version, BinaryPath: "/opt/karakoram/openbao-" + version + "/bao", ListenAddress: bind, APIAddr: "https://" + net.JoinHostPort(name, "8200"), ClusterAddr: "https://" + net.JoinHostPort(name, "8201"), Storage: baospec.Storage{Type: baospec.StorageRaft}, Seal: baospec.Seal{Type: baospec.SealShamir}, ProcessGeneration: m.ProcessGeneration, LogLevel: options.LogLevel}
	recoveryCA, err := memberCertificate(ctx, rc, secretRecoveryCA)
	if err != nil {
		return v, err
	}
	spec.RecoveryCAPEM = recoveryCA.CertPEM
	spec.PluginDirectory = pluginDirectory
	spec.Plugins = memberPlugins(options)
	spec.TrustCAPEMs, err = p.trustCAs(ctx, rc)
	if err != nil {
		return v, err
	}
	spec.Seal, err = p.memberSeal(ctx, rc, options)
	if err != nil {
		return v, err
	}
	if options.Storage.Type == "postgresql" {
		spec.Storage, err = p.memberPostgresStorage(ctx, rc, s)
		if err != nil {
			return v, err
		}
	}
	for _, other := range ordered(s) {
		if options.Storage.Type != "postgresql" && other.Name != name && !other.Removing {
			spec.Storage.RetryJoin = append(spec.Storage.RetryJoin, baospec.RetryJoin{LeaderAPIAddr: "https://" + net.JoinHostPort(other.Name, "8200")})
		}
	}
	v.EngineSpec, err = json.Marshal(spec)
	return v, err
}

var _ engine.AgentConfigSource = (*Provider)(nil)
