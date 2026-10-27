// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"math"
	"net/url"
	"path"
	"slices"
	"strconv"
	"time"
)

func hash(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func certificate(ctx context.Context, rc *engine.Context, base string) (agentapi.Certificate, error) {
	crt, e := secret(ctx, rc, base+".crt")
	if e != nil {
		return agentapi.Certificate{}, e
	}
	key, e := secret(ctx, rc, base+".key")
	return agentapi.Certificate{CertPEM: string(crt), KeyPEM: string(key)}, e
}
func (p *Provider) desiredConfig(ctx context.Context, rc *engine.Context, s *state, m *member) (agentapi.MemberConfig, error) {
	o, e := parseOptionsWithDefaults(rc.Cluster.Spec.EngineOptions, rc.Cluster.Spec.Topology.Type)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	ca, e := secret(ctx, rc, secretCA)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	admin, e := secret(ctx, rc, secretAdmin)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	pw, e := secret(ctx, rc, secretClusterPassword)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	bindHost := "::"
	if rc.Backend.Layout().PrivateAddressFamily == infra.AddressFamilyIPv4 {
		bindHost = "0.0.0.0"
	}
	spec := wvspec.MemberSpec{Version: rc.Cluster.Spec.Version, NodeName: m.Name, BindHost: bindHost, Ports: wvspec.Ports{HTTP: 8080, GRPC: 50051, Gossip: 7946, Data: 7947, Raft: 8300, RaftRPC: 8301, Metrics: 2112}, BootstrapExpect: len(s.Voters), TLS: rc.Cluster.Spec.TLS.Mode == "requireTLS", RootUser: AdminUser, AllowedKeys: []string{string(admin)}, ClusterUsername: ClusterUser, ClusterPassword: string(pw), Modules: []string{"backup-s3"}, InferenceEndpoints: map[string]string{}, ReplicationMinimumFactor: o.ReplicationMinimumFactor, AsyncReplication: o.AsyncReplication, QueryDefaultsLimit: o.QueryDefaultsLimit, DiskWarningPercent: o.DiskWarningPercent, DiskReadonlyPercent: o.DiskReadonlyPercent, MemoryLimitMiB: int64(rc.Cluster.Spec.Components.Engine.Resources.MaxRAM * 1024 * .85), GOMAXPROCS: max(1, int(math.Floor(rc.Cluster.Spec.Components.Engine.Resources.MaxCPU)))}
	if s.Rotation != nil && s.Rotation.User == AdminUser &&
		(s.Rotation.Stage == "add" || s.Rotation.Stage == "switch" || s.Rotation.Stage == "verify") {
		old, err := secret(ctx, rc, rotationOld)
		if err != nil {
			return agentapi.MemberConfig{}, err
		}
		next, err := secret(ctx, rc, rotationNew)
		if err != nil {
			return agentapi.MemberConfig{}, err
		}
		spec.AllowedKeys = []string{string(old), string(next)}
	}
	for _, name := range s.Voters {
		spec.RaftJoin = append(spec.RaftJoin, name+":8301")
		spec.GossipJoin = append(spec.GossipJoin, name+":7946")
	}
	for _, mod := range o.Modules {
		if !slices.Contains(spec.Modules, mod) {
			spec.Modules = append(spec.Modules, mod)
		}
	}
	for mod, v := range o.Inference {
		spec.InferenceEndpoints[InferenceEnv(mod)] = v.URL
		if !slices.Contains(spec.Modules, mod) {
			spec.Modules = append(spec.Modules, mod)
		}
	}
	slices.Sort(spec.Modules[1:])
	if name := rc.Cluster.Spec.Backup.Storage; name != "" {
		storage, e := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, name)
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		if storage == nil || storage.Type != "s3" {
			return agentapi.MemberConfig{}, invalid("backup storage must exist and use S3")
		}
		u, e := url.Parse(storage.EndpointURL)
		if e != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
			return agentapi.MemberConfig{}, invalid("invalid S3 endpoint")
		}
		spec.Backup = &wvspec.BackupS3{Bucket: storage.Bucket, Path: path.Join(storage.Prefix, rc.Cluster.Ref().Namespace, rc.Cluster.Name, s.UID), Endpoint: u.Host, UseSSL: u.Scheme == "https", Region: storage.Region, AccessKey: storage.Credentials.AccessKeyID, SecretKey: storage.Credentials.SecretAccessKey}
	}
	if e = spec.Validate(); e != nil {
		return agentapi.MemberConfig{}, e
	}
	cfg := agentapi.MemberConfig{Engine: "weaviate", Cluster: rc.Cluster.Name, Member: m.Name, Role: "weaviate", EngineVersion: spec.Version, ConfigVersion: m.ConfigVersion, DesiredProcesses: []string{"weaviate"}, TLS: agentapi.TLSConfig{Mode: rc.Cluster.Spec.TLS.Mode, CAPEM: string(ca)}}
	cfg.AgentTLS, e = certificate(ctx, rc, "tls/agents/"+m.Name)
	if e != nil {
		return cfg, e
	}
	if spec.TLS {
		leaf, e := certificate(ctx, rc, "tls/members/"+m.Name)
		if e != nil {
			return cfg, e
		}
		cfg.TLS.CertPEM = leaf.CertPEM
		cfg.TLS.KeyPEM = leaf.KeyPEM
	}
	cfg.EngineSpec, e = json.Marshal(spec)
	return cfg, e
}
func configHash(cfg agentapi.MemberConfig) string {
	cfg.ConfigVersion = 0
	cfg.Volume = nil
	var spec wvspec.MemberSpec
	_ = json.Unmarshal(cfg.EngineSpec, &spec)
	spec.RestartToken = ""
	cfg.EngineSpec, _ = json.Marshal(spec)
	return hash(cfg)
}
func activateConfig(ctx context.Context, rc *engine.Context, s *state, m *member, cfg agentapi.MemberConfig) error {
	s.ConfigVersion++
	cfg.ConfigVersion = s.ConfigVersion
	raw, e := json.Marshal(cfg)
	if e != nil {
		return e
	}
	if e = putSecret(ctx, rc, "config/"+m.Name, raw); e != nil {
		return e
	}
	m.ConfigVersion = cfg.ConfigVersion
	m.DesiredVersion = cfg.EngineVersion
	m.TLSMode = cfg.TLS.Mode
	m.ConfigHash = configHash(cfg)
	return nil
}

func (p *Provider) publishExemptConfig(ctx context.Context, rc *engine.Context, s *state, m *member, desired agentapi.MemberConfig) (bool, error) {
	active, err := p.MemberConfig(ctx, rc, m.Name)
	if err != nil {
		return false, err
	}
	projected := active
	projected.AgentTLS = desired.AgentTLS
	leafChanged := false
	if active.TLS.CertPEM != "" && active.TLS.CertPEM != desired.TLS.CertPEM && active.TLS.CAPEM == desired.TLS.CAPEM {
		urgent, err := pki.NeedsRenewal([]byte(active.TLS.CertPEM), 7*24*time.Hour, now(rc))
		if err != nil {
			return false, err
		}
		if urgent {
			projected.TLS = desired.TLS
			leafChanged = true
		}
	}
	if configHash(projected) == configHash(active) {
		return false, nil
	}
	if leafChanged {
		if err = restartConfig(&projected, s.ConfigVersion+1); err != nil {
			return false, err
		}
	}
	if err = activateConfig(ctx, rc, s, m, projected); err != nil {
		return false, err
	}
	m.Stage = "deployed"
	return true, nil
}
func (p *Provider) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	m := s.find(name)
	if m == nil {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	raw, e := secret(ctx, rc, "config/"+name)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	var cfg agentapi.MemberConfig
	if e = json.Unmarshal(raw, &cfg); e != nil {
		return cfg, e
	}
	cfg.Volume = engine.RuntimeVolumeIdentity(s.UID, m.Name, m.Volume, m.VolumeID, m.RuntimeRecovery.Initialized)
	// Agent TLS renews independently; native TLS remains frozen until this member's roll.
	cfg.AgentTLS, e = certificate(ctx, rc, "tls/agents/"+name)
	return cfg, e
}
func restartConfig(cfg *agentapi.MemberConfig, token int64) error {
	var spec wvspec.MemberSpec
	if e := json.Unmarshal(cfg.EngineSpec, &spec); e != nil {
		return e
	}
	spec.RestartToken = strconv.FormatInt(token, 10)
	var e error
	cfg.EngineSpec, e = json.Marshal(spec)
	return e
}
