// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"path"
	"strings"
)

// PrepareCommands verifies and installs the locked server and matching backup tools.
func PrepareCommands(version, owner string) ([]string, error) {
	return New(Options{}).prepareCommands(version, owner)
}
func (p *Core) prepareCommands(version, owner string) ([]string, error) {
	server, backup, err := p.artifactPins(version)
	if err != nil {
		return nil, err
	}
	commands := []string{"sudo apt-get update && sudo apt-get install -y ca-certificates curl python3 xz-utils libaio1t64 libnuma1 libev4t64"}
	if p.Name() == "pxc" {
		commands[0] += " socat openssl"
	}
	for _, name := range []string{server, backup} {
		a, e := nativeartifacts.Get(name)
		if e != nil {
			return nil, e
		}
		u, e := nativeartifacts.ResolveURL(a)
		if e != nil {
			return nil, e
		}
		commands = append(commands, nativeartifacts.PrepareCommands(a, u, artifactRoot(a), owner)...)
	}
	return commands, nil
}
func artifactRoot(a nativeartifacts.Artifact) string {
	// Percona catalog keys already include the pinned release.
	return "/opt/karakoram/" + a.Name
}
func artifactDir(name string) (string, error) {
	a, err := nativeartifacts.Get(name)
	if err != nil {
		return "", err
	}
	top := strings.TrimSuffix(strings.TrimSuffix(a.File, ".tar.gz"), ".tar.xz")
	return path.Join(artifactRoot(a), top), nil
}
func (p *Core) runtimeSpec(rc *engine.Context, m *member) (infra.RuntimeSpec, error) {
	prepare, err := p.prepareCommands(m.Version, rc.Backend.Layout().User)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	if m.Role == "proxy" {
		a, e := nativeartifacts.Get("proxysql")
		if e != nil {
			return infra.RuntimeSpec{}, e
		}
		u, e := nativeartifacts.ResolveURL(a)
		if e != nil {
			return infra.RuntimeSpec{}, e
		}
		prepare = []string{"sudo apt-get update && sudo apt-get install -y ca-certificates curl", "printf '#!/bin/sh\\nexit 101\\n' | sudo tee /usr/sbin/policy-rc.d >/dev/null && sudo chmod 0755 /usr/sbin/policy-rc.d"}
		prepare = append(prepare, nativeartifacts.PrepareCommands(a, u, "/opt/karakoram", rc.Backend.Layout().User)...)
		prepare = append(prepare, "if [ -d /run/systemd/system ]; then sudo systemctl disable --now proxysql.service && sudo systemctl mask proxysql.service; fi")
	}
	ports := []infra.Port{{Port: 3306, Protocol: "tcp"}, {Port: 9443, Protocol: "tcp"}}
	if p.Name() == "pxc" && m.Role == "data" {
		ports = append(ports, infra.Port{Port: myspec.PortGalera, Protocol: "tcp"}, infra.Port{Port: myspec.PortGalera, Protocol: "udp"}, infra.Port{Port: myspec.PortGaleraIST, Protocol: "tcp"}, infra.Port{Port: myspec.PortGaleraSST, Protocol: "tcp"})
	}
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	if m.Role == "proxy" && o.Proxy.ReaderPort {
		ports = append(ports, infra.Port{Port: 3307, Protocol: "tcp"})
	}
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	runtime := engine.AgentRuntime{Cluster: rc.Cluster.Name, Member: m.Name, Engine: "mysql", Volume: m.Volume, AgentURL: agentURL, InternalCAPEM: p.options.InternalCAPEM, Ports: ports, Prepare: prepare}
	// Agent transport readiness is independent of GR quorum and clone completion.
	return runtime.Spec(rc.Backend.Layout()), nil
}
func (p *Core) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	m := s.find(name)
	if m == nil {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	revision, revisionErr := accessRevision(ctx, rc)
	if revisionErr != nil {
		return agentapi.MemberConfig{}, revisionErr
	}
	configVersion := max(m.ConfigVersion, revision)
	o, err := ParseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	get := func(key string) string {
		if err != nil {
			return ""
		}
		var b []byte
		b, err = secret(ctx, rc, key)
		return string(b)
	}
	cert := func(kind string) *myspec.Certificate {
		return &myspec.Certificate{CertPEM: get("tls/" + kind + "/" + name + ".crt"), KeyPEM: get("tls/" + kind + "/" + name + ".key")}
	}
	credential := func(name string) myspec.Credentials {
		return myspec.Credentials{Username: name, Password: get("users/" + name)}
	}
	version := m.Version
	if version == "" {
		version = rc.Cluster.Spec.Version
	}
	server, backup, e := p.artifactPins(version)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	serverDir, e := artifactDir(server)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	backupDir, e := artifactDir(backup)
	if e != nil {
		return agentapi.MemberConfig{}, e
	}
	ram := m.Resources.MinRAM
	if ram <= 0 {
		ram = rc.Cluster.Spec.Components.Engine.Resources.MinRAM
	}
	buffer := int64(ram * float64(1<<30) / 2)
	if buffer < 128<<20 {
		buffer = 128 << 20
	}
	o.Parameters = memberParameters(m, o.Parameters)
	sp := myspec.Spec{Member: name, Role: m.Role, Topology: s.Topology, Ordinal: m.Index, Version: version, ServerDir: serverDir, XtraBackupDir: backupDir, BufferPoolBytes: buffer, MaxConnections: 200, Parameters: o.Parameters, CAPEM: get("tls/ca.crt"), ServerCert: cert("servers"), Superuser: credential("root"), Agent: credential("kk_agent"), Replication: credential("kk_repl"), Monitor: credential("kk_monitor"), BackupUser: credential("kk_backup"), AllowInitialize: m.Role == "data" && !m.Initialized && !m.CloneReplacesData && !s.Formation.reforming()}
	sp.SlowShutdown = m.SlowShutdown
	sp.Admin = credential("admin")
	for _, peer := range s.data() {
		sp.Peers = append(sp.Peers, myspec.Peer{Name: peer.Name, Host: peer.Name, Port: 3306})
	}
	desired := []string{"mysqld"}
	if m.Role == "data" && s.Topology == "group" {
		g := &myspec.GroupReplication{GroupName: s.Formation.GroupName, FormationEpoch: s.Formation.FormationEpoch, Seeds: sp.Peers, Consistency: o.GroupReplication.Consistency, UnreachableMajorityTimeoutSeconds: o.GroupReplication.UnreachableMajorityTimeout, AutorejoinTries: 3, MemberWeight: 50}
		g.Bootstrap = name == s.Formation.Seed && s.Formation.State == "bootstrapping" && (s.Formation.Adopting || s.Formation.reforming() || !m.Initialized)
		// The seed can be formed while restored followers still await cloning.
		// Destructive permission belongs to that member's outstanding receipt.
		if m.CloneReplacesData && !m.Initialized && !m.CloneDone && m.Clone != nil {
			g.Reclone, g.RecloneReceipt = true, m.Clone.Receipt
		}
		if name != s.Formation.Seed && !m.Initialized {
			for _, peer := range s.data() {
				st, e := live(ctx, rc, peer.Name)
				if e == nil && onlinePrimary(st) {
					g.CloneDonor = &myspec.Peer{Name: peer.Name, Host: peer.Name, Port: 3306}
					break
				}
			}
		}
		sp.GroupReplication = g
	}
	if m.Role == "data" && s.Topology == "cluster" {
		g := &myspec.Galera{ClusterName: s.Formation.GroupName, FormationEpoch: s.Formation.FormationEpoch, GaleraPort: myspec.PortGalera, ISTPort: myspec.PortGaleraIST, SSTPort: myspec.PortGaleraSST, GCacheSize: "128M"}
		g.TrafficCert = &myspec.Certificate{CertPEM: get(galeraTrafficBase + ".crt"), KeyPEM: get(galeraTrafficBase + ".key")}
		g.TrafficCAPEM = get(galeraTrafficCA + ".crt")
		for _, peer := range s.data() {
			if !peer.Removing {
				g.Peers = append(g.Peers, myspec.Peer{Name: peer.Name, Host: peer.Name, Port: myspec.PortGalera})
			}
		}
		g.Bootstrap = name == s.Formation.Seed && s.Formation.State == "bootstrapping"
		if name == s.Formation.Seed {
			g.StartPosition = s.Formation.StartPosition
		}
		sp.Galera = g
	}
	if m.Role == "proxy" {
		desired = []string{"proxysql"}
		sp.Proxy = &myspec.Proxy{Monitor: credential("kk_monitor"), Admin: credential("kk_proxy_admin"), ReadWriteSplit: o.Proxy.ReadWriteSplit, ReaderPort: o.Proxy.ReaderPort, MaxConnections: 200}
		if s.Topology == "cluster" {
			sp.Proxy.Mode = "galera"
		}
		for _, peer := range s.data() {
			if peer.Stage == "joined" && !peer.Removing {
				sp.Proxy.Backends = append(sp.Proxy.Backends, myspec.Peer{Name: peer.Name, Host: peer.Name, Port: 3306})
			}
		}
		users, e := p.proxyUsers(ctx, rc)
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		sp.Proxy.Users = users
	}
	if m.Role == "data" && rc.Cluster.Spec.Backup.Storage != "" {
		bs, e := loadBackupState(rc.Cluster)
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		storage, e := p.backupStorage(ctx, rc, "", rc.Cluster.Ref().Namespace)
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		repo, e := repository(storage, repoPrefix(storage, rc.Cluster, s, bs.Generation))
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		activeRepository := bs.RestoreOperation == nil
		if bs.RestoreOperation != nil {
			var cp backupCheckpoint
			if e := json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); e != nil {
				return agentapi.MemberConfig{}, e
			}
			activeRepository = cp.Step == "activate" && cp.Generation == bs.Generation
		}
		enabled := rc.Cluster.Spec.Backup.PITR.Enabled && activeRepository && (rc.Cluster.Spec.DataSource == nil || bs.CloneComplete)
		sp.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: enabled, RotateIntervalSeconds: o.Binlog.ArchiveInterval, ExpireSeconds: o.Binlog.ExpireSeconds}}
	}
	if m.Offline {
		desired = []string{}
	}
	agentCert := cert("agents")
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	raw, err := json.Marshal(sp)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	var volume *agentapi.VolumeIdentity
	if m.Role == "data" && m.VolumeID != "" {
		volume = &agentapi.VolumeIdentity{ClusterUID: s.UID, Member: m.Name, Volume: m.Volume, VolumeID: m.VolumeID, Initialized: m.Initialized}
	}
	return agentapi.MemberConfig{Cluster: rc.Cluster.Name, Member: name, Engine: "mysql", EngineSpec: raw, Role: m.Role, Port: 3306, EngineVersion: version, ConfigVersion: configVersion, DesiredProcesses: desired, Volume: volume, AgentTLS: agentapi.Certificate{CertPEM: agentCert.CertPEM, KeyPEM: agentCert.KeyPEM}}, nil
}
