// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// PrepareCommands installs only pinned native packages and suppresses distro daemons.
func PrepareCommands(version string) []string {
	m := major(version)
	bootstrap := nativeartifacts.Artifact{
		Name: "percona-release", File: "percona-release.deb",
		SHA256: "10a419065480e4cd4ad6d1036e742832203e79ff537461115361ec2f7ff62e36",
	}
	return []string{
		"sudo apt-get update && sudo apt-get install -y ca-certificates curl gnupg lsb-release",
		nativeartifacts.PrepareCommands(bootstrap, "https://repo.percona.com/apt/percona-release_latest.noble_all.deb", "", "")[0],
		fmt.Sprintf("sudo percona-release setup -y ppg-%d", m),
		"printf '#!/bin/sh\\nexit 101\\n' | sudo tee /usr/sbin/policy-rc.d >/dev/null && sudo chmod 0755 /usr/sbin/policy-rc.d && sudo apt-get install -y postgresql-common && printf 'create_main_cluster = false\\n' | sudo tee /etc/postgresql-common/createcluster.conf >/dev/null",
		fmt.Sprintf("sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y percona-postgresql-%d='%s' percona-postgresql-contrib='1:293-1.noble' percona-patroni='1:4.1.5-1.noble' percona-pgbackrest='1:2.59.1-1.noble' etcd-server='1:3.5.33-1.noble' etcd-client='1:3.5.33-1.noble' python3-etcd3gw python3-psycopg2", m, packagePins[version]),
	}
}
func (p *Core) runtimeSpec(rc *engine.Context, m *member) (infra.RuntimeSpec, error) {
	prepare := PrepareCommands(rc.Cluster.Spec.Version)
	s, err := load(rc.Cluster)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	if m.Role == "data" && s.Upgrade != nil && s.Upgrade.Stage != "finalize" {
		// Install the old release last: shared client wrappers stay compatible
		// until the offline upgrade switches the data directory to the new major.
		prepare = append(prepare, PrepareCommands(s.Upgrade.From)...)
	}
	if m.Role == "proxy" {
		a, err := nativeartifacts.Get("proxysql")
		if err != nil {
			return infra.RuntimeSpec{}, err
		}
		u, err := nativeartifacts.ResolveURL(a)
		if err != nil {
			return infra.RuntimeSpec{}, err
		}
		prepare = []string{
			"sudo apt-get update && sudo apt-get install -y ca-certificates curl",
			"printf '#!/bin/sh\\nexit 101\\n' | sudo tee /usr/sbin/policy-rc.d >/dev/null && sudo chmod 0755 /usr/sbin/policy-rc.d",
		}
		prepare = append(prepare, nativeartifacts.PrepareCommands(a, u, "/opt/karakoram", rc.Backend.Layout().User)...)
		// The package enables its systemd unit directly, bypassing policy-rc.d.
		// Only the member agent may supervise the proxy and own its listeners.
		prepare = append(prepare, "if [ -d /run/systemd/system ]; then sudo systemctl disable --now proxysql.service && sudo systemctl mask proxysql.service; fi")
	} else if p.options.Flavour != nil {
		extra, err := p.options.Flavour.PrepareCommands(rc.Backend.Layout(), major(rc.Cluster.Spec.Version))
		if err != nil {
			return infra.RuntimeSpec{}, err
		}
		prepare = append(prepare, extra...)
	}
	ports := []infra.Port{{Port: 5432, Protocol: "tcp"}, {Port: 9443, Protocol: "tcp"}}
	if m.Role == "data" && rc.Cluster.Spec.Topology.Type == "ha" {
		for _, port := range []int{8008, 2379, 2380} {
			ports = append(ports, infra.Port{Port: port, Protocol: "tcp"})
		}
	}
	o, err := p.parseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return infra.RuntimeSpec{}, err
	}
	dataRead, _ := engine.DataAccessLevels(rc.Cluster)
	if m.Role == "proxy" && (o.Proxy.ReaderPort || dataRead) {
		ports = append(ports, infra.Port{Port: 5433, Protocol: "tcp"})
	}
	agentURL := rc.AgentURL
	if agentURL == "" {
		agentURL = p.options.ControlPlaneURL
	}
	runtime := engine.AgentRuntime{
		Cluster: rc.Cluster.Name, Member: m.Name, Engine: "postgres", Volume: m.Volume,
		AgentURL: agentURL, InternalCAPEM: p.options.InternalCAPEM,
		Ports: ports, Prepare: prepare,
	}
	if m.Role == "proxy" {
		runtime.ReadinessArgs = engine.StatelessReadiness(rc, m.Name, &m.NativeReadiness, "--engine=postgres", "--data-dir="+rc.Backend.Layout().DataDir(m.Volume))
	}
	return runtime.Spec(rc.Backend.Layout()), nil
}

// MemberConfig renders secret-bearing configuration only for an allocated member.
func (p *Core) MemberConfig(ctx context.Context, rc *engine.Context, name string) (agentapi.MemberConfig, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	m := s.find(name)
	if m == nil {
		return agentapi.MemberConfig{}, engine.ErrNotFound
	}
	o, err := p.parseOptions(m.Options)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	if m.Role == "data" && s.Rolling != nil && s.Rolling.Phase < len(s.Rolling.Phases) {
		o, err = ParseOptions(s.Rolling.Phases[s.Rolling.Phase].Options)
		if err != nil {
			return agentapi.MemberConfig{}, err
		}
	}
	get := func(k string) string {
		if err != nil {
			return ""
		}
		var b []byte
		b, err = secret(ctx, rc, k)
		return string(b)
	}
	cert := func(kind string) *pgspec.Certificate {
		return &pgspec.Certificate{CertPEM: get("tls/" + kind + "/" + name + ".crt"), KeyPEM: get("tls/" + kind + "/" + name + ".key")}
	}
	credential := func(name string) pgspec.Credentials {
		return pgspec.Credentials{Username: name, Password: get("users/" + name)}
	}
	version := m.Version
	if version == "" {
		version = rc.Cluster.Spec.Version
	}
	sp := pgspec.Spec{
		Role: m.Role, Topology: m.Topology, Flavour: "postgres",
		Scope: rc.Cluster.Name + "-" + s.UID,
		Major: major(version), BinDir: fmt.Sprintf("/usr/lib/postgresql/%d/bin", major(version)),
		Version: version, Member: name,
		Retiring: retirementTagsDesired(m),
		CAPEM:    get("tls/ca.crt"), ServerCert: cert("servers"),
	}
	sp.ListenHost = "::"
	if rc.Backend.Layout().PrivateAddressFamily == infra.AddressFamilyIPv4 {
		sp.ListenHost = "0.0.0.0"
	}
	if sp.Topology == "" {
		sp.Topology = rc.Cluster.Spec.Topology.Type
	}
	if p.options.Flavour != nil {
		sp.Flavour = p.options.Flavour.Name()
	}
	agentCert := cert("agents")
	for _, peer := range s.data() {
		if m.Role == "proxy" {
			// Planned and deploying identities may not have a resolvable
			// runtime yet. Publishing them can block ProxySQL's shared
			// monitor on DNS and remove an unrelated healthy writer.
			joined := peer.Stage == "joined" || peer.Stage == "deployed" && peer.ProxyReady
			if peer.Removing || !joined || peer.ServiceID == "" {
				continue
			}
			runtime, err := rc.Backend.GetService(ctx, peer.Name)
			if err != nil {
				return agentapi.MemberConfig{}, err
			}
			if runtime == nil || runtime.Status != infra.StatusRunning && runtime.Status != infra.StatusActive {
				continue
			}
		}
		etcd := ""
		if sp.Topology == "ha" {
			etcd = "voter"
			if peer.EtcdLearner {
				etcd = "learner"
			}
		}
		sp.Peers = append(sp.Peers, pgspec.Peer{Name: peer.Name, Host: peer.Name, Etcd: etcd})
	}
	desired := []string{"postgres"}
	if m.Role == "data" {
		sp.Superuser = credential("postgres")
		sp.Replication = credential("kk_replicator")
		sp.Rewind = credential("kk_rewind")
		sp.Monitor = credential("kk_monitor")
		sp.DataAccess, err = p.dataRoles(ctx, rc)
		if err != nil {
			return agentapi.MemberConfig{}, err
		}
		sp.PatroniREST = credential("kk_admin")
		cidrs := []string{"::/0"}
		if rc.Backend.Layout().PrivateAddressFamily == infra.AddressFamilyIPv4 {
			cidrs = []string{"0.0.0.0/0"}
		}
		sp.Postgres = &pgspec.Postgres{Parameters: o.Parameters, RequireTLS: rc.Cluster.Spec.TLS.Mode == "requireTLS", ClientCIDRs: cidrs, Synchronous: o.SynchronousMode}
		if p.options.Flavour != nil {
			if sp.Postgres.Parameters == nil {
				sp.Postgres.Parameters = map[string]string{}
			}
			for k, v := range p.options.Flavour.Parameters() {
				sp.Postgres.Parameters[k] = v
			}
			sp.Postgres.SharedPreloadLibraries = p.options.Flavour.SharedPreloadLibraries()
		}
		sp.Bootstrap = &pgspec.Bootstrap{Method: "initdb", AllowInitDB: !s.Formed && !engine.InitialDataSourceUnproven(rc.Cluster), Generation: s.BootstrapGeneration}
		if !s.Formed {
			sp.Bootstrap.SeedMember = s.ConversionSeed
		}
		if sp.Topology == "ha" {
			entries := []string{}
			for _, peer := range sp.Peers {
				entries = append(entries, peer.Name+"=https://"+peer.Host+":2380")
			}
			sp.Etcd = &pgspec.Etcd{InitialCluster: strings.Join(entries, ","), State: m.EtcdState, Token: s.UID}
			if m.InitialCluster != "" {
				sp.Etcd.InitialCluster = m.InitialCluster
			}
			desired = []string{"etcd", "patroni"}
		}
		if storageName := rc.Cluster.Spec.Backup.Storage; storageName != "" {
			storage, err := p.backupStorage(ctx, rc, storageName, rc.Cluster.Ref().Namespace)
			if err != nil {
				return agentapi.MemberConfig{}, err
			}
			repo, err := repository(storage, repoPrefix(storage, rc.Cluster, s))
			if err != nil {
				return agentapi.MemberConfig{}, err
			}
			timeout := 60
			if rc.Cluster.Spec.Backup.PITR.Enabled && rc.Cluster.Spec.Backup.PITR.OplogSpanMinutes > 0 {
				timeout = rc.Cluster.Spec.Backup.PITR.OplogSpanMinutes * 60
			}
			sp.Backup = &pgspec.Backup{Repository: repo, ArchiveTimeoutSeconds: timeout}
		}
		if err := p.configureBackup(ctx, rc, s, &sp); err != nil {
			return agentapi.MemberConfig{}, err
		}
	} else {
		maxConnections, e := p.proxyConnectionLimit(rc, s, m)
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		desired = []string{"proxysql"}
		sp.Proxy = &pgspec.Proxy{
			Monitor: credential("kk_monitor"), Admin: credential("kk_proxy_admin"),
			ReadWriteSplit: o.Proxy.ReadWriteSplit, ReaderPort: o.Proxy.ReaderPort,
			BackendTLS: true, Threads: 2, MaxConnections: maxConnections,
			Users: []pgspec.ProxyUser{{
				Username: "postgres", Password: get("users/postgres"),
				RequireTLS: rc.Cluster.Spec.TLS.Mode == "requireTLS",
			}},
		}
		dataRead, _ := engine.DataAccessLevels(rc.Cluster)
		sp.Proxy.ReaderPort = sp.Proxy.ReaderPort || dataRead
		roles, e := p.dataRoles(ctx, rc)
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		for _, role := range roles.Roles {
			if role.Enabled {
				sp.Proxy.Users = append(sp.Proxy.Users, pgspec.ProxyUser{Username: role.Username, Password: role.Password, RequireTLS: rc.Cluster.Spec.TLS.Mode == "requireTLS"})
			}
		}
		users, e := p.proxyUsers(ctx, rc)
		if e != nil {
			return agentapi.MemberConfig{}, e
		}
		sp.Proxy.Users = append(sp.Proxy.Users, users...)
		bs, err := loadBackupState(rc.Cluster)
		if err != nil {
			return agentapi.MemberConfig{}, err
		}
		if bs.ManagedRolesHeld {
			sp.Proxy.Users = nil
		}
		for _, peer := range sp.Peers {
			sp.Proxy.Servers = append(sp.Proxy.Servers, peer.Host)
		}
		if rc.Cluster.Spec.TLS.Mode != "disabled" {
			sp.Proxy.ServerCert = sp.ServerCert
		}
	}
	if m.Role == "data" && m.Offline {
		desired = []string{}
		if sp.Topology == "ha" {
			desired = []string{"etcd"}
		}
	}
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	var volume *agentapi.VolumeIdentity
	if m.Role == "data" && m.VolumeID != "" {
		initialized := m.RuntimeRecovery.Initialized || m.Stage == "joined"
		if sp.Bootstrap != nil && sp.Bootstrap.Method == "restore" {
			// Only a durable restore generation authorizes writing replacement
			// data. Ordinary config pulls never adopt an empty initialized volume.
			initialized = false
		}
		volume = &agentapi.VolumeIdentity{ClusterUID: s.UID, Member: m.Name, Volume: m.Volume, VolumeID: m.VolumeID, Initialized: initialized}
	}
	raw, err := json.Marshal(sp)
	if err != nil {
		return agentapi.MemberConfig{}, err
	}
	configVersion := m.ConfigVersion
	if m.TLSReloadPending {
		configVersion = m.TLSReloadVersion
	}
	return agentapi.MemberConfig{
		Cluster: rc.Cluster.Name, Member: name, Engine: "postgres", EngineSpec: raw,
		Role: m.Role, Port: 5432, EngineVersion: version,
		ConfigVersion: configVersion, DesiredProcesses: desired,
		Volume:                volume,
		AgentTLS:              agentapi.Certificate{CertPEM: agentCert.CertPEM, KeyPEM: agentCert.KeyPEM},
		CertificateReloadOnly: m.TLSReloadOnly,
	}, nil
}
