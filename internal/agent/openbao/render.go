// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"path/filepath"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baospec"
)

func ports(s baospec.MemberSpec) (int, int, int) {
	a, c, m := s.APIPort, s.ClusterPort, s.MetricsPort
	if a == 0 {
		a = baospec.APIPort
	}
	if c == 0 {
		c = baospec.ClusterPort
	}
	if m == 0 {
		m = baospec.MetricsPort
	}
	return a, c, m
}
func spec(c agentapi.MemberConfig) (baospec.MemberSpec, error) {
	var s baospec.MemberSpec
	if json.Unmarshal(c.EngineSpec, &s) != nil {
		return s, errors.New("invalid openbao member specification")
	}
	if s.NodeID == "" || s.NodeID != c.Member || !filepath.IsAbs(s.BinaryPath) || net.ParseIP(s.ListenAddress) == nil {
		return s, errors.New("invalid openbao member identity, binary or listener")
	}
	for _, v := range []string{s.APIAddr, s.ClusterAddr} {
		u, e := url.Parse(v)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return s, errors.New("invalid openbao advertised address")
		}
	}
	a, b, d := ports(s)
	if a < 1 || a > 65535 || b < 1 || b > 65535 || d < 1 || d > 65535 || a == b || a == d || b == d {
		return s, errors.New("invalid openbao ports")
	}
	recovery := s.RecoveryPort
	if recovery == 0 {
		recovery = baospec.RecoveryPort
	}
	if recovery < 1 || recovery > 65535 || recovery == a || recovery == b || recovery == d {
		return s, errors.New("invalid openbao recovery port")
	}
	if c.TLS.CAPEM == "" || c.TLS.CertPEM == "" || c.TLS.KeyPEM == "" || c.TLS.Mode == "disabled" {
		return s, errors.New("openbao requires TLS")
	}
	if s.Storage.Type != baospec.StorageRaft && (s.Storage.Type != baospec.StoragePostgreSQL || s.Storage.PostgreSQL == nil) {
		return s, errors.New("invalid openbao storage")
	}
	switch s.Seal.Type {
	case "", baospec.SealShamir:
	case baospec.SealTransit:
		if s.Seal.Transit == nil {
			return s, errors.New("missing transit seal")
		}
	case baospec.SealAWSKMS:
		if s.Seal.AWSKMS == nil {
			return s, errors.New("missing awskms seal")
		}
	default:
		return s, errors.New("invalid openbao seal")
	}
	for _, p := range s.Plugins {
		if !filepath.IsAbs(s.PluginDirectory) || !filepath.IsLocal(p.Command) || filepath.Base(p.Command) != p.Command || p.Name == "" || (p.Type != "database" && p.Type != "kms") {
			return s, errors.New("invalid openbao plugin")
		}
	}
	switch s.LogLevel {
	case "", "trace", "debug", "info", "warn", "error":
	default:
		return s, errors.New("invalid openbao log level")
	}
	return s, nil
}
func render(dir string, s baospec.MemberSpec, c agentapi.MemberConfig) (map[string][]byte, error) {
	base := filepath.Join(dir, "karakoram", "openbao")
	path := func(n string) string { return filepath.Join(base, n) }
	files := map[string][]byte{"openbao/ca.pem": []byte(c.TLS.CAPEM), "openbao/server.pem": []byte(c.TLS.CertPEM), "openbao/server.key": []byte(c.TLS.KeyPEM)}
	a, cluster, m := ports(s)
	listener := func(addr string) map[string]any {
		return map[string]any{"address": addr, "tls_cert_file": path("server.pem"), "tls_key_file": path("server.key"), "tls_min_version": "tls12"}
	}
	metrics := listener(net.JoinHostPort("127.0.0.1", strconv.Itoa(m)))
	metrics["cluster_address"] = "127.0.0.1:0"
	metrics["telemetry"] = map[string]any{"unauthenticated_metrics_access": true}
	api := listener(net.JoinHostPort(s.ListenAddress, strconv.Itoa(a)))
	api["cluster_address"] = net.JoinHostPort(s.ListenAddress, strconv.Itoa(cluster))
	api["disable_unauthed_generate_root_endpoints"] = true
	metrics["disable_unauthed_generate_root_endpoints"] = true
	cfg := map[string]any{"api_addr": s.APIAddr, "cluster_addr": s.ClusterAddr, "listener": []any{map[string]any{"tcp": api}, map[string]any{"tcp": metrics}}, "telemetry": map[string]any{"prometheus_retention_time": "60s", "disable_hostname": true}, "log_level": s.LogLevel}
	if s.RecoveryCAPEM != "" {
		recoveryPort := s.RecoveryPort
		if recoveryPort == 0 {
			recoveryPort = baospec.RecoveryPort
		}
		recovery := listener(net.JoinHostPort(s.ListenAddress, strconv.Itoa(recoveryPort)))
		recovery["cluster_address"] = "127.0.0.1:0"
		recovery["tls_require_and_verify_client_cert"] = true
		recovery["tls_client_ca_file"] = path("recovery-ca.pem")
		// OpenBao >=2.5.3 disables this deprecated shares-based workflow
		// by default. Enable it only behind independent recovery-client mTLS.
		recovery["disable_unauthed_generate_root_endpoints"] = false
		files["openbao/recovery-ca.pem"] = []byte(s.RecoveryCAPEM)
		cfg["listener"] = append(cfg["listener"].([]any), map[string]any{"tcp": recovery})
	}
	if s.LogLevel == "" {
		cfg["log_level"] = "info"
	}
	if s.Storage.Type == baospec.StorageRaft {
		st := map[string]any{"path": filepath.Join(dir, "openbao", "raft"), "node_id": s.NodeID}
		joins := []any{}
		for _, j := range s.Storage.RetryJoin {
			v := map[string]any{"leader_api_addr": j.LeaderAPIAddr, "leader_ca_cert_file": path("ca.pem")}
			if j.TLSServerName != "" {
				v["leader_tls_servername"] = j.TLSServerName
			}
			joins = append(joins, v)
		}
		if len(joins) > 0 {
			st["retry_join"] = joins
		}
		cfg["storage"] = map[string]any{"raft": st}
	} else {
		p := s.Storage.PostgreSQL
		u, e := url.Parse(p.ConnectionURL)
		if e != nil {
			return nil, errors.New("invalid postgresql storage URL")
		}
		if p.CAPEM != "" {
			files["openbao/postgres-ca.pem"] = []byte(p.CAPEM)
			q := u.Query()
			q.Set("sslrootcert", path("postgres-ca.pem"))
			u.RawQuery = q.Encode()
		}
		cfg["storage"] = map[string]any{"postgresql": map[string]any{"connection_url": u.String(), "table": p.Table, "ha_table": p.HATable, "ha_enabled": true}}
	}
	if s.Seal.Type == baospec.SealTransit {
		p := s.Seal.Transit
		v := map[string]any{"address": p.Address, "mount_path": p.MountPath, "key_name": p.KeyName, "token": p.Token, "tls_server_name": p.TLSServerName}
		if p.CAPEM != "" {
			files["openbao/transit-ca.pem"] = []byte(p.CAPEM)
			v["tls_ca_cert"] = path("transit-ca.pem")
		}
		cfg["seal"] = map[string]any{"transit": v}
	}
	if s.Seal.Type == baospec.SealAWSKMS {
		p := s.Seal.AWSKMS
		cfg["seal"] = map[string]any{"awskms": map[string]any{"region": p.Region, "kms_key_id": p.KMSKeyID, "endpoint": p.Endpoint, "access_key": p.AccessKey, "secret_key": p.SecretKey}}
	}
	if s.PluginDirectory != "" {
		cfg["plugin_directory"] = s.PluginDirectory
	}
	if len(s.Plugins) > 0 {
		plugins := []any{}
		for _, p := range s.Plugins {
			v := map[string]any{"command": p.Command}
			if p.Version != "" {
				v["version"] = p.Version
			}
			plugins = append(plugins, map[string]any{p.Type: map[string]any{p.Name: v}})
		}
		cfg["plugin"] = plugins
	}
	for i, p := range s.TrustCAPEMs {
		files["openbao/trust/"+strconv.Itoa(i)+".pem"] = []byte(p)
	}
	data, e := json.MarshalIndent(cfg, "", "  ")
	if e != nil {
		return nil, e
	}
	files["openbao/server.json"] = data
	return files, nil
}
