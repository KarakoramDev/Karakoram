// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package postgres implements the native PostgreSQL member runner.
package postgres

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

var identifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]{0,62}$`)
var gucName = regexp.MustCompile(`^[a-z][a-z0-9_.]{0,62}$`)
var dnsLabel = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9-]*$`)

func decodeSpec(c agentapi.MemberConfig) (pgspec.Spec, error) {
	var s pgspec.Spec
	if len(c.EngineSpec) > 1<<20 {
		return s, errors.New("postgres payload exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(c.EngineSpec))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return s, fmt.Errorf("postgres payload: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return s, errors.New("postgres payload must contain one object")
	}
	return s, validate(s)
}
func safeValue(s string, max int) bool { return len(s) <= max && !strings.ContainsAny(s, "\r\n\x00") }
func validHost(h string) bool {
	if net.ParseIP(h) != nil {
		return true
	}
	if len(h) == 0 || len(h) > 253 {
		return false
	}
	for _, p := range strings.Split(h, ".") {
		if len(p) == 0 || len(p) > 63 || !dnsLabel.MatchString(p) {
			return false
		}
	}
	return true
}
func credentials(c pgspec.Credentials) bool {
	return identifier.MatchString(c.Username) && len(c.Password) > 0 && safeValue(c.Password, 1024)
}

func usesPatroni(s pgspec.Spec) bool {
	return s.Topology == pgspec.TopologyHA || s.Topology == pgspec.TopologyDistributed
}
func validate(s pgspec.Spec) error {
	if s.Role != pgspec.RoleData && s.Role != pgspec.RoleProxy {
		return errors.New("invalid postgres member role")
	}
	if s.Topology != pgspec.TopologySingle && !usesPatroni(s) {
		return errors.New("invalid postgres topology")
	}
	if s.Role == pgspec.RoleData && s.Topology == pgspec.TopologyDistributed && s.Citus == nil || s.Citus != nil && s.Topology != pgspec.TopologyDistributed {
		return errors.New("distributed data members require Citus configuration")
	}
	if s.Citus != nil && (s.Citus.Group < 0 || !validSQLName(s.Citus.Database)) {
		return errors.New("invalid Citus group or database")
	}
	if s.ListenHost != "" && s.ListenHost != "0.0.0.0" && s.ListenHost != "::" {
		return errors.New("invalid PostgreSQL listener address")
	}
	if !identifier.MatchString(s.Scope) || !identifier.MatchString(s.Member) {
		return errors.New("invalid scope or member")
	}
	if s.Flavour != "" && s.Flavour != pgspec.FlavourPostgres && s.Flavour != pgspec.FlavourSupabase {
		return errors.New("invalid postgres flavour")
	}
	if s.Major < 16 || s.Major > 18 || !filepath.IsAbs(s.BinDir) || !safeValue(s.BinDir, 512) {
		return errors.New("invalid PostgreSQL binary directory or major")
	}
	if len(s.Peers) > 16 {
		return errors.New("too many postgres peers")
	}
	seen := map[string]bool{}
	for _, p := range s.Peers {
		if !identifier.MatchString(p.Name) || !validHost(p.Host) || seen[p.Name] {
			return errors.New("invalid or duplicate postgres peer")
		}
		seen[p.Name] = true
	}
	if s.ServerCert != nil {
		if _, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM)); err != nil {
			return errors.New("invalid postgres TLS certificate")
		}
	}
	if s.Role == pgspec.RoleProxy {
		p := s.Proxy
		if p == nil || !credentials(p.Admin) || !credentials(p.Monitor) || len(p.Servers) > 16 || len(p.Users) > 1000 || strings.ContainsAny(p.Admin.Password, ":;") {
			return errors.New("invalid proxy configuration")
		}
		if p.BackendTLS && !x509.NewCertPool().AppendCertsFromPEM([]byte(s.CAPEM)) {
			return errors.New("proxy backend TLS requires a valid cluster CA")
		}
		for _, h := range p.Servers {
			if !validHost(h) {
				return errors.New("invalid proxy server")
			}
		}
		seen = map[string]bool{}
		for _, u := range p.Users {
			if !validSQLName(u.Username) || u.Password == "" || !safeValue(u.Password, 1024) || seen[u.Username] {
				return errors.New("invalid or duplicate proxy user")
			}
			seen[u.Username] = true
			if u.RequireTLS && p.ServerCert == nil {
				return errors.New("proxy TLS user requires certificate")
			}
		}
		if p.Threads < 0 || p.Threads > 128 || p.MaxConnections < 0 || p.MaxConnections > 1000000 {
			return errors.New("invalid proxy resource settings")
		}
		if p.ServerCert != nil {
			if _, err := tls.X509KeyPair([]byte(p.ServerCert.CertPEM), []byte(p.ServerCert.KeyPEM)); err != nil {
				return errors.New("invalid proxy certificate")
			}
		}
		return nil
	}
	if s.Postgres == nil || !credentials(s.Superuser) || !credentials(s.Replication) || !credentials(s.Rewind) || !credentials(s.Monitor) {
		return errors.New("postgres settings and system credentials required")
	}
	users := map[string]bool{}
	for _, c := range []pgspec.Credentials{s.Superuser, s.Replication, s.Rewind, s.Monitor} {
		if users[c.Username] {
			return errors.New("system role names must be distinct")
		}
		users[c.Username] = true
	}
	p := s.Postgres
	if p.PgBackRestReplicas && s.Backup == nil {
		return errors.New("pgBackRest replicas require a backup repository")
	}
	if p.Synchronous != "" && p.Synchronous != pgspec.SyncOff && p.Synchronous != pgspec.SyncOn && p.Synchronous != pgspec.SyncStrict {
		return errors.New("invalid synchronous mode")
	}
	if len(p.Parameters) > 128 || len(p.ClientCIDRs) > 64 || len(p.SharedPreloadLibraries) > 32 {
		return errors.New("postgres settings exceed limit")
	}
	for k, v := range p.Parameters {
		if !gucName.MatchString(k) || !safeValue(v, 4096) {
			return errors.New("invalid postgres parameter")
		}
	}
	for _, v := range p.SharedPreloadLibraries {
		if !gucName.MatchString(v) {
			return errors.New("invalid preload library")
		}
	}
	for _, v := range p.ClientCIDRs {
		if _, _, err := net.ParseCIDR(v); err != nil {
			return errors.New("invalid client CIDR")
		}
	}
	if p.RequireTLS && s.ServerCert == nil {
		return errors.New("postgres TLS requires certificate")
	}
	if s.Bootstrap != nil && s.Bootstrap.SeedMember != "" && (!usesPatroni(s) || !seen[s.Bootstrap.SeedMember]) {
		return errors.New("bootstrap seed must be an HA data member")
	}
	if usesPatroni(s) {
		if !seen[s.Member] || s.Etcd == nil || !credentials(s.PatroniREST) || s.ServerCert == nil || len(s.Etcd.Endpoints) == 0 && ((s.Etcd.State != "new" && s.Etcd.State != "existing") || !safeValue(s.Etcd.Token, 255) || s.Etcd.Token == "") {
			return errors.New("invalid HA configuration")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(s.CAPEM)) {
			return errors.New("HA CA required")
		}
		for _, endpoint := range s.Etcd.Endpoints {
			host, port, err := net.SplitHostPort(endpoint)
			if err != nil || !validHost(host) || port != "2379" {
				return errors.New("invalid etcd client endpoint")
			}
		}
		parts := []string{}
		if len(s.Etcd.Endpoints) == 0 {
			parts = strings.Split(s.Etcd.InitialCluster, ",")
		}
		if len(parts) > 16 || len(s.Etcd.Endpoints) > 16 {
			return errors.New("invalid initial etcd cluster")
		}
		for _, v := range parts {
			name, address, ok := strings.Cut(v, "=")
			if !ok || !identifier.MatchString(name) || !strings.HasPrefix(address, "https://") {
				return errors.New("invalid initial etcd peer")
			}
			h, p, err := net.SplitHostPort(strings.TrimPrefix(address, "https://"))
			if err != nil || p != "2380" || !validHost(h) {
				return errors.New("invalid initial etcd peer URL")
			}
		}
	}
	if s.Bootstrap != nil {
		b := s.Bootstrap
		if b.Generation < 0 || (b.Method != "" && b.Method != "initdb" && b.Method != "restore") {
			return errors.New("invalid bootstrap method")
		}
		if recovery := b.RecoverySuperuser; recovery != nil && (b.Method != "restore" || s.Topology != pgspec.TopologySingle || !credentials(*recovery) || recovery.Username != s.Superuser.Username) {
			return errors.New("invalid temporary recovery credential")
		}
		if b.Method == "restore" {
			if b.Restore == nil {
				return errors.New("restore source required")
			}
			if err := validateRepository(b.Restore.Source); err != nil {
				return err
			}
			if !safeValue(b.Restore.Set, 128) || !safeValue(b.Restore.TargetTime, 64) || !safeValue(b.Restore.TargetName, 63) || b.Restore.TargetTime != "" && b.Restore.TargetName != "" {
				return errors.New("invalid restore target")
			}
		}
	}
	if s.Backup != nil {
		if err := validateRepository(s.Backup.Repository); err != nil {
			return err
		}
		for _, repository := range s.Backup.AdditionalRepositories {
			if err := validateRepository(repository); err != nil {
				return err
			}
			if repository.Stanza != s.Backup.Repository.Stanza {
				return errors.New("archive repositories must use the same stanza")
			}
		}
		if s.Backup.ArchiveTimeoutSeconds < 0 || s.Backup.ArchiveTimeoutSeconds > 86400 {
			return errors.New("invalid archive timeout")
		}
	}
	return nil
}
func validateRepository(r pgspec.Repository) error {
	if !identifier.MatchString(r.Stanza) || !strings.HasPrefix(r.Path, "/") || strings.Contains(r.Path, "..") || r.S3.Port < 0 || r.S3.Port > 65535 {
		return errors.New("invalid backup repository")
	}
	for _, v := range []string{r.Path, r.S3.Endpoint, r.S3.Bucket, r.S3.Region, r.S3.AccessKey, r.S3.SecretKey} {
		if v == "" || !safeValue(v, 1024) {
			return errors.New("invalid backup repository setting")
		}
	}
	return nil
}
