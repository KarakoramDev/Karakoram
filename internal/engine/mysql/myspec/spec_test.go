// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"encoding/json"
	"encoding/pem"
	"testing"
)

func validSpec() Spec {
	cert := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{1}}))
	key := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte{1}}))
	return Spec{Member: "samplem1", Role: RoleData, Topology: TopologySingle, Version: "8.4.11-11", ServerDir: "/opt/karakoram/percona-server-8.4.11-11", XtraBackupDir: "/opt/karakoram/percona-xtrabackup-8.4.0-7", BufferPoolBytes: 128 << 20, MaxConnections: 100, Superuser: Credentials{UserRoot, "secret"}, Agent: Credentials{UserAgent, "secret"}, Replication: Credentials{UserReplication, "secret"}, Monitor: Credentials{UserMonitor, "secret"}, BackupUser: Credentials{UserBackup, "secret"}, CAPEM: cert, ServerCert: &Certificate{cert, key}}
}
func validRepository() Repository {
	return Repository{S3: S3{Endpoint: "https://s3.example.test", Bucket: "backups", Region: "us-east-1", AccessKey: "key", SecretKey: "secret", PathStyle: true, VerifyTLS: true}, Path: "mysql/team/sample-uid"}
}
func TestSpec(t *testing.T) {
	s := validSpec()
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var got Spec
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if err = got.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestSpecRejectsInvalid(t *testing.T) {
	for name, mutate := range map[string]func(*Spec){
		"name": func(s *Spec) { s.Member = "bad/name" }, "role": func(s *Spec) { s.Role = "writer" }, "ordinal": func(s *Spec) { s.Ordinal = -1 }, "version": func(s *Spec) { s.Version = "9.0" }, "directory": func(s *Spec) { s.ServerDir = "/tmp/server" }, "key": func(s *Spec) { s.ServerCert.KeyPEM = "bad" }, "ca": func(s *Spec) { s.CAPEM = "bad" }, "account": func(s *Spec) { s.Agent.Username = "admin" }, "password": func(s *Spec) { s.Replication.Password = "" }, "protected": func(s *Spec) { s.Parameters = map[string]string{"gtid_mode": "OFF"} }, "port": func(s *Spec) { s.Peers = []Peer{{"member", "db.example.test", 65536}} }, "group missing": func(s *Spec) { s.Topology = TopologyGroup }, "connections": func(s *Spec) { s.MaxConnections = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			mutate(&s)
			if s.Validate() == nil {
				t.Fatal("accepted invalid spec")
			}
		})
	}
}
func TestGroupAndProxy(t *testing.T) {
	s := validSpec()
	s.Topology = TopologyGroup
	s.GroupReplication = &GroupReplication{GroupName: "11111111-2222-3333-4444-555555555555", FormationEpoch: 1, Seeds: []Peer{{"member", "db.example.test", 3306}}, Consistency: ConsistencyBeforeOnPrimaryFailover, UnreachableMajorityTimeoutSeconds: 30, AutorejoinTries: 3, MemberWeight: 50}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.GroupReplication.GroupName = "not-uuid"
	if s.Validate() == nil {
		t.Fatal("invalid UUID accepted")
	}
	s = validSpec()
	s.Role = RoleProxy
	s.Proxy = &Proxy{Backends: []Peer{{"member", "db.example.test", 3306}}, Monitor: Credentials{UserMonitor, "secret"}, Admin: Credentials{"admin", "secret"}, MaxConnections: 100, Users: []ProxyUser{{"app", "secret", true}}}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.Proxy.Users[0].Username = UserRoot
	if s.Validate() == nil {
		t.Fatal("system frontend accepted")
	}
}

func TestRestoreSpecRetainsMySQLUUIDValidation(t *testing.T) {
	for _, topology := range []string{TopologySingle, TopologyGroup} {
		t.Run(topology, func(t *testing.T) {
			s := validSpec()
			s.Topology = topology
			if topology == TopologyGroup {
				s.GroupReplication = &GroupReplication{GroupName: "11111111-2222-3333-4444-555555555555", FormationEpoch: 1, Seeds: []Peer{{"member", "db.example.test", 3306}}, Consistency: ConsistencyBeforeOnPrimaryFailover, UnreachableMajorityTimeoutSeconds: 30, AutorejoinTries: 3, MemberWeight: 50}
			}
			s.Restore = &Restore{Source: validRepository(), BackupID: "backup-1", Label: "full-1", ObjectPaths: []string{"mysql/full-1"}, ObjectManifests: []BackupManifestRef{{ObjectPrefix: "mysql/full-1", Digest: manifestTestDigest, Size: 1}}, NewGroupName: "22222222-3333-4444-5555-666666666666"}
			if err := s.Validate(); err != nil {
				t.Fatal(err)
			}
			s.Restore.NewGroupName = "22222222333344445555666666666666"
			if err := s.Validate(); err == nil {
				t.Fatal("non-PXC restore accepted a Galera-style name")
			}
		})
	}
}
func TestActions(t *testing.T) {
	repo := validRepository()
	if err := repo.Validate(); err != nil {
		t.Fatal(err)
	}
	request := BackupRequest{Receipt: "operation-1", Type: "full", Repository: repo, ObjectPrefix: "mysql/full-1"}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	request.Type = "incremental"
	if request.Validate() == nil {
		t.Fatal("missing base accepted")
	}
	request.BaseLSN = 100
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	r := RestoreRequest{Receipt: "operation-2", Restore: Restore{Source: repo, BackupID: "backup-1", Label: "full-1", ObjectPaths: []string{"mysql/full-1"}, ObjectManifests: []BackupManifestRef{{ObjectPrefix: "mysql/full-1", Digest: manifestTestDigest, Size: 1}}, TargetGTID: "11111111-2222-3333-4444-555555555555:1-42", NewGroupName: "22222222-3333-4444-5555-666666666666"}}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	r.Restore.TargetTime = "2026-10-21T00:00:00Z"
	if r.Validate() == nil {
		t.Fatal("ambiguous PITR target accepted")
	}
	r.Restore.TargetTime = ""
	r.Restore.ObjectPaths = []string{"../escape"}
	if r.Validate() == nil {
		t.Fatal("object traversal accepted")
	}
	if (SwitchoverRequest{TargetUUID: "invalid"}).Validate() == nil {
		t.Fatal("bad primary accepted")
	}
	if (Binlog{RotateIntervalSeconds: 300, ExpireSeconds: 300}).Validate() == nil {
		t.Fatal("unsafe expiry accepted")
	}
}
func TestParameters(t *testing.T) {
	for _, p := range []map[string]string{{"max_connections": "100"}, {"innodb_buffer_pool_size": "134217728"}, {"slow_query_log": "ON"}, {"long_query_time": "0.5"}} {
		if err := ValidateParameters(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []map[string]string{{"max_connections": "0"}, {"max_connections": "100\nplugin_load=x"}, {"long_query_time": "NaN"}, {"slow_query_log": "yes"}, {"transaction_isolation": "invalid"}, {"plugin_load": "evil"}, {"innodb_flush_log_at_trx_commit": "0"}} {
		if ValidateParameters(p) == nil {
			t.Fatalf("accepted %v", p)
		}
	}
}
