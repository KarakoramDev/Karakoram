// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package wvspec

import (
	"encoding/json"
	"strings"
	"testing"
)

func validSpec() MemberSpec {
	return MemberSpec{Version: "1.40.1", NodeName: "wv-0", Ports: Ports{8080, 50051, 7946, 7947, 8300, 8301, 2112}, BindHost: "127.0.0.1", RaftJoin: []string{"wv-0:8301"}, BootstrapExpect: 1, RootUser: "karakoram-admin", AllowedKeys: []string{"secret-one"}, ClusterUsername: "cluster", ClusterPassword: "secret-two", Modules: []string{"backup-s3"}, ReplicationMinimumFactor: 1, AsyncReplication: true, QueryDefaultsLimit: 25, DiskWarningPercent: 80, DiskReadonlyPercent: 90, MemoryLimitMiB: 870, GOMAXPROCS: 1}
}
func TestValidate(t *testing.T) {
	for name, change := range map[string]func(*MemberSpec){
		"version":          func(s *MemberSpec) { s.Version = "../../bin" },
		"minor":            func(s *MemberSpec) { s.Version = "1.38.1" },
		"name":             func(s *MemberSpec) { s.NodeName = "bad,node" },
		"bind":             func(s *MemberSpec) { s.BindHost = "localhost" },
		"duplicate ports":  func(s *MemberSpec) { s.Ports.GRPC = s.Ports.HTTP },
		"low port":         func(s *MemberSpec) { s.Ports.HTTP = 1023 },
		"high port":        func(s *MemberSpec) { s.Ports.HTTP = 65536 },
		"bootstrap":        func(s *MemberSpec) { s.BootstrapExpect = 3 },
		"join":             func(s *MemberSpec) { s.RaftJoin = []string{"wv-0"} },
		"keys":             func(s *MemberSpec) { s.AllowedKeys = []string{"same", "same"} },
		"key injection":    func(s *MemberSpec) { s.AllowedKeys = []string{"a,b"} },
		"root":             func(s *MemberSpec) { s.RootUser = "" },
		"cluster auth":     func(s *MemberSpec) { s.ClusterPassword = "" },
		"replication":      func(s *MemberSpec) { s.ReplicationMinimumFactor = 10 },
		"query":            func(s *MemberSpec) { s.QueryDefaultsLimit = 0 },
		"disk ordering":    func(s *MemberSpec) { s.DiskWarningPercent = 90 },
		"disk upper":       func(s *MemberSpec) { s.DiskReadonlyPercent = 96 },
		"memory":           func(s *MemberSpec) { s.MemoryLimitMiB = 0 },
		"cpu":              func(s *MemberSpec) { s.GOMAXPROCS = 0 },
		"module":           func(s *MemberSpec) { s.Modules = append(s.Modules, "unknown") },
		"backup module":    func(s *MemberSpec) { s.Modules = nil },
		"duplicate module": func(s *MemberSpec) { s.Modules = append(s.Modules, "backup-s3") },
		"enterprise env": func(s *MemberSpec) {
			s.InferenceEndpoints = map[string]string{"NAMESPACES_ENABLED": "https://example.com"}
		},
		"license env": func(s *MemberSpec) { s.InferenceEndpoints = map[string]string{"LICENSE_KEY": "https://example.com"} },
		"undeclared inference": func(s *MemberSpec) {
			s.InferenceEndpoints = map[string]string{"CLIP_INFERENCE_API": "https://example.com"}
		},
		"missing inference": func(s *MemberSpec) { s.Modules = append(s.Modules, "multi2vec-clip") },
	} {
		t.Run(name, func(t *testing.T) {
			s := validSpec()
			change(&s)
			if s.Validate() == nil {
				t.Fatal("accepted invalid spec")
			}
		})
	}
	s := validSpec()
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.AllowedKeys = append(s.AllowedKeys, "rotation-key")
	s.Version = "1.39.12"
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestInferenceURLs(t *testing.T) {
	for _, raw := range []string{"file:///tmp/model", "https://user:password@example.com", "https://example.com/#secret", "https://example.com/?key=secret", "http://example.com:65536", "https://", "http://example.com\n"} {
		s := validSpec()
		s.Modules = append(s.Modules, "multi2vec-clip")
		s.InferenceEndpoints = map[string]string{"CLIP_INFERENCE_API": raw}
		if s.Validate() == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	s := validSpec()
	s.Modules = append(s.Modules, "multi2vec-clip")
	s.InferenceEndpoints = map[string]string{"CLIP_INFERENCE_API": "http://127.0.0.1:9000/model"}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
}
func TestBackup(t *testing.T) {
	for _, endpoint := range []string{"https://example.com", "example.com/path", "user:pass@example.com", "example.com:70000", "example.com?secret=x"} {
		s := validSpec()
		s.Backup = &BackupS3{Bucket: "backups", Path: "prefix", Endpoint: endpoint, Region: "region", AccessKey: "access", SecretKey: "secret"}
		if s.Validate() == nil {
			t.Errorf("accepted %q", endpoint)
		}
	}
	s := validSpec()
	s.Backup = &BackupS3{Bucket: "backups", Path: "prefix/cluster", Endpoint: "s3.example.com:443", UseSSL: true, Region: "region", AccessKey: "access", SecretKey: "secret"}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	s.Backup.Path = "prefix/../other"
	if s.Validate() == nil {
		t.Fatal("accepted traversal")
	}
}
func TestWireAndPaths(t *testing.T) {
	s := validSpec()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var got MemberSpec
	if err = json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if err = got.Validate(); err != nil {
		t.Fatal(err)
	}
	if BinaryPath(s.Version) != "/opt/karakoram/weaviate-1.40.1/weaviate" {
		t.Fatal(BinaryPath(s.Version))
	}
	var r ReadinessResponse
	if err = json.Unmarshal([]byte(`{"ready":true,"configVersion":4,"version":"1.40.1"}`), &r); err != nil || !r.Ready || r.ConfigVersion != 4 {
		t.Fatal(r, err)
	}
	seen := map[string]bool{}
	for _, m := range MetricNames {
		if !strings.HasPrefix(m, "karakoram_weaviate_") || seen[m] {
			t.Fatal(m)
		}
		seen[m] = true
	}
}
