// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

func testSpec() wvspec.MemberSpec {
	return wvspec.MemberSpec{Version: "1.40.1", NodeName: "node00", BindHost: "127.0.0.1", Ports: wvspec.Ports{HTTP: 28080, GRPC: 25051, Gossip: 27946, Data: 27947, Raft: 28300, RaftRPC: 28301, Metrics: 22112}, RaftJoin: []string{"node00:28301"}, BootstrapExpect: 1, RootUser: "root", AllowedKeys: []string{"root-secret"}, ClusterUsername: "cluster", ClusterPassword: "cluster-secret", Modules: []string{"backup-s3"}, ReplicationMinimumFactor: 1, AsyncReplication: true, QueryDefaultsLimit: 25, DiskWarningPercent: 80, DiskReadonlyPercent: 90, MemoryLimitMiB: 256, GOMAXPROCS: 2}
}
func config(s wvspec.MemberSpec) agentapi.MemberConfig {
	raw, _ := json.Marshal(s)
	return agentapi.MemberConfig{Engine: wvspec.Engine, EngineSpec: raw, EngineVersion: s.Version, ConfigVersion: 1}
}
func TestRender(t *testing.T) {
	s := testSpec()
	c := config(s)
	p, files := render("/data", s, c)
	if !reflect.DeepEqual(p.Args, []string{"--host", "127.0.0.1", "--port", "28080", "--scheme", "http"}) || len(files) != 0 {
		t.Fatal(p.Args, files)
	}
	env := strings.Join(p.Env, "\n")
	for _, v := range []string{"PERSISTENCE_DATA_PATH=/data/weaviate", "AUTHENTICATION_APIKEY_ALLOWED_KEYS=root-secret", "AUTHENTICATION_APIKEY_USERS=root", "ENABLE_MODULES=", "GOMEMLIMIT=256MiB", "REPLICA_MOVEMENT_ENABLED=true"} {
		if !slices.Contains(p.Env, v) {
			t.Fatal("missing", v)
		}
	}
	for _, k := range wvspec.CommunityDisabledEnv {
		if !slices.Contains(p.Env, k+"=false") {
			t.Fatal(k)
		}
	}
	if strings.Contains(env, "LICENSE_KEY") || strings.Contains(strings.Join(p.Args, " "), "secret") {
		t.Fatal("secret or license leak")
	}
	s.Backup = &wvspec.BackupS3{Bucket: "bucket", Endpoint: "127.0.0.1:9000", Region: "us-east-1", AccessKey: "access", SecretKey: "backup-secret"}
	s.TLS = true
	s.AsyncReplication = false
	c.TLS = agentapi.TLSConfig{CertPEM: "cert", KeyPEM: "key", CAPEM: "ca"}
	p, files = render("/data", s, c)
	for _, v := range []string{"ENABLE_MODULES=backup-s3", "AWS_SECRET_ACCESS_KEY=backup-secret", "BACKUP_S3_USE_SSL=false", "ASYNC_REPLICATION_DISABLED=true", "GRPC_CERT_FILE=/data/karakoram/tls/weaviate.crt"} {
		if !slices.Contains(p.Env, v) {
			t.Fatal(v)
		}
	}
	if !reflect.DeepEqual(p.Args, []string{"--scheme", "https", "--tls-host", "127.0.0.1", "--tls-port", "28080", "--tls-certificate", "/data/karakoram/tls/weaviate.crt", "--tls-key", "/data/karakoram/tls/weaviate.key"}) || len(files) != 3 {
		t.Fatal(p.Args, files)
	}
}

type fakeHost struct {
	dir                  string
	states               map[string]agentapi.ProcessStatus
	maintenance, stopped bool
	actions              []string
	proc                 agent.ProcessSpec
}

func (h *fakeHost) DataDir() string { return h.dir }
func (h *fakeHost) WriteFile(name string, data []byte, mode os.FileMode) error {
	p := filepath.Join(h.dir, "karakoram", name)
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		return err
	}
	return os.WriteFile(p, data, mode)
}
func (h *fakeHost) Configure(_ string, p agent.ProcessSpec) error { h.proc = p; return nil }
func (h *fakeHost) Snapshot() (map[string]agentapi.ProcessStatus, bool) {
	return h.states, h.maintenance
}
func (h *fakeHost) Stopped(string) bool { return h.stopped }
func (h *fakeHost) Action(_ context.Context, _ string, action string) error {
	h.actions = append(h.actions, action)
	return nil
}
func TestApplyFingerprintHolds(t *testing.T) {
	root := t.TempDir()
	t.Setenv("KARAKORAM_WEAVIATE_INSTALL_ROOT", root)
	s := testSpec()
	p := binary(s.Version)
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("executable"), 0700); err != nil {
		t.Fatal(err)
	}
	h := &fakeHost{dir: t.TempDir(), states: map[string]agentapi.ProcessStatus{wvspec.Process: {PID: 1}}}
	c := config(s)
	if _, err := apply(t.Context(), h, c, s); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(h.actions, []string{"restart"}) {
		t.Fatal(h.actions)
	}
	h.actions = nil
	if _, err := apply(t.Context(), h, c, s); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(h.actions, []string{"start"}) {
		t.Fatal(h.actions)
	}
	for _, hold := range []string{"maintenance", "operator", "desired"} {
		t.Run(hold, func(t *testing.T) {
			h.actions = nil
			s.RestartToken = hold
			c = config(s)
			h.maintenance = hold == "maintenance"
			h.stopped = hold == "operator"
			if hold == "desired" {
				c.DesiredProcesses = []string{}
			}
			if _, err := apply(t.Context(), h, c, s); err != nil {
				t.Fatal(err)
			}
			if hold != "desired" && len(h.actions) != 0 {
				t.Fatal(h.actions)
			}
			h.maintenance = false
			h.stopped = false
			c.DesiredProcesses = nil
			h.actions = nil
			if _, err := apply(t.Context(), h, c, s); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(h.actions, []string{"restart"}) {
				t.Fatal(h.actions)
			}
		})
	}
	p1, f1 := render(h.dir, s, c)
	a := fingerprint(s, p1, f1)
	s.RestartToken += "x"
	if a == fingerprint(s, p1, f1) {
		t.Fatal("token not fingerprinted")
	}
	f1["cert"] = []byte("changed")
	if a == fingerprint(testSpec(), p1, f1) {
		t.Fatal("certificate not fingerprinted")
	}
}
func TestApplyValidation(t *testing.T) {
	for _, mutate := range []func(*agentapi.MemberConfig){func(c *agentapi.MemberConfig) { c.Engine = "postgres" }, func(c *agentapi.MemberConfig) { c.EngineSpec = nil }, func(c *agentapi.MemberConfig) { c.EngineVersion = "1.39.12" }, func(c *agentapi.MemberConfig) { s := testSpec(); s.Ports.HTTP = 0; *c = config(s) }, func(c *agentapi.MemberConfig) { s := testSpec(); s.TLS = true; *c = config(s) }} {
		c := config(testSpec())
		mutate(&c)
		if _, err := New().Apply(t.Context(), nil, c); err == nil {
			t.Fatal("invalid accepted")
		}
	}
	t.Setenv("KARAKORAM_WEAVIATE_INSTALL_ROOT", t.TempDir())
	h := &fakeHost{dir: t.TempDir()}
	if _, err := apply(t.Context(), h, config(testSpec()), testSpec()); err == nil || len(h.actions) != 0 {
		t.Fatal("missing executable accepted", err)
	}
}
func TestReadinessRequestBounds(t *testing.T) {
	for _, body := range []string{`{}`, `{"unknown":true}`, `{} {}`, `null`, strings.Repeat(" ", 4097) + `{}`} {
		r := httptest.NewRequest("POST", "/", strings.NewReader(body))
		err := decodeReadiness(httptest.NewRecorder(), r)
		if (body == `{}`) != (err == nil) {
			t.Fatalf("body length %d: %v", len(body), err)
		}
	}
}
func TestMetricsTranslation(t *testing.T) {
	data, err := os.ReadFile("testdata/weaviate-1.40.1.prom")
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := translateMetrics(strings.NewReader(string(data)))
	if err != nil {
		t.Fatal(err)
	}
	// Expected values are derived directly from the captured source samples:
	// query histogram count=1 (not its ten cumulative buckets or sum=0);
	// requests=1+2+1+1+1; LSM memtables=149+44+1005+81. There are no
	// segment-size samples in this capture and only one distinct collection.
	assertMetricValues(t, metrics, map[string]metricExpectation{
		"karakoram_weaviate_up":                      {1, false},
		"karakoram_weaviate_objects":                 {0, false},
		"karakoram_weaviate_shards":                  {1 + 0 + 0 + 0, false},
		"karakoram_weaviate_collections":             {1, false},
		"karakoram_weaviate_queries_total":           {1, true},
		"karakoram_weaviate_requests_total":          {1 + 2 + 1 + 1 + 1, true},
		"karakoram_weaviate_batch_objects_total":     {0 + 0, true},
		"karakoram_weaviate_vector_index_capacity":   {1000, false},
		"karakoram_weaviate_vector_index_tombstones": {0, false},
		"karakoram_weaviate_lsm_bytes":               {149 + 44 + 1005 + 81, false},
		"karakoram_weaviate_async_queue_length":      {0, false},
	})
	for _, body := range []string{"", "not prometheus", strings.Repeat(" ", (2<<20)+1)} {
		if _, err := translateMetrics(strings.NewReader(body)); err == nil {
			t.Fatal("invalid accepted")
		}
	}
	metrics, err = translateMetrics(strings.NewReader("object_count{class_name=\"A\",shard_name=\"one\"} 2\nobject_count{class_name=\"A\",shard_name=\"two\"} 3\nobject_count{class_name=\"B\"} NaN\n"))
	if err != nil {
		t.Fatal(err)
	}
	assertMetricValues(t, metrics, map[string]metricExpectation{
		"karakoram_weaviate_up":          {1, false},
		"karakoram_weaviate_objects":     {2 + 3, false},
		"karakoram_weaviate_collections": {1, false},
	})
}
