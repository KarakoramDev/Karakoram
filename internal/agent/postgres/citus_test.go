// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

func TestDistributedRender(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyDistributed
	s.Citus = &pgspec.Citus{Group: 2, Database: "application"}
	s.Etcd = &pgspec.Etcd{Endpoints: []string{"dcs.example:2379"}}
	s.Postgres.SharedPreloadLibraries = []string{"pg_stat_statements", "citus", "pg_tde", "citus"}
	s.Postgres.Parameters = map[string]string{"wal_level": "replica", "shared_preload_libraries": "wrong"}
	p := dataPaths("/data")
	text, err := renderPatroni(s, p)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err = yaml.Unmarshal([]byte(text), &config); err != nil {
		t.Fatal(err)
	}
	citus := config["citus"].(map[string]any)
	if citus["group"] != 2 || citus["database"] != "application" {
		t.Fatalf("Citus stanza: %v", citus)
	}
	etcd := config["etcd3"].(map[string]any)
	if hosts := etcd["hosts"].([]any); len(hosts) != 1 || hosts[0] != "dcs.example:2379" {
		t.Fatalf("client endpoints: %v", hosts)
	}
	params := config["postgresql"].(map[string]any)["parameters"].(map[string]any)
	if params["wal_level"] != "logical" || params["shared_preload_libraries"] != "citus,pg_stat_statements,pg_tde" {
		t.Fatalf("Citus invariants: %v", params)
	}
	// Keep a small readable golden of the distributed-only rendering surface;
	// unrelated Patroni defaults remain covered by the existing HA tests.
	projection, err := yaml.Marshal(map[string]any{
		"citus":      config["citus"],
		"etcd_hosts": etcd["hosts"],
		"parameters": map[string]any{
			"shared_preload_libraries": params["shared_preload_libraries"],
			"wal_level":                params["wal_level"],
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile("testdata/distributed-patroni.golden")
	if err != nil {
		t.Fatal(err)
	}
	if string(projection) != string(golden) {
		t.Fatalf("distributed render differs from golden:\n%s", projection)
	}
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, file := range files {
		found = found || file.name == "patroni.yml"
	}
	if !found {
		t.Fatal("distributed configuration omitted Patroni")
	}
}

func TestLegacyRenderUnchangedByCitusContract(t *testing.T) {
	for _, topology := range []string{pgspec.TopologySingle, pgspec.TopologyHA} {
		t.Run(topology, func(t *testing.T) {
			s := testSpec()
			s.Topology = topology
			p := dataPaths("/data")
			before, err := render(s, p, nil)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), `"citus"`) || strings.Contains(string(encoded), `"targetName"`) {
				t.Fatal("legacy wire payload changed")
			}
			var roundTrip pgspec.Spec
			if err = json.Unmarshal(encoded, &roundTrip); err != nil {
				t.Fatal(err)
			}
			after, err := render(roundTrip, p, nil)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := json.Marshal(before)
			b, _ := json.Marshal(after)
			if string(a) != string(b) {
				t.Fatal("legacy render bytes changed after wire round trip")
			}
			for _, file := range after {
				if strings.Contains(file.content, "citus:") {
					t.Fatal("legacy config contains Citus stanza")
				}
			}
		})
	}
}

func TestCitusActionValidation(t *testing.T) {
	invalid := []any{&pgspec.CitusRebalanceStartRequest{}, &pgspec.CitusRebalanceStartRequest{Receipt: "r", Strategy: "x'); DROP TABLE x;--"}, &pgspec.CitusRebalanceRequest{JobID: -1}, &pgspec.CitusNodeRequest{Host: "localhost", Port: 0}, &pgspec.CitusNodeRequest{Host: "bad host", Port: 5432}, &pgspec.CitusRestorePointRequest{Name: strings.Repeat("x", 64)}, &pgspec.ArchiveCoversRequest{LSN: "0/1' OR true"}}
	for _, req := range invalid {
		if validateCitusInput(req) == nil {
			t.Errorf("accepted invalid request %#v", req)
		}
	}
	valid := []any{&pgspec.CitusRebalanceStartRequest{Receipt: "operation/rebalance"}, &pgspec.CitusRebalanceRequest{JobID: 1}, &pgspec.CitusNodeRequest{Host: "worker.example", Port: 5432}, &pgspec.CitusRestorePointRequest{Name: "consistent-point"}, &pgspec.ArchiveCoversRequest{LSN: "0/ABCDEF"}}
	for _, req := range valid {
		if err := validateCitusInput(req); err != nil {
			t.Errorf("rejected valid request %#v: %v", req, err)
		}
	}
	for _, payload := range []string{`{"jobId":1,"unknown":true}`, `{"jobId":1} {}`, `null`} {
		req := httptest.NewRequest("POST", "/", strings.NewReader(payload))
		var in pgspec.CitusRebalanceRequest
		err := decodeAction(httptest.NewRecorder(), req, &in)
		if err == nil && validateCitusInput(&in) == nil {
			t.Errorf("accepted payload %s", payload)
		}
	}
}

func TestDCSGroupPrefix(t *testing.T) {
	s := testSpec()
	if got := dcsPrefix(s); got != "/karakoram/test-scope/" {
		t.Fatal(got)
	}
	for _, group := range []int{0, 1, 12} {
		s.Citus = &pgspec.Citus{Group: group, Database: "postgres"}
		prefix := dcsPrefix(s)
		if !strings.HasSuffix(prefix, "/") || prefix == "/karakoram/test-scope/" {
			t.Fatalf("unsafe group reset prefix %q", prefix)
		}
	}
	s.Citus.Group = 0
	a := dcsPrefix(s)
	s.Citus.Group = 1
	b := dcsPrefix(s)
	if strings.HasPrefix(a, b) || strings.HasPrefix(b, a) {
		t.Fatal("group reset prefixes overlap")
	}
}

func TestNamedRestoreArgs(t *testing.T) {
	s := testSpec()
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &pgspec.Restore{TargetName: "formation-point", Source: pgspec.Repository{Stanza: "cluster"}}}
	args, err := restoreArgs(s, dataPaths("/data"))
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Join(args, " ")
	if !strings.Contains(text, "--type=name --target=formation-point") || strings.Contains(text, "--type=immediate") {
		t.Fatal(text)
	}
	s.Bootstrap.Restore.TargetTime = "2026-10-20T00:00:00Z"
	if _, err = restoreArgs(s, dataPaths("/data")); err == nil {
		t.Fatal("accepted mutually exclusive restore targets")
	}
}

func TestDistributedSpecValidation(t *testing.T) {
	s := testSpec()
	s.Topology = pgspec.TopologyDistributed
	s.Citus = &pgspec.Citus{Group: 1, Database: "postgres"}
	s.Peers = []pgspec.Peer{{Name: s.Member, Host: "localhost"}}
	s.Etcd = &pgspec.Etcd{Endpoints: []string{"localhost:2379"}}
	s.CAPEM, s.ServerCert = testTLS(t)
	for _, endpoint := range []string{"localhost:2379", "192.0.2.1:2379", "[2001:db8::1]:2379"} {
		s.Etcd.Endpoints = []string{endpoint}
		if err := validate(s); err != nil {
			t.Fatalf("rejected endpoint %q: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{"https://localhost:2379", "https://[2001:db8::1]:2379", "2001:db8::1:2379"} {
		s.Etcd.Endpoints = []string{endpoint}
		if validate(s) == nil {
			t.Fatalf("accepted malformed client endpoint %q", endpoint)
		}
	}
	s.Etcd.Endpoints = []string{"localhost:2379"}
	s.Citus = nil
	if validate(s) == nil {
		t.Fatal("accepted distributed spec without group")
	}
}
