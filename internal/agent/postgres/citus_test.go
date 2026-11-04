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

func TestCitusRestoreMetadataIsLocalAtomicAndTargetOwned(t *testing.T) {
	s := testSpec()
	s.Citus = &pgspec.Citus{Group: 1, Database: "application"}
	req := pgspec.CitusRestoreMetadataRequest{Nodes: []pgspec.CitusNode{
		{GroupID: 0, Host: "target-coordinator", Port: 5432, Role: "primary"},
		{GroupID: 0, Host: "target-coordinator-replica", Port: 5432, Role: "secondary"},
		{GroupID: 1, Host: "target-worker", Port: 5432, Role: "primary"},
		{GroupID: 1, Host: "target-worker-replica", Port: 5432, Role: "secondary"},
	}}
	if _, err := citusRestoreMetadataSQL(s, req); err == nil {
		t.Fatal("catalog rewrite accepted without recovery client fences")
	}
	s.Postgres.RecoveryClientsHeld = true
	sql, err := citusRestoreMetadataSQL(s, req)
	if err != nil {
		t.Fatal(err)
	}
	firstWrite := strings.Index(sql, "UPDATE pg_catalog.pg_dist_node")
	for _, guard := range []string{"BEGIN;", "SET LOCAL synchronous_commit = local", "SET LOCAL citus.enable_ddl_propagation = off", "SET LOCAL citus.enable_metadata_sync = off", "LOCK TABLE", "restored Citus groups differ", "restored Citus group differs"} {
		if index := strings.Index(sql, guard); index < 0 || index >= firstWrite {
			t.Error("local recovery guard missing or too late", guard)
		}
	}
	for _, mutation := range []string{
		"nodename='target-coordinator', nodeport=5432 WHERE groupid=0",
		"nodename='target-worker', nodeport=5432 WHERE groupid=1",
		"DELETE FROM pg_catalog.pg_dist_poolinfo;",
		`ALTER DATABASE "application" SET citus.local_hostname TO 'target-worker';`,
		"(1,'target-coordinator-replica',5432)",
		"(1,'target-worker-replica',5432)",
		"INSERT INTO pg_catalog.pg_dist_node SELECT * FROM pg_temp._karakoram_restore_secondaries;",
		"WHERE noderole='primary') <> 2",
	} {
		if !strings.Contains(sql, mutation) {
			t.Error("missing target-owned metadata", mutation)
		}
	}
	if !strings.HasSuffix(sql, "COMMIT;\n") || strings.Contains(sql, "citus_update_node") || strings.Contains(sql, "SET nodeid=") || strings.Contains(sql, "SET groupid=") {
		t.Fatal("rewrite can propagate, lose placement identity or commit partially")
	}
	removeSecondaries := strings.Index(sql, "DELETE FROM pg_catalog.pg_dist_node WHERE noderole='secondary'")
	reinsertSecondaries := strings.Index(sql, "INSERT INTO pg_catalog.pg_dist_node")
	if removeSecondaries < 0 || removeSecondaries >= firstWrite || reinsertSecondaries <= firstWrite {
		t.Fatal("promoted secondary address can collide with primary remapping")
	}
	if strings.Contains(sql, "count(*) FROM pg_catalog.pg_dist_node) <>") {
		t.Fatal("valid secondary rows counted as extra replication groups")
	}
	bad := []pgspec.CitusRestoreMetadataRequest{
		{},
		{Nodes: []pgspec.CitusNode{{GroupID: 1, Host: "worker", Port: 5432, Role: "primary"}}},
		{Nodes: []pgspec.CitusNode{{GroupID: 0, Host: "coordinator", Port: 5432, Role: "primary"}, {GroupID: 0, Host: "worker", Port: 5432, Role: "primary"}}},
		{Nodes: []pgspec.CitusNode{{GroupID: 0, Host: "coordinator", Port: 5432, Role: "primary"}, {GroupID: 1, Host: "coordinator", Port: 5432, Role: "primary"}}},
		{Nodes: []pgspec.CitusNode{{GroupID: 0, Host: "bad'host", Port: 5432, Role: "primary"}}},
		{Nodes: []pgspec.CitusNode{{GroupID: 0, Host: "coordinator", Port: 5432, Role: "primary"}, {GroupID: 1, Host: "worker", Port: 5432, Role: "secondary"}}},
		{Nodes: []pgspec.CitusNode{{GroupID: 0, Host: "coordinator", Port: 5432, Role: "unknown"}}},
	}
	for _, input := range bad {
		if err := validateCitusInput(&input); err == nil {
			t.Error("accepted invalid metadata mapping", input)
		}
	}
	s.Citus.Group = 2
	if _, err := citusRestoreMetadataSQL(s, req); err == nil {
		t.Fatal("accepted mapping without local group")
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
