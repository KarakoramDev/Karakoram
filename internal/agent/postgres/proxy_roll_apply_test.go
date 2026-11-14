// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	model "github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	pgengine "github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/dokploy"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Pull and apply during the roll itself, not just before and after it. An empty
// rendered list is destructive intent: Apply deletes the writer and the epoch
// observer forgets its old postmaster, so neither can recover locally afterward.
func TestProxyForcedApplyDuringDokployRoll(t *testing.T) {
	for _, action := range []string{"deploy", "restart", "scale"} {
		t.Run(action, func(t *testing.T) {
			api := &proxyRollAPI{action: action, task: "running"}
			server := httptest.NewServer(api)
			t.Cleanup(server.Close)
			t.Setenv("KARAKORAM_DOKPLOY_ALLOWED_URLS", server.URL)
			backend, err := dokploy.NewFactory().New(infra.Target{Kind: "dokploy", ID: "rollenv", Token: "synthetic-test-key", Config: map[string]string{"url": server.URL}})
			if err != nil {
				t.Fatal(err)
			}
			provider := pgengine.New(pgengine.Options{})
			sealer, err := secrets.New(strings.Repeat("x", 32))
			if err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { st.Close() })
			cluster := &model.Cluster{Name: "sample", Namespace: model.DefaultNamespace, Generation: 1}
			provider.ApplyDefaults(&cluster.Spec)
			cluster.Spec.Topology.Type = "single"
			if err := st.CreateCluster(t.Context(), cluster); err != nil {
				t.Fatal(err)
			}
			rc := &engine.Context{Cluster: cluster, Backend: backend, Store: st, Secrets: st, Clock: engine.RealClock{}, ObservationEpoch: "roll-test"}
			putSecret := func(key, value string) {
				t.Helper()
				if err := st.SetClusterSecret(t.Context(), cluster.Ref(), key, []byte(value)); err != nil {
					t.Fatal(err)
				}
			}
			for _, key := range []string{"tls/ca.crt", "tls/servers/samplepx.crt", "tls/servers/samplepx.key", "tls/agents/samplepx.crt", "tls/agents/samplepx.key", "users/kk_monitor", "users/kk_proxy_admin", "users/postgres"} {
				putSecret(key, "synthetic-test-value")
			}
			setState := func(removing bool) {
				t.Helper()
				raw, err := json.Marshal(map[string]any{"postgres": map[string]any{
					"uid": "roll-cluster", "configVersion": 1, "topology": "single", "formed": true,
					"members": []map[string]any{
						{"name": "samplep0", "role": "data", "stage": "joined", "serviceId": "app-primary", "version": cluster.Spec.Version, "replicas": 1, "configVersion": 1, "removing": removing},
						{"name": "samplepx", "role": "proxy", "stage": "joined", "serviceId": "app-proxy", "version": cluster.Spec.Version, "replicas": 1, "configVersion": 1},
					},
				}})
				if err != nil {
					t.Fatal(err)
				}
				cluster.Internal = raw
			}
			setState(false)
			admin := proxyRollAdmin(t)
			query := func(ctx context.Context, sql string) ([]tableRow, error) { return queryTable(ctx, admin, sql) }
			epochs := map[string]time.Time{}
			recovery := epochRecovery(t)
			old := time.Unix(100, 0)
			for i, phase := range []struct {
				name, task string
				pending    bool
			}{
				{"before", "running", false},
				{"pending-old-task", "running", true},
				{"stopped-task-gap", "shutdown", true},
				{"empty-task-gap", "", true},
				{"replacement-running", "running", true},
				{"receipt-cleared", "running", false},
			} {
				api.set(phase.task, phase.pending)
				// Force real Apply work at every phase without altering server intent.
				putSecret("users/kk_monitor", fmt.Sprintf("synthetic-monitor-%d", i))
				cfg, err := provider.MemberConfig(t.Context(), rc, "samplepx")
				if err != nil {
					t.Fatalf("%s render: %v", phase.name, err)
				}
				var spec pgspec.Spec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if spec.Proxy == nil {
					t.Fatal("real MemberConfig did not render a proxy")
				}
				changed, err := reconcileProxy(t.Context(), admin, spec.Proxy, "", "/synthetic/ca.crt")
				if err != nil || changed == 0 {
					t.Fatalf("%s forced Apply: statements=%d error=%v", phase.name, changed, err)
				}
				rows, err := query(t.Context(), "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers")
				if err != nil || len(rows) != 1 || rows[0]["hostname"] != "samplep0" || rows[0]["status"] != "ONLINE" {
					t.Fatalf("%s Apply removed the primary inside its roll: runtime=%v error=%v rendered=%v", phase.name, rows, err, spec.Proxy.Servers)
				}
				if i == 0 {
					// Model native role discovery once; subsequent LOADs must preserve it.
					if _, err := admin.Exec(t.Context(), "UPDATE pgsql_servers SET hostgroup_id='10' WHERE hostname='samplep0' AND port=5432"); err != nil {
						t.Fatal(err)
					}
					if _, err := admin.Exec(t.Context(), "LOAD PGSQL SERVERS TO RUNTIME"); err != nil {
						t.Fatal(err)
					}
				} else if rows[0]["hostgroup_id"] != "10" {
					t.Fatalf("%s Apply lost the primary's native writer role: %v", phase.name, rows)
				}
				err = checkProxyEpochs(t.Context(), spec.Proxy.Servers, epochs, func(context.Context, string) (time.Time, error) {
					if phase.task != "running" {
						return time.Time{}, errors.New("postmaster unavailable during roll")
					}
					if i >= 4 {
						return old.Add(time.Second), nil
					}
					return old, nil
				}, query, recovery, epochLog())
				if err != nil {
					t.Fatalf("%s epoch observation: %v", phase.name, err)
				}
				want := old
				if i >= 4 {
					want = old.Add(time.Second)
				}
				if !epochs["samplep0"].Equal(want) {
					t.Fatalf("%s lost or failed to recover the epoch: %v", phase.name, epochs)
				}
			}
			// Retirement is authoritative even if an active receipt still exists.
			api.set("running", true)
			setState(true)
			cfg, err := provider.MemberConfig(t.Context(), rc, "samplepx")
			if err != nil {
				t.Fatal(err)
			}
			var retired pgspec.Spec
			if err := json.Unmarshal(cfg.EngineSpec, &retired); err != nil {
				t.Fatal(err)
			}
			plan, err := proxyPlan(t.Context(), admin, retired.Proxy, "", "/synthetic/ca.crt")
			if err != nil || !strings.Contains(strings.Join(plan, "\n"), "DELETE FROM pgsql_servers WHERE hostname='samplep0'") {
				t.Fatalf("retirement did not plan deletion: %v %v", plan, err)
			}
			if _, err := reconcileProxy(t.Context(), admin, retired.Proxy, "", "/synthetic/ca.crt"); err != nil {
				t.Fatal(err)
			}
			rows, err := query(t.Context(), "SELECT hostname FROM runtime_pgsql_servers")
			if err != nil || len(rows) != 0 {
				t.Fatalf("retirement retained runtime backend: %v %v", rows, err)
			}
			if err := checkProxyEpochs(t.Context(), retired.Proxy.Servers, epochs, func(context.Context, string) (time.Time, error) {
				t.Error("retired backend was observed")
				return old, nil
			}, query, recovery, epochLog()); err != nil {
				t.Fatal(err)
			}
			if len(epochs) != 0 {
				t.Fatalf("retirement retained epoch: %v", epochs)
			}
		})
	}
}

// Only the four read endpoints used by real Dokploy RuntimeStates are served.
// Unexpected provider APIs fail rather than falling back to a fake backend.
type proxyRollAPI struct {
	mu      sync.Mutex
	action  string
	task    string
	pending bool
}

func (a *proxyRollAPI) set(task string, pending bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.task, a.pending = task, pending
}

func (a *proxyRollAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var result any
	switch r.URL.Path {
	case "/api/environment.one":
		metadata := map[string]any{"kind": "runtime", "hostname": "samplep0", "environment": "rollenv", "desired": "running", "deployed": true, "min": 1, "max": 1, "vars": map[string]string{"KARAKORAM_OWNER": "roll-cluster"}}
		if a.pending {
			metadata["pending"] = map[string]any{"action": a.action, "appId": "app-primary", "hostname": "samplep0", "appName": "samplep0-swarm", "serviceIdentity": "swarmprimary", "issued": time.Now().UTC().Add(-time.Minute)}
		}
		raw, _ := json.Marshal(metadata)
		result = map[string]any{"applications": []map[string]any{{"applicationId": "app-primary", "name": "samplep0", "appName": "samplep0-swarm", "environmentId": "rollenv", "description": "karakoram:v1:" + string(raw), "applicationStatus": "done"}}}
	case "/api/swarm.getNodeApps":
		result = []map[string]string{{"ID": "swarmprimary", "Name": "samplep0-swarm"}}
	case "/api/docker.getConfig":
		if r.URL.Query().Get("containerId") != "swarmprimary" {
			http.Error(w, "wrong service inspection identity", http.StatusBadRequest)
			return
		}
		result = map[string]any{"ID": "swarmprimary", "Spec": map[string]any{"Name": "samplep0-swarm", "Mode": map[string]any{"Replicated": map[string]int{"Replicas": 1}}, "TaskTemplate": map[string]any{"ContainerSpec": map[string]any{"Labels": map[string]string{"karakoram.managed": "true", "karakoram.hostname": "samplep0", "karakoram.kind": "runtime", "karakoram.owner": "roll-cluster", "karakoram.environment": "rollenv", "karakoram.application": "app-primary"}}}}}
	case "/api/docker.getServiceContainersByAppName":
		if r.URL.Query().Get("appName") != "swarmprimary" {
			http.Error(w, "task lookup must bind inspected service ID", http.StatusBadRequest)
			return
		}
		result = []map[string]string{}
		if a.task != "" {
			result = []map[string]string{{"containerId": "task-primary", "serviceId": "swarmprimary", "name": "samplep0-swarm.1", "state": a.task, "currentState": a.task + " 1 second ago"}}
		}
	default:
		http.Error(w, "unexpected provider API", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}

// A stateful admin protocol fixture: DELETE changes memory, LOAD replaces
// runtime, SAVE replaces disk. It never derives server rows from desired intent.
func proxyRollAdmin(t *testing.T) *pgx.Conn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
		b := pgproto3.NewBackend(conn, conn)
		if _, err := b.ReceiveStartupMessage(); err != nil {
			return
		}
		b.Send(&pgproto3.AuthenticationOk{})
		b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		b.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
		b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if b.Flush() != nil {
			return
		}
		tables := map[string][]tableRow{}
		quoted := regexp.MustCompile(`'((?:[^']|'')*)'`)
		values := func(sql string) []string {
			var result []string
			for _, match := range quoted.FindAllStringSubmatch(sql, -1) {
				result = append(result, strings.ReplaceAll(match[1], "''", "'"))
			}
			return result
		}
		copyRows := func(rows []tableRow) []tableRow {
			result := make([]tableRow, 0, len(rows))
			for _, row := range rows {
				copy := tableRow{}
				for key, value := range row {
					copy[key] = value
				}
				result = append(result, copy)
			}
			return result
		}
		for {
			message, err := b.Receive()
			if err != nil {
				return
			}
			q, ok := message.(*pgproto3.Query)
			if !ok {
				return
			}
			sql := q.String
			supported := true
			switch {
			case strings.HasPrefix(sql, "SELECT "):
				columns, from, _ := strings.Cut(strings.TrimPrefix(sql, "SELECT "), " FROM ")
				table, filter, _ := strings.Cut(from, " WHERE ")
				cols := strings.Split(columns, ",")
				fields := make([]pgproto3.FieldDescription, len(cols))
				for i, col := range cols {
					fields[i] = pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 25, DataTypeSize: -1}
				}
				b.Send(&pgproto3.RowDescription{Fields: fields})
				rows := tables[table]
				if table == "stats_pgsql_connection_pool" {
					rows = []tableRow{{"ConnFree": "0", "ConnUsed": "0"}}
				}
				for _, row := range rows {
					if host := values(filter); strings.HasPrefix(filter, "hostname=") && len(host) > 0 && row["hostname"] != host[0] {
						continue
					}
					data := make([][]byte, len(cols))
					for i, col := range cols {
						data[i] = []byte(row[col])
					}
					b.Send(&pgproto3.DataRow{Values: data})
				}
			case strings.HasPrefix(sql, "INSERT INTO "):
				table, rest, _ := strings.Cut(strings.TrimPrefix(sql, "INSERT INTO "), " (")
				columns, data, _ := strings.Cut(rest, ") VALUES (")
				row := tableRow{}
				vals := values(data)
				for i, col := range strings.Split(columns, ",") {
					row[col] = vals[i]
				}
				if table == "pgsql_servers" {
					row["status"] = "ONLINE"
				}
				tables[table] = append(tables[table], row)
			case strings.HasPrefix(sql, "DELETE FROM "):
				table, condition, _ := strings.Cut(strings.TrimPrefix(sql, "DELETE FROM "), " WHERE ")
				key, _, _ := strings.Cut(condition, "=")
				id := values(condition)[0]
				var keep []tableRow
				for _, row := range tables[table] {
					if row[key] != id {
						keep = append(keep, row)
					}
				}
				tables[table] = keep
			case strings.HasPrefix(sql, "UPDATE "):
				table, rest, _ := strings.Cut(strings.TrimPrefix(sql, "UPDATE "), " SET ")
				assignment, condition, _ := strings.Cut(rest, " WHERE ")
				field, _, _ := strings.Cut(assignment, "=")
				key, _, _ := strings.Cut(condition, "=")
				vals := values(sql)
				found := false
				for _, row := range tables[table] {
					if row[key] == vals[1] {
						row[field] = vals[0]
						found = true
					}
				}
				if !found && table == "global_variables" {
					tables[table] = append(tables[table], tableRow{key: vals[1], field: vals[0]})
				}
			case strings.HasPrefix(sql, "LOAD "), strings.HasPrefix(sql, "SAVE "):
				prefix := "runtime_"
				if strings.HasPrefix(sql, "SAVE ") {
					prefix = "disk."
				}
				var names []string
				switch {
				case strings.Contains(sql, " VARIABLES "):
					names = []string{"global_variables"}
				case strings.Contains(sql, " SERVERS "):
					names = []string{"pgsql_servers", "pgsql_replication_hostgroups"}
				case strings.Contains(sql, " USERS "):
					names = []string{"pgsql_users"}
				case strings.Contains(sql, " QUERY RULES "):
					names = []string{"pgsql_query_rules"}
				default:
					supported = false
				}
				for _, name := range names {
					tables[prefix+name] = copyRows(tables[name])
				}
			default:
				supported = false
			}
			if supported {
				b.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
			} else {
				b.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "unsupported fixture SQL"})
			}
			b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if b.Flush() != nil {
				return
			}
		}
	}()
	cfg, err := pgx.ParseConfig("postgres://admin:synthetic@" + listener.Addr().String() + "/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(t.Context(), cfg)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()); _ = listener.Close(); <-done })
	return conn
}
