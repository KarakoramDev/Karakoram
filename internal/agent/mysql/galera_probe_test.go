// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type galeraProbeConnector struct {
	groupTestConnector
	fence    bool
	executed []string
}

func (c *galeraProbeConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *galeraProbeConnector) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.HasPrefix(query, "SHOW GLOBAL STATUS") {
		return &nativeStatusRows{rows: [][]driver.Value{{"wsrep_cluster_status", "Primary"}, {"wsrep_local_state_comment", "Synced"}, {"wsrep_ready", "ON"}, {"wsrep_connected", "ON"}, {"wsrep_cluster_size", "3"}, {"wsrep_cluster_state_uuid", "11111111-2222-3333-4444-555555555555"}, {"wsrep_last_committed", "10"}}}, nil
	}
	return c.groupTestConnector.QueryContext(ctx, query, args)
}
func (c *galeraProbeConnector) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	c.executed = append(c.executed, query)
	return driver.RowsAffected(1), nil
}

func TestGaleraHistoryProbeRequiresFencedReplicatedBinlog(t *testing.T) {
	const uuid = "11111111-2222-3333-4444-555555555555"
	for _, scenario := range []string{"safe", "unfenced", "wsrep off", "binlog off"} {
		t.Run(scenario, func(t *testing.T) {
			fence, wsrep, binlog := scenario != "unfenced", scenario != "wsrep off", scenario != "binlog off"
			c := &galeraProbeConnector{groupTestConnector: groupTestConnector{t: t}}
			c.queries = []groupQuery{{contains: "SELECT @@GLOBAL.gtid_executed", values: []driver.Value{uuid + ":1-10", uuid, fence}}}
			if fence {
				c.queries = append(c.queries, groupQuery{contains: "SELECT @@SESSION.wsrep_on,@@SESSION.sql_log_bin", values: []driver.Value{wsrep, binlog}})
			}
			if scenario == "safe" {
				c.queries = append(c.queries, groupQuery{contains: "SELECT @@GLOBAL.gtid_executed", values: []driver.Value{uuid + ":1-11", uuid, true}})
			}
			db := sql.OpenDB(c)
			defer db.Close()
			out, err := executeGaleraHistoryProbe(t.Context(), db)
			if scenario != "safe" {
				if err == nil || len(c.executed) != 0 {
					t.Fatalf("unsafe probe mutated history: %v, %v", err, c.executed)
				}
			} else if err != nil || len(c.executed) != 1 || c.executed[0] != "CREATE DATABASE IF NOT EXISTS _karakoram" || out.GTIDExecuted != uuid+":1-11" {
				t.Fatalf("probe did not use ordinary managed TOI: %+v %v %v", out, err, c.executed)
			}
		})
	}
}

func TestGaleraHistoryProbeReceiptAndAuthorization(t *testing.T) {
	root := t.TempDir()
	p := dataPaths(root)
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	req := myspec.GaleraHistoryProbeRequest{Receipt: "restore/probe/1", ClusterName: "restored", FormationEpoch: 2}
	result := myspec.GaleraStatusResponse{StateUUID: "11111111-2222-3333-4444-555555555555", GTIDExecuted: "11111111-2222-3333-4444-555555555555:1"}
	digest := sha256.Sum256([]byte(req.Receipt))
	receipt := struct {
		Request myspec.GaleraHistoryProbeRequest `json:"request"`
		Result  myspec.GaleraStatusResponse      `json:"result"`
	}{req, result}
	if err := durableJSON(filepath.Join(p.root, fmt.Sprintf("galera-history-probe-%x.json", digest)), receipt); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{dir: root, current: myspec.Spec{Galera: &myspec.Galera{ClusterName: "restored", FormationEpoch: 2, RestoreFence: true}}}
	// No database exists: a durable receipt must return without another SQL write.
	out, err := runner.galeraHistoryProbe(t.Context(), req)
	if err != nil || out.GTIDExecuted != result.GTIDExecuted {
		t.Fatalf("receipt replay failed: %+v %v", out, err)
	}
	runner.current.Galera.RestoreFence = false
	if _, err := runner.galeraHistoryProbe(t.Context(), req); err == nil {
		t.Fatal("probe authorized without restore fence")
	}
	runner.current.Galera.RestoreFence = true
	runner.current.Galera.FormationEpoch++
	req.FormationEpoch++
	if _, err := runner.galeraHistoryProbe(t.Context(), req); err == nil {
		t.Fatal("receipt reused across formation epoch")
	}
}

func TestGaleraRestoreOfflineFenceRendersAndReleases(t *testing.T) {
	for _, fenced := range []bool{true, false} {
		c := &galeraProbeConnector{groupTestConnector: groupTestConnector{t: t}}
		db := sql.OpenDB(c)
		spec := myspec.Spec{Galera: &myspec.Galera{RestoreFence: fenced}}
		rendered := RenderGaleraOptions(spec, t.TempDir())
		if rendered["offline_mode"] != fmt.Sprint(fenced) {
			t.Fatal("startup client fence differs from desired state")
		}
		runner := &Runner{}
		if err := runner.reload(t.Context(), db, spec, spec); err != nil {
			t.Fatal(err)
		}
		db.Close()
		found := false
		for _, query := range c.executed {
			found = found || query == "SET GLOBAL `offline_mode` = "+fmt.Sprint(fenced)
		}
		if !found {
			t.Fatalf("live fence transition missing: %v", c.executed)
		}
	}
}

func TestGaleraRestoreDonorRequiresClientFence(t *testing.T) {
	spec := testGaleraSpec()
	spec.Galera.SSTDonor = spec.Galera.Peers[0].Name
	if err := spec.Galera.Validate(); err == nil {
		t.Fatal("unfenced SST donor override was accepted")
	}
	spec.Galera.RestoreFence = true
	if err := spec.Galera.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := RenderGaleraOptions(spec, t.TempDir())["wsrep_sst_donor"]; got != spec.Galera.SSTDonor {
		t.Fatalf("trusted seed donor gained a fallback: %q", got)
	}
	spec.Galera.SSTDonor = "foreign-donor"
	if err := spec.Galera.Validate(); err == nil {
		t.Fatal("unconfigured SST donor override was accepted")
	}
}

type restoreFencedAccessDeps struct{ calls int }

func (*restoreFencedAccessDeps) GaleraAccess() bool        { return true }
func (*restoreFencedAccessDeps) AccessRestoreFenced() bool { return true }
func (d *restoreFencedAccessDeps) LocalAccessDB(context.Context) (*sql.DB, error) {
	d.calls++
	return nil, fmt.Errorf("SQL must not open during restore")
}

func TestGaleraRestoreFenceQuiescesManagedWriters(t *testing.T) {
	deps := &restoreFencedAccessDeps{}
	mux := http.NewServeMux()
	RegisterAccessRoutes(mux, deps)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/mysql/"+myspec.ActionAccessUsers, strings.NewReader(`{}`)))
	if response.Code != http.StatusServiceUnavailable || deps.calls != 0 {
		t.Fatalf("account actor reached privileged SQL during restore: status=%d calls=%d", response.Code, deps.calls)
	}
	runner := &Runner{current: myspec.Spec{Role: myspec.RoleData, Galera: &myspec.Galera{RestoreFence: true}, Backup: &myspec.Backup{Binlog: myspec.Binlog{Enabled: true}}}}
	cancelled := 0
	runner.archiveCancel = func() { cancelled++ }
	runner.resetArchiveLoop(true)
	if cancelled != 1 || runner.archiveCancel != nil {
		t.Fatal("restore fence did not cancel archival or restarted its privileged loop")
	}
	if err := runner.archiveBinlogs(t.Context()); err != nil {
		t.Fatalf("fenced direct archive trigger reached SQL: %v", err)
	}
	for _, rekey := range []bool{true, false} {
		spec := testGaleraSpec()
		spec.Galera.RestoreFence = true
		startup := renderStartup(spec, dataPaths(t.TempDir()), rekey)
		wsrep, binlog := strings.Index(startup, "SET SESSION wsrep_on=OFF;"), strings.Index(startup, "SET SESSION sql_log_bin=0;")
		if wsrep < 0 || binlog < 0 {
			t.Fatal("startup account recovery lost local non-binlogged isolation")
		}
		for _, mutation := range []string{"CREATE USER", "ALTER USER", "GRANT ", "REVOKE "} {
			if at := strings.Index(startup, mutation); at >= 0 && (at < wsrep || at < binlog) {
				t.Fatalf("startup %s preceded isolated session fencing", mutation)
			}
		}
	}
}
