// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type epochAdmin struct {
	calls      []string
	status     string
	configured string
	pools      []uint64
	fail       string
	cancel     context.CancelFunc
}

func (a *epochAdmin) query(ctx context.Context, sql string) ([]tableRow, error) {
	a.calls = append(a.calls, sql)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if a.fail == sql {
		a.fail = ""
		return nil, errors.New("admin failure")
	}
	switch {
	case strings.HasPrefix(sql, "SELECT hostgroup_id,status FROM runtime_"):
		return []tableRow{{"hostgroup_id": "10", "status": a.status}}, nil
	case strings.HasPrefix(sql, "SELECT hostgroup_id,status FROM pgsql_servers"):
		return []tableRow{{"hostgroup_id": "20", "status": a.configured}}, nil
	case strings.HasPrefix(sql, "SELECT ConnFree,ConnUsed"):
		n := a.pools[0]
		if len(a.pools) > 1 {
			a.pools = a.pools[1:]
		}
		if n == 0 {
			return []tableRow{{"ConnFree": "0", "ConnUsed": "0"}}, nil
		}
		return []tableRow{{"ConnFree": "2", "ConnUsed": "1"}}, nil
	case strings.Contains(sql, "status='OFFLINE_HARD'"):
		a.configured = "OFFLINE_HARD"
		if a.cancel != nil {
			a.cancel()
		}
	case strings.Contains(sql, "status='ONLINE'"):
		a.configured = "ONLINE"
	case sql == "LOAD PGSQL SERVERS TO RUNTIME":
		a.status = a.configured
	}
	return nil, nil
}

func epochLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func epochRecovery(t *testing.T) *proxyEpochRecovery {
	t.Helper()
	path := filepath.Join(t.TempDir(), "epoch.json")
	r, err := readProxyEpochRecovery(path, func(data []byte) error {
		return os.WriteFile(path, data, 0600)
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestProxyEpochObservations(t *testing.T) {
	old := time.Unix(100, 0)
	for _, scenario := range []string{"first", "unchanged", "unreachable", "changed"} {
		t.Run(scenario, func(t *testing.T) {
			epochs := map[string]time.Time{}
			if scenario != "first" {
				epochs["db"] = old
			}
			a := &epochAdmin{status: "ONLINE", configured: "ONLINE", pools: []uint64{3, 3, 0}}
			err := checkProxyEpochs(t.Context(), []string{"db"}, epochs, func(context.Context, string) (time.Time, error) {
				if scenario == "unreachable" {
					return time.Time{}, errors.New("backend down")
				}
				if scenario == "unchanged" {
					return old, nil
				}
				return old.Add(time.Second), nil
			}, a.query, epochRecovery(t), epochLog())
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "changed" {
				if len(a.calls) != 0 {
					t.Fatal("unexpected admin action", a.calls)
				}
				return
			}
			filter := " WHERE hostname='db' AND port=5432"
			pool := "SELECT ConnFree,ConnUsed FROM stats_pgsql_connection_pool WHERE srv_host='db' AND srv_port=5432"
			want := []string{
				"SELECT hostgroup_id,status FROM runtime_pgsql_servers" + filter,
				"SELECT hostgroup_id,status FROM pgsql_servers" + filter,
				pool,
				"UPDATE pgsql_servers SET status='OFFLINE_HARD'" + filter,
				"SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers",
				"SELECT hostname,port,hostgroup_id,status FROM pgsql_servers",
				"LOAD PGSQL SERVERS TO RUNTIME",
				pool, pool,
				"UPDATE pgsql_servers SET status='ONLINE'" + filter,
				"SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers",
				"SELECT hostname,port,hostgroup_id,status FROM pgsql_servers",
				"LOAD PGSQL SERVERS TO RUNTIME",
			}
			if !reflect.DeepEqual(a.calls, want) {
				t.Fatalf("order got %q want %q", a.calls, want)
			}
			if !epochs["db"].Equal(old.Add(time.Second)) {
				t.Fatal("successful flush did not advance epoch")
			}
		})
	}
}

func TestProxyEpochFailureRestoresStatus(t *testing.T) {
	filter := " WHERE hostname='db' AND port=5432"
	for _, failure := range []string{"timeout", "cancel", "offline", "load", "pool", "restore", "restore-load"} {
		t.Run(failure, func(t *testing.T) {
			a := &epochAdmin{status: "ONLINE", configured: "ONLINE", pools: []uint64{3, 0}}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			switch failure {
			case "timeout":
				a.pools = []uint64{3}
			case "cancel":
				a.cancel = cancel
			case "offline":
				a.fail = "UPDATE pgsql_servers SET status='OFFLINE_HARD'" + filter
			case "load":
				a.fail = "LOAD PGSQL SERVERS TO RUNTIME"
			case "restore":
				a.fail = "UPDATE pgsql_servers SET status='ONLINE'" + filter
			}
			query := func(ctx context.Context, sql string) ([]tableRow, error) {
				if failure == "pool" && strings.HasPrefix(sql, "SELECT ConnFree") && len(a.calls) > 3 {
					a.calls = append(a.calls, sql)
					return nil, errors.New("pool failure")
				}
				if failure == "restore-load" && sql == "LOAD PGSQL SERVERS TO RUNTIME" && len(a.calls) > 8 {
					a.calls = append(a.calls, sql)
					return nil, errors.New("restore load failure")
				}
				return a.query(ctx, sql)
			}
			old := time.Unix(100, 0)
			epochs := map[string]time.Time{"db": old}
			err := checkProxyEpochs(ctx, []string{"db"}, epochs, func(context.Context, string) (time.Time, error) {
				return old.Add(time.Second), nil
			}, query, epochRecovery(t), epochLog())
			if err == nil || !epochs["db"].Equal(old) {
				t.Fatalf("failure must surface and retain epoch: %v %v", err, epochs)
			}
			if failure == "restore" {
				if a.calls[len(a.calls)-1] != "UPDATE pgsql_servers SET status='ONLINE'"+filter {
					t.Fatal("missing restoration attempt", a.calls)
				}
				return
			}
			last := a.calls[len(a.calls)-4:]
			want := []string{
				"UPDATE pgsql_servers SET status='ONLINE'" + filter,
				"SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers",
				"SELECT hostname,port,hostgroup_id,status FROM pgsql_servers",
				"LOAD PGSQL SERVERS TO RUNTIME",
			}
			if !reflect.DeepEqual(last, want) {
				t.Fatal("missing restoration", a.calls)
			}
		})
	}
}

func TestProxyEpochPreservesNativeShun(t *testing.T) {
	for _, status := range []string{"SHUNNED", "OFFLINE_SOFT", "OFFLINE_HARD"} {
		t.Run(status, func(t *testing.T) {
			a := &epochAdmin{status: status, configured: "ONLINE"}
			flushed, err := flushProxyEpoch(t.Context(), "db", time.Unix(1, 0), time.Unix(2, 0), a.query, epochRecovery(t), epochLog())
			if err != nil || flushed || len(a.calls) != 1 {
				t.Fatal("native status overwritten", flushed, err, a.calls)
			}
		})
	}
}

func TestProxyEpochRemovedBackendIsForgotten(t *testing.T) {
	epochs := map[string]time.Time{"removed": time.Unix(1, 0)}
	if err := checkProxyEpochs(t.Context(), nil, epochs, nil, nil, epochRecovery(t), epochLog()); err != nil || len(epochs) != 0 {
		t.Fatal(err, epochs)
	}
}

func TestProxyEpochLoadPreservesUnrelatedNativeShun(t *testing.T) {
	for _, failLoad := range []bool{false, true} {
		var calls []string
		err := loadProxyEpochServers(t.Context(), "restarted", func(ctx context.Context, sql string) ([]tableRow, error) {
			calls = append(calls, sql)
			switch sql {
			case "SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers":
				return []tableRow{
					{"hostname": "restarted", "port": "5432", "status": "OFFLINE_HARD"},
					{"hostname": "replica", "port": "5432", "status": "SHUNNED"},
				}, nil
			case "SELECT hostname,port,hostgroup_id,status FROM pgsql_servers":
				return []tableRow{
					{"hostname": "restarted", "port": "5432", "status": "ONLINE"},
					{"hostname": "replica", "port": "5432", "status": "ONLINE"},
				}, nil
			case "LOAD PGSQL SERVERS TO RUNTIME":
				if failLoad {
					return nil, errors.New("load failure")
				}
			}
			return nil, nil
		}, epochRecovery(t))
		if (err != nil) != failLoad {
			t.Fatal("load error not propagated", err)
		}
		want := []string{
			"SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers",
			"SELECT hostname,port,hostgroup_id,status FROM pgsql_servers",
			"UPDATE pgsql_servers SET status='SHUNNED' WHERE hostname='replica' AND port='5432' AND status='ONLINE'",
			"LOAD PGSQL SERVERS TO RUNTIME",
			"UPDATE pgsql_servers SET status='ONLINE' WHERE hostname='replica' AND port='5432' AND status='SHUNNED'",
		}
		if !reflect.DeepEqual(calls, want) {
			t.Fatalf("native state not preserved through load: %q", calls)
		}
	}
}

func TestProxyEpochRetriesDurableRestorationBeforeObservation(t *testing.T) {
	for _, failure := range []string{"update", "load", "budget"} {
		t.Run(failure, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recovery.json")
			save := func(data []byte) error { return os.WriteFile(path, data, 0600) }
			recovery, err := readProxyEpochRecovery(path, save)
			if err != nil {
				t.Fatal(err)
			}
			a := &epochAdmin{status: "ONLINE", configured: "ONLINE", pools: []uint64{3, 0}}
			cleanup := false
			failed := false
			query := func(ctx context.Context, sql string) ([]tableRow, error) {
				if strings.HasPrefix(sql, "UPDATE pgsql_servers SET status='ONLINE'") {
					cleanup = true
				}
				if !failed && cleanup && ((failure != "load" && strings.HasPrefix(sql, "UPDATE pgsql_servers SET status='ONLINE'")) || (failure == "load" && sql == "LOAD PGSQL SERVERS TO RUNTIME")) {
					failed = true
					if failure == "budget" {
						return nil, context.DeadlineExceeded
					}
					return nil, errors.New("cleanup unavailable")
				}
				return a.query(ctx, sql)
			}
			old := time.Unix(100, 0)
			flushed, err := flushProxyEpoch(t.Context(), "db", old, old.Add(time.Second), query, recovery, epochLog())
			if err == nil || !flushed || a.status != "OFFLINE_HARD" || len(recovery.pending) != 1 {
				t.Fatalf("fake must retain an owned runtime fence: flushed=%v err=%v runtime=%s pending=%v", flushed, err, a.status, recovery.pending)
			}
			// Recreate both the journal object and epoch map, as on agent restart.
			recovery, err = readProxyEpochRecovery(path, save)
			if err != nil || !recovery.pending["db"].Equal(old) {
				t.Fatal("ownership did not survive restart", err, recovery)
			}
			epochs := map[string]time.Time{}
			observedAfterRestore := false
			err = checkProxyEpochs(t.Context(), []string{"db"}, epochs, func(context.Context, string) (time.Time, error) {
				observedAfterRestore = a.status == "ONLINE" && a.configured == "ONLINE"
				return time.Time{}, errors.New("backend unavailable")
			}, a.query, recovery, epochLog())
			if err != nil || !observedAfterRestore || len(recovery.pending) != 0 || !epochs["db"].Equal(old) {
				t.Fatal("next iteration did not restore before observation", err, a.status, recovery.pending, epochs)
			}
			reloaded, err := readProxyEpochRecovery(path, save)
			if err != nil || len(reloaded.pending) != 0 {
				t.Fatal("completed ownership not durably cleared", err, reloaded)
			}
		})
	}
}

func TestProxyEpochJournalFailurePreventsFence(t *testing.T) {
	a := &epochAdmin{status: "ONLINE", configured: "ONLINE", pools: []uint64{3}}
	recovery := &proxyEpochRecovery{
		pending: map[string]time.Time{},
		save:    func([]byte) error { return errors.New("disk unavailable") },
	}
	if _, err := flushProxyEpoch(t.Context(), "db", time.Unix(1, 0), time.Unix(2, 0), a.query, recovery, epochLog()); err == nil {
		t.Fatal("journal error hidden")
	}
	if a.status != "ONLINE" || a.configured != "ONLINE" || len(recovery.pending) != 0 {
		t.Fatal("fenced without durable ownership", a.status, a.configured)
	}
}

func TestProxyEpochReplicaFlushPreservesWriterRoleAndConnections(t *testing.T) {
	runtime := []tableRow{
		{"hostname": "primary", "port": "5432", "hostgroup_id": "10", "status": "ONLINE"},
		{"hostname": "replica", "port": "5432", "hostgroup_id": "20", "status": "ONLINE"},
	}
	configured := []tableRow{
		{"hostname": "primary", "port": "5432", "hostgroup_id": "20", "status": "ONLINE"},
		{"hostname": "replica", "port": "5432", "hostgroup_id": "10", "status": "ONLINE"},
	}
	writerConnections, loads := 4, 0
	copyRows := func(rows []tableRow) []tableRow {
		copied := make([]tableRow, len(rows))
		for i, row := range rows {
			copied[i] = tableRow{}
			for key, value := range row {
				copied[i][key] = value
			}
		}
		return copied
	}
	query := func(ctx context.Context, sql string) ([]tableRow, error) {
		switch {
		case sql == "SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers":
			return copyRows(runtime), nil
		case sql == "SELECT hostname,port,hostgroup_id,status FROM pgsql_servers":
			return copyRows(configured), nil
		case strings.HasPrefix(sql, "SELECT hostgroup_id,status FROM runtime_"):
			return []tableRow{runtime[1]}, nil
		case strings.HasPrefix(sql, "SELECT hostgroup_id,status FROM pgsql_servers"):
			return []tableRow{configured[1]}, nil
		case strings.HasPrefix(sql, "SELECT ConnFree,ConnUsed"):
			if runtime[1]["status"] == "OFFLINE_HARD" {
				return []tableRow{{"ConnFree": "0", "ConnUsed": "0"}}, nil
			}
			return []tableRow{{"ConnFree": "1", "ConnUsed": "0"}}, nil
		case strings.HasPrefix(sql, "UPDATE pgsql_servers SET hostgroup_id="):
			if strings.Contains(sql, "hostname='primary'") {
				configured[0]["hostgroup_id"] = "10"
			} else {
				configured[1]["hostgroup_id"] = "20"
			}
		case strings.HasPrefix(sql, "UPDATE pgsql_servers SET status='OFFLINE_HARD'"):
			configured[1]["status"] = "OFFLINE_HARD"
		case strings.HasPrefix(sql, "UPDATE pgsql_servers SET status='ONLINE'"):
			configured[1]["status"] = "ONLINE"
		case sql == "LOAD PGSQL SERVERS TO RUNTIME":
			loads++
			if configured[0]["hostgroup_id"] != runtime[0]["hostgroup_id"] || configured[0]["status"] != "ONLINE" {
				writerConnections = 0 // Native removal kills the old writer pool.
			}
			runtime = copyRows(configured)
		default:
			t.Fatalf("unexpected query: %s", sql)
		}
		return nil, nil
	}
	flushed, err := flushProxyEpoch(t.Context(), "replica", time.Unix(1, 0), time.Unix(2, 0), query, epochRecovery(t), epochLog())
	if err != nil || !flushed || loads != 2 || writerConnections != 4 || runtime[0]["hostgroup_id"] != "10" || runtime[1]["hostgroup_id"] != "20" {
		t.Fatalf("replica flush disturbed writer: flushed=%v err=%v loads=%d connections=%d runtime=%v", flushed, err, loads, writerConnections, runtime)
	}
}

func TestProxyEpochUnrelatedResetFailureSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.json")
	save := func(data []byte) error { return os.WriteFile(path, data, 0600) }
	recovery, err := readProxyEpochRecovery(path, save)
	if err != nil {
		t.Fatal(err)
	}
	a := &epochAdmin{status: "ONLINE", configured: "ONLINE", pools: []uint64{0}}
	configuredWriter, runtimeWriter := "ONLINE", "SHUNNED"
	failReset := true
	reset := "UPDATE pgsql_servers SET status='ONLINE' WHERE hostname='writer' AND port='5432' AND status='SHUNNED'"
	query := func(ctx context.Context, sql string) ([]tableRow, error) {
		switch sql {
		case "SELECT hostname,port,hostgroup_id,status FROM runtime_pgsql_servers":
			return []tableRow{
				{"hostname": "replica", "port": "5432", "hostgroup_id": "20", "status": a.status},
				{"hostname": "writer", "port": "5432", "hostgroup_id": "10", "status": runtimeWriter},
			}, nil
		case "SELECT hostname,port,hostgroup_id,status FROM pgsql_servers":
			return []tableRow{
				{"hostname": "replica", "port": "5432", "hostgroup_id": "20", "status": a.configured},
				{"hostname": "writer", "port": "5432", "hostgroup_id": "10", "status": configuredWriter},
			}, nil
		case "UPDATE pgsql_servers SET status='SHUNNED' WHERE hostname='writer' AND port='5432' AND status='ONLINE'":
			configuredWriter = "SHUNNED"
			return nil, nil
		case reset:
			if failReset {
				return nil, errors.New("unrelated reset unavailable")
			}
			if configuredWriter == "SHUNNED" {
				configuredWriter = "ONLINE"
			}
			return nil, nil
		case "LOAD PGSQL SERVERS TO RUNTIME":
			runtimeWriter = configuredWriter
		}
		return a.query(ctx, sql)
	}
	old := time.Unix(100, 0)
	_, err = flushProxyEpoch(t.Context(), "replica", old, old.Add(time.Second), query, recovery, epochLog())
	if err == nil || configuredWriter != "SHUNNED" || a.status != "OFFLINE_HARD" || len(recovery.pending) != 1 || len(recovery.statusRestores) != 1 {
		t.Fatalf("failed reset must retain all ownership: err=%v writer=%s replica=%s pending=%v restores=%v", err, configuredWriter, a.status, recovery.pending, recovery.statusRestores)
	}
	// The agent process restarts while ProxySQL and its temporary config survive.
	recovery, err = readProxyEpochRecovery(path, save)
	if err != nil || len(recovery.statusRestores) != 1 || len(recovery.pending) != 1 {
		t.Fatal("temporary mutation not durable", err, recovery)
	}
	failReset = false
	observedRestored := false
	err = checkProxyEpochs(t.Context(), []string{"replica"}, map[string]time.Time{}, func(context.Context, string) (time.Time, error) {
		observedRestored = configuredWriter == "ONLINE" && runtimeWriter == "SHUNNED" && a.status == "ONLINE"
		return time.Time{}, errors.New("backend unavailable")
	}, query, recovery, epochLog())
	if err != nil || !observedRestored || len(recovery.pending) != 0 || len(recovery.statusRestores) != 0 {
		t.Fatal("restart did not repair unrelated status before releasing ownership", err, configuredWriter, runtimeWriter, recovery)
	}
	// Native recovery can now bring the writer online. Future global loads must
	// not re-shun it using abandoned temporary configuration.
	runtimeWriter = "ONLINE"
	if err = loadProxyEpochServers(t.Context(), "replica", query, recovery); err != nil || runtimeWriter != "ONLINE" || configuredWriter != "ONLINE" {
		t.Fatal("later LOAD re-shunned recovered writer", err, runtimeWriter, configuredWriter)
	}
	reloaded, err := readProxyEpochRecovery(path, save)
	if err != nil || len(reloaded.pending) != 0 || len(reloaded.statusRestores) != 0 {
		t.Fatal("restored state not durably cleared", err, reloaded)
	}
}

func TestProxyEpochStatusRecoveryRetainsAllInversesOnPartialFailure(t *testing.T) {
	for _, failure := range []string{"reset", "persist"} {
		t.Run(failure, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "recovery.json")
			failSave := false
			save := func(data []byte) error {
				if failSave {
					return errors.New("journal unavailable")
				}
				return os.WriteFile(path, data, 0600)
			}
			recovery, err := readProxyEpochRecovery(path, save)
			if err != nil {
				t.Fatal(err)
			}
			for _, sql := range []string{"reset A", "reset B"} {
				if err = recovery.rememberStatusRestore(sql); err != nil {
					t.Fatal(err)
				}
			}
			failSave = failure == "persist"
			err = recovery.restoreStatuses(t.Context(), func(ctx context.Context, sql string) ([]tableRow, error) {
				if failure == "reset" && sql == "reset B" {
					return nil, errors.New("second reset failed")
				}
				return nil, nil
			})
			if err == nil || len(recovery.statusRestores) != 2 {
				t.Fatal("partial recovery lost ownership", err, recovery)
			}
			failSave = false
			recovery, err = readProxyEpochRecovery(path, save)
			if err != nil || len(recovery.statusRestores) != 2 {
				t.Fatal("partial recovery was not durable", err, recovery)
			}
			var calls []string
			err = recovery.restoreStatuses(t.Context(), func(ctx context.Context, sql string) ([]tableRow, error) {
				calls = append(calls, sql)
				return nil, nil
			})
			if err != nil || !reflect.DeepEqual(calls, []string{"reset A", "reset B"}) || len(recovery.statusRestores) != 0 {
				t.Fatal("did not retry all inverses", err, calls, recovery)
			}
		})
	}
}
