// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestArchivePendingMigrationIsDurableBeforeVersionCutover(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending-write-blocked=%t", blocked), func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "index.json")
			old := archiveIndex{Version: 1, Pending: map[string]bool{"source/binlog.000001": true}}
			old.Status.GTIDSet = recoverySID + ":1-3"
			if err := durableJSON(name, old); err != nil {
				t.Fatal(err)
			}
			if blocked {
				if err := os.Mkdir(name+".pending", 0700); err != nil {
					t.Fatal(err)
				}
			}
			migrated, err := loadArchiveState(name)
			if blocked {
				if err == nil {
					t.Fatal("migration ignored pending write failure")
				}
				authoritative, err := readArchiveIndex(name)
				if err != nil || authoritative.Version != 1 || !authoritative.Pending["source/binlog.000001"] {
					t.Fatal("failed migration replaced authoritative old index")
				}
				return
			}
			if err != nil || migrated.Version != 2 || !migrated.Pending["source/binlog.000001"] || migrated.Status.GTIDSet != old.Status.GTIDSet {
				t.Fatalf("migration lost state: %+v %v", migrated, err)
			}
			raw, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err = json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			if _, inline := fields["pending"]; inline {
				t.Fatal("v2 summary still embeds pending backlog")
			}
			if err = os.Remove(name + ".pending"); err != nil {
				t.Fatal(err)
			}
			if _, err = loadArchiveState(name); err == nil {
				t.Fatal("missing v2 pending inventory was treated as empty")
			}
		})
	}
}

func TestArchiveSummaryDoesNotRewriteLargePendingInventory(t *testing.T) {
	name := filepath.Join(t.TempDir(), "index.json")
	idx := archiveIndex{Version: 2, Pending: map[string]bool{}}
	for i := 1; i <= 10000; i++ {
		idx.Pending[fmt.Sprintf("%s/binlog.%06d", recoverySID, i)] = true
	}
	if err := writeArchivePending(name, idx.Pending); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(name + ".pending")
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() < 100000 {
		t.Fatal("fixture does not contain a large pending inventory")
	}
	for i := 1; i <= 100; i++ {
		idx.Status.LastArchivedFile = fmt.Sprintf("binlog.%06d", i)
		if err = writeArchiveSummary(name, idx); err != nil {
			t.Fatal(err)
		}
		summary, err := os.Stat(name)
		if err != nil || summary.Size() > 4096 {
			t.Fatalf("per-file summary scales with backlog: %v %v", summary, err)
		}
	}
	after, err := os.Stat(name + ".pending")
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("summary commits rewrote pending inventory")
	}
}

type pendingRecoveryConnector struct {
	*archiveMemoryConnector
	logs [][]driver.Value
}

func (c *pendingRecoveryConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *pendingRecoveryConnector) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if q == "SHOW BINARY LOGS" {
		return &nativeStatusRows{rows: c.logs}, nil
	}
	return c.archiveMemoryConnector.QueryContext(ctx, q, args)
}

func TestArchiveReconcilesStalePendingReceiptsBeforeNativePurgeGap(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(fmt.Sprintf("completed-receipt=%t", completed), func(t *testing.T) {
			dir := t.TempDir()
			p := paths{root: dir}
			repo := manifestS3Fixture(t, map[string][]byte{})
			name := archiveIndexPath(p, repo)
			oldKey := recoverySID + "/binlog.000001"
			activeKey := recoverySID + "/binlog.000002"
			idx := archiveIndex{Version: 2, Pending: map[string]bool{oldKey: true, activeKey: true}}
			idx.Status.GTIDSet = recoverySID + ":1-3"
			if completed {
				idx.Status.LastArchivedFile = "binlog.000001"
				idx.Status.GTIDSet = recoverySID + ":1-4"
			}
			if err := writeArchivePending(name, idx.Pending); err != nil {
				t.Fatal(err)
			}
			if err := writeArchiveSummary(name, idx); err != nil {
				t.Fatal(err)
			}
			// Crash boundary: summary and immutable receipt are durable, but the pending
			// batch has not been rewritten. Native expiry then removes the closed file.
			if completed {
				if err := durableJSON(archiveReceiptPath(name, oldKey), binlogSidecar{ServerUUID: recoverySID, File: "binlog.000001", GTIDSet: recoverySID + ":4"}); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "binlog.000002"), make([]byte, 512), 0600); err != nil {
				t.Fatal(err)
			}
			s := fakeMysqlbinlog(t, "exit 0\n")
			s.Backup = &myspec.Backup{Repository: repo, Binlog: myspec.Binlog{Enabled: true}}
			connector := &pendingRecoveryConnector{archiveMemoryConnector: &archiveMemoryConnector{groupTestConnector: groupTestConnector{t: t}, dir: dir}, logs: [][]driver.Value{{"binlog.000002", int64(512)}}}
			db := sql.OpenDB(connector)
			defer db.Close()
			if err := archiveNativeBinlogs(t.Context(), s, p, db, func(context.Context) (myspec.StatusResponse, error) {
				return myspec.StatusResponse{}, errors.New("unexpected rotation")
			}); err != nil {
				t.Fatal(err)
			}
			loaded, err := loadArchiveState(name)
			if err != nil {
				t.Fatal(err)
			}
			if completed {
				if loaded.Status.Gap || loaded.Pending[oldKey] || loaded.Status.GTIDSet != idx.Status.GTIDSet {
					t.Fatalf("completed stale pending created a false gap: %+v", loaded)
				}
			} else if !loaded.Status.Gap || loaded.Status.GapReason != "unarchived_binlog_purged" || !loaded.Pending[oldKey] {
				t.Fatalf("genuinely unarchived purged file was forgotten: %+v", loaded)
			}
		})
	}
}
