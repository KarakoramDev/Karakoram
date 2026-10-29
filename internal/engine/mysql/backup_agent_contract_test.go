// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	agentmysql "github.com/KarakoramDev/Karakoram/internal/agent/mysql"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Exercise the production agent parser and the CP operation together: a receipt
// fabricated from request.Type hides the native full-backuped/full distinction.
func TestBackupOperationAdmitsAgentParsedCheckpoints(t *testing.T) {
	p, rc, backend := backupFixture(t, "group")
	var request myspec.BackupRequest
	var parsed myspec.BackupResult
	backend.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
		switch r.URL.Path {
		case "/v1/mysql/backup":
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Error(err)
				return true
			}
			// These are native on-disk checkpoint contents, not a constructed
			// BackupResult. In particular a full is never called "full" here.
			checkpoint := "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 19418282\nlast_lsn = 19418282\nflushed_lsn = 19418282\n"
			if request.Type == "incremental" {
				checkpoint = "backup_type = incremental\nfrom_lsn = 19418282\nto_lsn = 19418299\nlast_lsn = 19418299\nflushed_lsn = 19418299\n"
			}
			dir := t.TempDir()
			for name, content := range map[string]string{
				"xtrabackup_checkpoints": checkpoint,
				"xtrabackup_info":        fmt.Sprintf("tool_version = 8.4.0-7\nserver_version = %s\nend_time = 2026-10-21 09:07:58\n", backend.native[member].Version),
				"xtrabackup_binlog_info": "binlog.000003\t198\taaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10\n",
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0600); err != nil {
					t.Error(err)
					return true
				}
			}
			var err error
			parsed, err = agentmysql.ParseBackupMetadata(dir)
			if err != nil {
				t.Error(err)
				return true
			}
			// Production runBackup attaches request provenance and the sealed
			// manifest after parsing. Do not replace any parsed metadata field.
			parsed.ObjectPrefix = request.ObjectPrefix
			parsed.PhysicalIdentity = request.PhysicalIdentity
			parsed.ManifestDigest, parsed.ManifestSize = fixtureManifestDigest, 1
			json.NewEncoder(w).Encode(myspec.BackupResponse{Receipt: request.Receipt, State: "running"})
			return true
		case "/v1/mysql/backup-info":
			json.NewEncoder(w).Encode(myspec.BackupResponse{Receipt: request.Receipt, State: "done", Result: &parsed})
			return true
		}
		return false
	}
	for _, kind := range []string{"full", "incremental"} {
		t.Run(kind, func(t *testing.T) {
			item := &domain.Backup{Namespace: "default", Cluster: "demo", Name: kind, Storage: "archive", Type: kind, Phase: domain.PhasePending}
			if err := rc.Store.CreateBackup(t.Context(), item); err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(map[string]string{"backup": kind})
			op := &domain.Operation{ID: "parsed-checkpoint-" + kind, Kind: domain.OperationBackup, Payload: payload}
			done := false
			for range 4 {
				result, err := p.RunOperation(t.Context(), rc, op)
				if err != nil {
					t.Fatal(err)
				}
				op.Checkpoint = result.Checkpoint
				if result.Done {
					done = true
					break
				}
			}
			saved, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), kind)
			if err != nil {
				t.Fatal(err)
			}
			if !done || saved.Phase != domain.PhaseSucceeded || saved.MySQL.FromLSN != parsed.FromLSN || saved.MySQL.ToLSN != parsed.ToLSN {
				t.Fatalf("parsed agent receipt not admitted: done=%v inventory=%+v", done, saved)
			}
		})
	}
}
