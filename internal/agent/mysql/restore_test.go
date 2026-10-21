// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

const recoverySID = "11111111-2222-3333-4444-555555555555"

func TestRestorePrepareArguments(t *testing.T) {
	got := prepareArguments("full", []string{"inc1", "inc2"})
	want := [][]string{{"--prepare", "--apply-log-only", "--target-dir=full"}, {"--prepare", "--target-dir=full", "--incremental-dir=inc1", "--apply-log-only"}, {"--prepare", "--target-dir=full", "--incremental-dir=inc2"}, {"--prepare", "--target-dir=full"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if got := prepareArguments("full", nil); len(got) != 2 {
		t.Fatalf("full-only: %v", got)
	}
}
func TestRestoreFence(t *testing.T) {
	for _, tc := range []struct {
		role   string
		intent bool
		pid    int
		ok     bool
	}{{myspec.RoleData, true, 0, true}, {myspec.RoleData, false, 0, false}, {myspec.RoleData, true, 10, false}, {myspec.RoleProxy, true, 0, false}} {
		if got := restoreFenceIntent(tc.role, tc.intent, tc.pid) == nil; got != tc.ok {
			t.Fatalf("%+v allowed=%v", tc, got)
		}
	}
}
func TestRestoreReceiptResumeStages(t *testing.T) {
	for _, stage := range []string{"fetch", "prepare", "ready", "move-back", "replay", "finalize"} {
		t.Run(stage, func(t *testing.T) {
			p := dataPaths(t.TempDir())
			if err := privateDir(p.root); err != nil {
				t.Fatal(err)
			}
			name, err := receiptPath(p, "restore", "test")
			if err != nil {
				t.Fatal(err)
			}
			req := myspec.RestoreRequest{Receipt: "test", Restore: myspec.Restore{ObjectPaths: []string{"full"}}}
			r := restoreReceipt{Hash: restoreHash(req), Status: myspec.RestoreStatus{Receipt: "test", State: "running", Stage: stage}, Active: true}
			if err = durableJSON(name, r); err != nil {
				t.Fatal(err)
			}
			loaded, err := loadRestoreReceipt(name)
			if err != nil {
				t.Fatal(err)
			}
			if restoreInterrupted(loaded) != (stage != "fetch" && stage != "finalize") {
				t.Fatal("unsafe resume policy")
			}
			loaded.Active = false
			if restoreInterrupted(loaded) {
				t.Fatal("completed stage did not resume")
			}
			if restoreStartGuard(p) == nil {
				t.Fatal("incomplete restore allowed startup")
			}
			loaded.Status.State = "done"
			if err = durableJSON(name, loaded); err != nil {
				t.Fatal(err)
			}
			if err = restoreStartGuard(p); err != nil {
				t.Fatal(err)
			}
			req.Restore.TargetGTID = recoverySID + ":1"
			if loaded.Hash == restoreHash(req) {
				t.Fatal("changed restore reused receipt")
			}
			if _, err = loadRestoreReceipt(filepath.Join(p.root, "missing")); err == nil {
				t.Fatal("missing receipt accepted")
			}
		})
	}
}
func TestPITROriginalSourceCoverageAndForeignRefusal(t *testing.T) {
	base := recoverySID + ":1-3"
	end := time.UnixMicro(1000000)
	backup := myspec.BackupResult{GTIDSet: base, EndTime: end, SourceUUID: recoverySID, BinlogFile: "binlog.000009", BinlogPosition: 4}
	logs := []archivedBinlog{{"binlogs/writer-b/binlog.000001", binlogSidecar{PreviousGTIDSet: recoverySID + ":1-4", GTIDSet: recoverySID + ":5", Transactions: []binlogTransaction{{GTID: recoverySID + ":5", OriginalCommitTimestamp: 1200000}}}}, {"binlogs/writer-a/binlog.000010", binlogSidecar{PreviousGTIDSet: base, GTIDSet: recoverySID + ":4", Transactions: []binlogTransaction{{GTID: recoverySID + ":4", OriginalCommitTimestamp: 1100000}}}}, {"binlogs/writer-c/binlog.000003", binlogSidecar{PreviousGTIDSet: base, GTIDSet: recoverySID + ":4", Transactions: []binlogTransaction{{GTID: recoverySID + ":4", OriginalCommitTimestamp: 1100000}}}}}
	for i := range logs {
		logs[i].meta.ServerUUID = recoverySID
		logs[i].meta.File = filepath.Base(logs[i].key)
	}
	logs[0].meta.File = "binlog.000011"
	logs[2].meta.ServerUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	selected, target, err := selectBinlogs(logs, backup, recoverySID+":1-5", "")
	if err != nil || target != recoverySID+":1-5" || len(selected) != 2 {
		t.Fatalf("selection=%v target=%s err=%v", selected, target, err)
	}
	_, _, err = selectBinlogs(logs[:1], backup, recoverySID+":1-5", "")
	if err == nil {
		t.Fatal("accepted GTID gap")
	}
}
func TestPITRTimeMicroseconds(t *testing.T) {
	base := recoverySID + ":1-3"
	end := time.UnixMicro(1000000)
	backup := myspec.BackupResult{GTIDSet: base, EndTime: end, SourceUUID: recoverySID, BinlogFile: "binlog.000009", BinlogPosition: 4}
	logs := []archivedBinlog{{"binlog", binlogSidecar{PreviousGTIDSet: base, GTIDSet: recoverySID + ":4-5", Transactions: []binlogTransaction{{GTID: recoverySID + ":4", OriginalCommitTimestamp: 1100000}, {GTID: recoverySID + ":5", OriginalCommitTimestamp: 1900000}}}}}
	logs[0].meta.ServerUUID, logs[0].meta.File = recoverySID, "binlog.000010"
	_, target, err := selectBinlogs(logs, backup, "", time.UnixMicro(1500000).UTC().Format(time.RFC3339Nano))
	if err != nil || target != recoverySID+":1-4" {
		t.Fatalf("target=%s err=%v", target, err)
	}
	for _, micros := range []int64{999999, 1900001} {
		if _, _, err = selectBinlogs(logs, backup, "", time.UnixMicro(micros).UTC().Format(time.RFC3339Nano)); err == nil {
			t.Fatalf("accepted uncovered target %d", micros)
		}
	}
}
func TestParseBinlogOriginalCommitTimestamp(t *testing.T) {
	data := []byte("# Previous-GTIDs\n# " + recoverySID + ":1-3\n# original_commit_timestamp=1100000 (x)\n# immediate_commit_timestamp=9999999 (x)\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":4'/*!*/;\n# end_log_pos 486 CRC32 0 Xid = 10\nCOMMIT/*!*/;\n# original_commit_timestamp=1900000 (x)\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":5'/*!*/;\n# end_log_pos 600 CRC32 0 Xid = 11\nCOMMIT/*!*/;\n")
	out, err := parseBinlogOutput(data)
	if err != nil || len(out.Transactions) != 2 || out.Transactions[0].OriginalCommitTimestamp != 1100000 || out.Transactions[1].EndPosition != 600 || out.GTIDSet != recoverySID+":4-5" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestRestoreOrphanStatusFailsClosed(t *testing.T) {
	p := dataPaths(t.TempDir())
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	name, err := receiptPath(p, "restore", "orphan")
	if err != nil {
		t.Fatal(err)
	}
	if err = durableJSON(name, restoreReceipt{Status: myspec.RestoreStatus{State: "running", Stage: "move-back"}, Active: true}); err != nil {
		t.Fatal(err)
	}
	r, err := observeRestoreReceipt(name)
	if err != nil || r.Status.State != "failed" || r.Status.Error != "interrupted" {
		t.Fatalf("receipt=%+v err=%v", r, err)
	}
}

func TestParseBinlogDDLBoundary(t *testing.T) {
	data := []byte("# original_commit_timestamp=1100000\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":1'/*!*/;\n# end_log_pos 300 Query\nCREATE DATABASE restored\n/*!*/;\n# end_log_pos 379 GTID\tlast_committed=1\n# original_commit_timestamp=1200000\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":2'/*!*/;\n# end_log_pos 500 Query\nCREATE TABLE restored.t(id int)\n/*!*/;\n# end_log_pos 544 Rotate to binlog.000002\n")
	out, err := parseBinlogOutput(data)
	if err != nil || len(out.Transactions) != 2 || out.Transactions[0].EndPosition != 300 || out.Transactions[1].EndPosition != 500 {
		t.Fatalf("sidecar=%+v err=%v", out, err)
	}
}

func TestRestoreReplicationIdentityProviderBounds(t *testing.T) {
	pxc := testGaleraSpec()
	request := myspec.Restore{
		Source:          testBackupRepository(),
		ObjectPaths:     []string{"full"},
		ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: "full", Digest: physicalManifestTestDigest, Size: 1}},
		NewGroupName:    "9347c03140ab4a90b950ec3525ae4321",
	}
	if err := validateRestore(pxc, request); err != nil {
		t.Fatal("PXC rejected new 32-byte identity", err)
	}
	for _, invalid := range []string{"", pxc.Galera.ClusterName, "9347c031-40ab-4a90-b950-ec3525ae4321", strings.Repeat("a", 33)} {
		bad := request
		bad.NewGroupName = invalid
		if err := validateRestore(pxc, bad); err == nil {
			t.Fatalf("PXC restore accepted invalid or retained cluster identity %q", invalid)
		}
	}
	group := testGroupSpec()
	group.Topology = myspec.TopologyGroup
	if err := validateRestore(group, request); err == nil {
		t.Fatal("GR restore accepted 32-character non-UUID PXC identity")
	}
	request.NewGroupName = "9347c031-40ab-4a90-b950-ec3525ae4321"
	if err := validateRestore(group, request); err != nil {
		t.Fatal("GR restore rejected new UUID identity", err)
	}
}

func TestPITRExactSnapshotAnchorExcludesStaleAliases(t *testing.T) {
	const foreign = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	backup := myspec.BackupResult{GTIDSet: recoverySID + ":1-10", EndTime: time.UnixMicro(2000000), SourceUUID: recoverySID, BinlogFile: "binlog.000007", BinlogPosition: 500}
	logs := []archivedBinlog{
		{"stale-source/binlog.000001", binlogSidecar{ServerUUID: foreign, File: "binlog.000001", GTIDSet: foreign + ":1", Transactions: []binlogTransaction{{GTID: foreign + ":1", OriginalCommitTimestamp: 1000000, EndPosition: 300}}}},
		// Same-SID shifted alias predates the exact snapshot position even
		// though its GTID is absent from the full backup's executed set.
		{"source/binlog.000007", binlogSidecar{ServerUUID: recoverySID, File: "binlog.000007", PreviousGTIDSet: recoverySID + ":1-9", GTIDSet: recoverySID + ":10-12", Transactions: []binlogTransaction{
			{GTID: recoverySID + ":11", OriginalCommitTimestamp: 1800000, EndPosition: 400},
			{GTID: recoverySID + ":10", OriginalCommitTimestamp: 1900000, EndPosition: 500},
			{GTID: recoverySID + ":12", OriginalCommitTimestamp: 2100000, EndPosition: 600},
		}}},
	}
	for _, targetTime := range []string{"", time.UnixMicro(2100000).UTC().Format(time.RFC3339Nano)} {
		targetGTID := ""
		if targetTime == "" {
			targetGTID = recoverySID + ":1-10:12"
		}
		selected, target, err := selectBinlogs(logs, backup, targetGTID, targetTime)
		if err != nil || target != recoverySID+":1-10:12" || len(selected) != 1 {
			t.Fatalf("target=%s selected=%v err=%v", target, selected, err)
		}
	}
	if _, _, err := selectBinlogs(logs, backup, recoverySID+":1-12", ""); err == nil {
		t.Fatal("stale same-SID alias was admitted")
	}
}

func TestPITRForeignWriterNeedsFreshFullEvenWithMatchingGTIDPrefix(t *testing.T) {
	const inverse = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	backup := myspec.BackupResult{GTIDSet: recoverySID + ":1-10," + inverse + ":1", EndTime: time.UnixMicro(2000000), SourceUUID: recoverySID, BinlogFile: "binlog.000007", BinlogPosition: 500}
	logs := []archivedBinlog{{"other/binlog.000008", binlogSidecar{ServerUUID: inverse, File: "binlog.000008", PreviousGTIDSet: recoverySID + ":1-10", GTIDSet: recoverySID + ":11", Transactions: []binlogTransaction{{GTID: recoverySID + ":11", OriginalCommitTimestamp: 2100000, EndPosition: 600}}}}}
	if _, _, err := selectBinlogs(logs, backup, recoverySID+":1-11,"+inverse+":1", ""); err == nil {
		t.Fatal("canonical stale branch crossed errant baseline")
	}
	logs[0].meta.PreviousGTIDSet = backup.GTIDSet
	if _, _, err := selectBinlogs(logs, backup, recoverySID+":1-11,"+inverse+":1", ""); err == nil {
		t.Fatal("matching numeric prefix authenticated a foreign row history")
	}
	if _, _, err := selectBinlogs(logs, backup, "", time.UnixMicro(2100000).UTC().Format(time.RFC3339Nano)); err == nil {
		t.Fatal("time restore accepted foreign shifted-counter alias")
	}
	backup.SourceUUID = inverse // a fresh full from the actual new writer
	if _, _, err := selectBinlogs(logs, backup, recoverySID+":1-11,"+inverse+":1", ""); err != nil {
		t.Fatalf("fresh writer baseline rejected: %v", err)
	}
	backup.SourceUUID = ""
	if _, _, err := selectBinlogs(logs, backup, recoverySID+":1-11,"+inverse+":1", ""); err == nil {
		t.Fatal("unanchored legacy snapshot admitted")
	}
}

func TestPITRStartPositionReachesRealReplayCommand(t *testing.T) {
	backup := myspec.BackupResult{SourceUUID: recoverySID, BinlogFile: "binlog.000007", BinlogPosition: 500}
	selected := []archivedBinlog{
		{"other-stream", binlogSidecar{ServerUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", File: "binlog.000001"}},
		{"snapshot-tail", binlogSidecar{ServerUUID: recoverySID, File: backup.BinlogFile}},
		{"later-stream", binlogSidecar{ServerUUID: recoverySID, File: "binlog.000008"}},
	}
	position := replayStartPosition(selected, backup)
	if position != 500 || selected[0].key != "snapshot-tail" || selected[1].key != "other-stream" || selected[2].key != "later-stream" {
		t.Fatalf("snapshot ordering: %v position=%d", selected, position)
	}
	args := replayBinlogArguments(replayLogs{Files: []replayFile{{Path: "snapshot-tail"}, {Path: "other-stream"}, {Path: "later-stream"}}, StartPosition: position}, "base", "target")
	want := []string{"--skip-gtids=false", "--exclude-gtids=base", "--include-gtids=target", "--start-position=500", "snapshot-tail", "other-stream", "later-stream"}
	if !reflect.DeepEqual(args, want) {
		t.Fatalf("replay arguments: %v", args)
	}
}

func TestPITRTargetsMustBeReplayClosed(t *testing.T) {
	backup := myspec.BackupResult{GTIDSet: recoverySID + ":1-3", EndTime: time.UnixMicro(100), SourceUUID: recoverySID, BinlogFile: "binlog.000007", BinlogPosition: 500}
	for _, split := range []bool{false, true} {
		t.Run(fmt.Sprint(split), func(t *testing.T) {
			logs := []archivedBinlog{{"first", binlogSidecar{ServerUUID: recoverySID, File: "binlog.000008", PreviousGTIDSet: backup.GTIDSet, GTIDSet: recoverySID + ":4-5", Transactions: []binlogTransaction{{GTID: recoverySID + ":4", OriginalCommitTimestamp: 200, EndPosition: 600}, {GTID: recoverySID + ":5", OriginalCommitTimestamp: 150, EndPosition: 700}}}}}
			if split {
				logs[0].meta.GTIDSet = recoverySID + ":4"
				logs[0].meta.Transactions = logs[0].meta.Transactions[:1]
				logs = append(logs, archivedBinlog{"second", binlogSidecar{ServerUUID: recoverySID, File: "binlog.000009", PreviousGTIDSet: recoverySID + ":1-4", GTIDSet: recoverySID + ":5", Transactions: []binlogTransaction{{GTID: recoverySID + ":5", OriginalCommitTimestamp: 150, EndPosition: 600}}}})
			}
			if _, _, err := selectBinlogs(logs, backup, recoverySID+":1-3:5", ""); err == nil {
				t.Fatal("explicit target omitted an application predecessor")
			}
			if _, _, err := selectBinlogs(logs, backup, "", time.UnixMicro(175).UTC().Format(time.RFC3339Nano)); err == nil {
				t.Fatal("regressing original timestamp created a replay hole")
			}
			if _, target, err := selectBinlogs(logs, backup, "", time.UnixMicro(200).UTC().Format(time.RFC3339Nano)); err != nil || target != recoverySID+":1-5" {
				t.Fatalf("closed prefix target=%s err=%v", target, err)
			}
			windows, err := replayCoverageWindows(logs, backup)
			if err != nil || len(windows) != 2 || !windows[0].Start.Equal(time.UnixMicro(100)) || !windows[0].End.Equal(time.UnixMicro(149)) || !windows[1].Start.Equal(time.UnixMicro(200)) || !windows[1].End.Equal(time.UnixMicro(200)) {
				t.Fatalf("unsafe advertised windows: %v err=%v", windows, err)
			}
		})
	}
}

func TestPITRForeignSameSIDAliasDoesNotExtendCoverage(t *testing.T) {
	backup := myspec.BackupResult{GTIDSet: recoverySID + ":1-11", EndTime: time.UnixMicro(200), SourceUUID: recoverySID, BinlogFile: "binlog.000007", BinlogPosition: 500}
	foreign := []archivedBinlog{{"stale", binlogSidecar{ServerUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", File: "binlog.000008", PreviousGTIDSet: backup.GTIDSet, GTIDSet: recoverySID + ":12", Transactions: []binlogTransaction{{GTID: recoverySID + ":12", OriginalCommitTimestamp: 210, EndPosition: 600}}}}}
	for _, byTime := range []bool{false, true} {
		gtid, targetTime := recoverySID+":1-12", ""
		if byTime {
			gtid, targetTime = "", time.UnixMicro(210).UTC().Format(time.RFC3339Nano)
		}
		if _, _, err := selectBinlogs(foreign, backup, gtid, targetTime); err == nil {
			t.Fatal("foreign same-SID shifted alias admitted")
		}
	}
	windows, err := replayCoverageWindows(foreign, backup)
	if err != nil || len(windows) != 1 || !windows[0].End.Equal(backup.EndTime) {
		t.Fatalf("foreign numeric union extended coverage: %v err=%v", windows, err)
	}
}

func TestRestorePreflightPlanFailureLeavesLiveDataUntouched(t *testing.T) {
	dir := t.TempDir()
	e := &Runner{dir: dir, current: myspec.Spec{Role: myspec.RoleData, Topology: myspec.TopologySingle}}
	_, p := e.state()
	if err := privateDir(p.data); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(p.data, "application-data")
	if err := os.WriteFile(marker, []byte("original live data"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := privateDir(p.root); err != nil {
		t.Fatal(err)
	}
	startup := filepath.Join(p.root, "startup.sql")
	if err := os.WriteFile(startup, []byte("original startup configuration"), 0600); err != nil {
		t.Fatal(err)
	}
	request := myspec.RestoreRequest{Receipt: "preflight-proof", Restore: myspec.Restore{Source: testBackupRepository(), BackupID: "base", Label: "base", ObjectPaths: []string{"repo/base/full"}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: "repo/base/full", Digest: physicalManifestTestDigest, Size: 1}}}}
	name, err := receiptPath(p, "restore", request.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	stage := name + ".staging"
	if err := privateDir(stage); err != nil {
		t.Fatal(err)
	}
	full := filepath.Join(stage, "0")
	if err := privateDir(full); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(full, "ibdata1"), []byte("prepared physical data"), 0600); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(stage, "replay-000000")
	if err := os.WriteFile(file, []byte("verified replay bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	hash, size, err := hashFile(file)
	if err != nil {
		t.Fatal(err)
	}
	receipt := restoreReceipt{Hash: restoreHash(request), SafePreflight: true, Status: myspec.RestoreStatus{Receipt: request.Receipt, State: "prepared", Stage: "ready"}, Replay: &restoreReplayPlan{Target: recoverySID + ":1", Logs: replayLogs{Files: []replayFile{{Path: file, ObjectKey: "repo/binlogs/source/binlog.000001", SHA256: hash, Size: size}}, StartPosition: 198}}}
	receipt.Replay.Physical, err = preparedPhysicalFiles(full)
	if err != nil {
		t.Fatal(err)
	}
	receipt.Status.PlanDigest, err = frozenRestorePlanDigest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := durableJSON(name, receipt); err != nil {
		t.Fatal(err)
	}
	if status, err := e.restore(t.Context(), nil, request, true); err != nil || status.State != "prepared" {
		t.Fatalf("prepared plan: %+v err=%v", status, err)
	}
	if err := os.WriteFile(file, []byte("changed after preflight"), 0600); err != nil {
		t.Fatal(err)
	}
	status, err := e.restore(t.Context(), nil, request, true)
	if err != nil || status.State != "failed" || status.Error != "replay_plan_changed" {
		t.Fatalf("changed plan: %+v err=%v", status, err)
	}
	if raw, err := os.ReadFile(marker); err != nil || string(raw) != "original live data" {
		t.Fatalf("live data changed: %q err=%v", raw, err)
	}
	if _, err := os.Stat(name + ".previous"); !os.IsNotExist(err) {
		t.Fatal("preflight renamed live datadir")
	}
	if err := restoreStartGuard(p); err != nil {
		t.Fatalf("side-effect-free failure fenced normal startup: %v", err)
	}
	if raw, err := os.ReadFile(startup); err != nil || string(raw) != "original startup configuration" {
		t.Fatalf("preflight modified startup SQL: %q err=%v", raw, err)
	}
	failed, err := loadRestoreReceipt(name)
	if err != nil {
		t.Fatal(err)
	}
	failed.SafePreflight = false // a genuinely destructive failed installation
	if err := durableJSON(name, failed); err != nil {
		t.Fatal(err)
	}
	if err := restoreStartGuard(p); err == nil {
		t.Fatal("destructive failure no longer fences startup")
	}
}

func TestPhysicalPreflightFailureDoesNotInstallOrFenceLiveData(t *testing.T) {
	e := &Runner{dir: t.TempDir(), current: myspec.Spec{Role: myspec.RoleData, Topology: myspec.TopologySingle}}
	_, p := e.state()
	if err := privateDir(p.data); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(p.data, "live-data")
	if err := os.WriteFile(marker, []byte("unchanged live dataset"), 0600); err != nil {
		t.Fatal(err)
	}
	request := myspec.RestoreRequest{Receipt: "physical-preflight", Restore: myspec.Restore{Source: testBackupRepository(), BackupID: "full", Label: "full", ObjectPaths: []string{"repo/base/full"}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: "repo/base/full", Digest: physicalManifestTestDigest, Size: 1}}}}
	name, err := receiptPath(p, "restore", request.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	stage := name + ".staging"
	full := filepath.Join(stage, "0")
	if err := privateDir(full); err != nil {
		t.Fatal(err)
	}
	physicalPath := filepath.Join(full, "ibdata1")
	if err := os.WriteFile(physicalPath, []byte("original prepared dataset"), 0600); err != nil {
		t.Fatal(err)
	}
	physical, err := preparedPhysicalFiles(full)
	if err != nil {
		t.Fatal(err)
	}
	receipt := restoreReceipt{Hash: restoreHash(request), SafePreflight: true, Status: myspec.RestoreStatus{Receipt: request.Receipt, State: "prepared", Stage: "ready"}, Replay: &restoreReplayPlan{Target: recoverySID + ":1", Physical: physical}}
	receipt.Status.PlanDigest, err = frozenRestorePlanDigest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := durableJSON(name, receipt); err != nil {
		t.Fatal(err)
	}
	request.PlanDigest = receipt.Status.PlanDigest
	if restoreHash(request) != receipt.Hash {
		t.Fatal("derived plan pin changed immutable intent identity")
	}
	if err := os.WriteFile(physicalPath, []byte("substituted prepared dataset"), 0600); err != nil {
		t.Fatal(err)
	}
	if status, err := e.restore(t.Context(), nil, request, true); err != nil || status.State != "failed" || status.Error != "replay_plan_changed" {
		t.Fatalf("physical tamper status=%+v err=%v", status, err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "unchanged live dataset" {
		t.Fatalf("preflight changed live data: %q err=%v", data, err)
	}
	if _, err := os.Stat(name + ".previous"); !os.IsNotExist(err) {
		t.Fatal("physical tamper reached datadir rename")
	}
	if err := restoreStartGuard(p); err != nil {
		t.Fatalf("non-destructive failure prevented normal restart: %v", err)
	}
}
