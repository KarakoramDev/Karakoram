// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Public-data mysqlbinlog-shaped headers model a member provisioned without
// historical binlogs: file 1 starts empty, subsequent files accumulate only the
// currently logged group SID. The historical SID remains in the physical full.
func purgedHistoryLog(t *testing.T, file, previous string, numbers ...int) archivedBinlog {
	t.Helper()
	text := "# at 127\n#700101  0:00:01 server id 1  end_log_pos 197 CRC32 0x00000000 Previous-GTIDs\n"
	if previous == "" {
		text += "# [empty]\n"
	} else {
		text += "# " + strings.ReplaceAll(previous, ",", ",\n# ") + "\n"
	}
	for i, n := range numbers {
		text += fmt.Sprintf("# original_commit_timestamp=%d\nSET @@SESSION.GTID_NEXT= '%s:%d'/*!*/;\n#700101  0:00:01 server id 1  end_log_pos %d CRC32 0x00000000 Xid = 1\nCOMMIT/*!*/;\n", 1000000+n*100000, recoverySID, n, 500+i*100)
	}
	meta, err := parseBinlogOutput([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	meta.ServerUUID, meta.File = recoverySID, file
	return archivedBinlog{key: file, meta: meta}
}

func TestPITRPurgedHistory(t *testing.T) {
	const historical = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff:1-100"
	for _, freshFull := range []bool{false, true} {
		t.Run(fmt.Sprintf("fresh-full-%t", freshFull), func(t *testing.T) {
			n, file := 3, 5
			if freshFull {
				n, file = 5, 6
			}
			backup := myspec.BackupResult{GTIDSet: historical + "," + fmt.Sprintf("%s:1-%d", recoverySID, n), EndTime: time.UnixMicro(int64(1000000 + n*100000)), SourceUUID: recoverySID, BinlogFile: fmt.Sprintf("binlog.%06d", file), BinlogPosition: 500}
			logs := []archivedBinlog{
				purgedHistoryLog(t, "binlog.000001", "", 1),
				purgedHistoryLog(t, backup.BinlogFile, fmt.Sprintf("%s:1-%d", recoverySID, n-1), n, n+1),
				purgedHistoryLog(t, fmt.Sprintf("binlog.%06d", file+1), fmt.Sprintf("%s:1-%d", recoverySID, n+1), n+2),
			}
			target := historical + "," + fmt.Sprintf("%s:1-%d", recoverySID, n+2)
			selected, _, err := selectBinlogs(logs, backup, target, "")
			if err != nil || len(selected) != 2 {
				t.Fatalf("post-full target rejected: files=%d err=%v", len(selected), err)
			}
			coverage, err := replayCoverage(logs, backup)
			if err != nil || len(coverage.Windows) != 1 || coverage.Windows[0].End.UnixMicro() != int64(1000000+(n+2)*100000) {
				t.Fatalf("coverage failed to advance: %+v %v", coverage, err)
			}
			// A reset/divergent file cannot discard previously logged history,
			// even when every omitted baseline GTID is safely in the full.
			previous := logs[2].meta.PreviousGTIDSet
			logs[2].meta.PreviousGTIDSet = fmt.Sprintf("%s:1-%d", recoverySID, n)
			if _, _, err := selectBinlogs(logs, backup, target, ""); err == nil {
				t.Fatal("accepted regressing logged history")
			}
			coverage, err = replayCoverage(logs, backup)
			if err != nil || coverage.BlockedReason != "baseline_logged_gtids_missing" {
				t.Fatalf("missing blocked-chain diagnostic: %+v %v", coverage, err)
			}
			logs[2].meta.PreviousGTIDSet = previous
			// Removing the anchor tail must not authorize the next file's dependency.
			logs[1] = purgedHistoryLog(t, backup.BinlogFile, fmt.Sprintf("%s:1-%d", recoverySID, n-1), n)
			if _, _, err := selectBinlogs(logs, backup, target, ""); err == nil {
				t.Fatal("accepted missing post-snapshot transaction")
			}
			coverage, err = replayCoverage(logs, backup)
			if err != nil || coverage.BlockedReason != "binlog_dependency_missing" {
				t.Fatalf("missing dependency diagnostic: %+v %v", coverage, err)
			}
			// Without the exact anchor there is no evidence for omitting historical.
			if _, _, err := selectBinlogs(logs[2:], backup, target, ""); err == nil {
				t.Fatal("accepted unauthenticated boundary")
			}
		})
	}
}

// Reuse the real mysqlbinlog 8.4.11 output captured for query-event parsing.
// Only the file boundary and an imported historical SID are synthesized.
func TestPITRPurgedHistoryNativeFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/mysqlbinlog-queries.txt")
	if err != nil {
		t.Fatal(err)
	}
	meta, err := parseBinlogOutput(raw)
	if err != nil || len(meta.Transactions) < 3 {
		t.Fatalf("native fixture: %+v %v", meta, err)
	}
	txs := meta.Transactions
	base := "bbbbbbbb-cccc-dddd-eeee-ffffffffffff:1-100," + txs[0].GTID
	backup := myspec.BackupResult{GTIDSet: base, SourceUUID: recoverySID, BinlogFile: "binlog.000001", BinlogPosition: txs[0].EndPosition, EndTime: time.UnixMicro(txs[0].OriginalCommitTimestamp)}
	first := binlogSidecar{ServerUUID: recoverySID, File: backup.BinlogFile, GTIDSet: recoverySID + ":1-2", Transactions: txs[:2]}
	second := binlogSidecar{ServerUUID: recoverySID, File: "binlog.000002", PreviousGTIDSet: first.GTIDSet, GTIDSet: txs[2].GTID, Transactions: txs[2:3]}
	logs := []archivedBinlog{{key: first.File, meta: first}, {key: second.File, meta: second}}
	if _, _, err := selectBinlogs(logs, backup, "bbbbbbbb-cccc-dddd-eeee-ffffffffffff:1-100,"+recoverySID+":1-3", ""); err != nil {
		t.Fatal(err)
	}
	coverage, err := replayCoverage(logs, backup)
	if err != nil || coverage.BlockedReason != "" || len(coverage.Windows) != 1 || coverage.Windows[0].End.UnixMicro() != txs[2].OriginalCommitTimestamp {
		t.Fatalf("native fixture coverage: %+v %v", coverage, err)
	}
}
