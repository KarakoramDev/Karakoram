// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// frozenBinlogParser is the fac5447b parser, retained only as a differential
// oracle. In particular, its Previous-GTIDs handling is not authoritative:
// it drops comma-terminated UUID lines, which the streaming parser must retain.
func frozenBinlogParser(data []byte) (binlogSidecar, error) {
	var out binlogSidecar
	scan := bufio.NewScanner(bytes.NewReader(data))
	scan.Buffer(make([]byte, 4096), 4<<20)
	var tx binlogTransaction
	var position uint64
	previous := false
	finish := func() error {
		if tx.GTID == "" {
			return nil
		}
		if tx.OriginalCommitTimestamp == 0 {
			return errors.New("missing_commit_timestamp")
		}
		tx.EndPosition = position
		out.Transactions = append(out.Transactions, tx)
		tx = binlogTransaction{}
		return nil
	}
	for scan.Scan() {
		line := scan.Text()
		if strings.Contains(line, "GTID\t") || strings.Contains(line, "Rotate to ") {
			if err := finish(); err != nil {
				return out, err
			}
		}
		if m := binlogPosition.FindStringSubmatch(line); m != nil {
			position, _ = strconv.ParseUint(m[1], 10, 64)
		}
		if strings.Contains(line, "Previous-GTIDs") {
			previous = true
			continue
		}
		if previous {
			v := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			if v == "[empty]" {
				previous = false
			} else if strings.HasPrefix(line, "# ") && !strings.Contains(v, " ") {
				if _, err := parseRecoveryGTIDs(v); err == nil {
					if out.PreviousGTIDSet != "" {
						out.PreviousGTIDSet += ","
					}
					out.PreviousGTIDSet += v
				} else {
					previous = false
				}
			} else {
				previous = false
			}
		}
		if m := binlogCommit.FindStringSubmatch(line); m != nil {
			if err := finish(); err != nil {
				return out, err
			}
			tx.OriginalCommitTimestamp, _ = strconv.ParseInt(m[1], 10, 64)
		}
		if m := binlogGTID.FindStringSubmatch(line); m != nil && m[1] != "AUTOMATIC" {
			if _, err := parseRecoveryGTIDs(m[1]); err != nil {
				return out, err
			}
			tx.GTID = m[1]
		}
		if tx.GTID != "" && (strings.Contains(line, "Xid =") || strings.HasPrefix(line, "COMMIT") || strings.Contains(line, "COMMIT/*!*/")) {
			if tx.OriginalCommitTimestamp == 0 {
				return out, errors.New("missing_commit_timestamp")
			}
			tx.EndPosition = position
			out.Transactions = append(out.Transactions, tx)
			tx = binlogTransaction{}
		}
	}
	if err := scan.Err(); err != nil {
		return out, err
	}
	if err := finish(); err != nil {
		return out, err
	}
	set := recoveryGTIDs{}
	for _, tx := range out.Transactions {
		g, _ := parseRecoveryGTIDs(tx.GTID)
		set.add(g)
		if out.FirstCommitTimestamp == 0 || tx.OriginalCommitTimestamp < out.FirstCommitTimestamp {
			out.FirstCommitTimestamp = tx.OriginalCommitTimestamp
		}
		if tx.OriginalCommitTimestamp > out.LastCommitTimestamp {
			out.LastCommitTimestamp = tx.OriginalCommitTimestamp
		}
	}
	out.GTIDSet = set.String()
	if out.FirstCommitTimestamp != 0 {
		out.FirstEventTime = time.UnixMicro(out.FirstCommitTimestamp).UTC()
		out.LastEventTime = time.UnixMicro(out.LastCommitTimestamp).UTC()
	}
	return out, nil
}

// Hand-authored public-data fixtures model native MySQL/Percona output. No lab
// binlogs or SQL have been copied. Both timestamp spellings deliberately appear
// before GTID_NEXT, as emitted by mysqlbinlog 8.0 and 8.4.
func nativeGTIDHeader(version string, number int, position int) string {
	return fmt.Sprintf("# at %d\n#700101  0:00:01 server id 1  end_log_pos %d CRC32 0x00000000 GTID\tlast_committed=0\tsequence_number=%d\trbr_only=yes\toriginal_committed_timestamp=1100000\timmediate_commit_timestamp=1100000\ttransaction_length=200\n# original_commit_timestamp=1100000 (1970-01-01 00:00:01.100000 UTC)\n# immediate_commit_timestamp=1100000 (1970-01-01 00:00:01.100000 UTC)\n/*!80001 SET @@session.original_commit_timestamp=1100000*//*!*/;\n/*!80014 SET @@session.original_server_version=%s*//*!*/;\n/*!80014 SET @@session.immediate_server_version=%s*//*!*/;\nSET @@SESSION.GTID_NEXT= '%s:%d'/*!*/;\n", position-80, position, number, version, version, recoverySID, number)
}

func TestNativeBinlogFixturesDifferential(t *testing.T) {
	otherSID := "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	previous := "# at 127\n#700101  0:00:01 server id 1  end_log_pos 200 CRC32 0x00000000 Previous-GTIDs\n# " + recoverySID + ":1-3,\n# " + otherSID + ":1-7\n"
	rows := "# at 280\n#700101  0:00:01 server id 1  end_log_pos 350 CRC32 0x00000000 Query\tthread_id=1\texec_time=0\terror_code=0\nSET TIMESTAMP=1/*!*/;\nBEGIN\n/*!*/;\n# at 350\n#700101  0:00:01 server id 1  end_log_pos 400 CRC32 0x00000000 Table_map: `example`.`items` mapped to number 90\n# at 400\n#700101  0:00:01 server id 1  end_log_pos 450 CRC32 0x00000000 Write_rows: table id 90 flags: STMT_END_F\n### INSERT INTO `example`.`items`\n### SET\n###   @1=1\n###   @2='first line\nsecond line'\n### INSERT INTO `example`.`items`\n### SET\n###   @1=2\n###   @2='another row'\n# at 450\n#700101  0:00:01 server id 1  end_log_pos 486 CRC32 0x00000000 Xid = 10\nCOMMIT/*!*/;\n"
	ddl := "# at 566\n#700101  0:00:01 server id 1  end_log_pos 650 CRC32 0x00000000 Query\tthread_id=1\texec_time=0\terror_code=0\nSET TIMESTAMP=1/*!*/;\nCREATE TABLE example.created (id int)\n/*!*/;\n"
	view := "# at 730\n#700101  0:00:01 server id 1  end_log_pos 780 CRC32 0x00000000 Query\tthread_id=0\texec_time=0\terror_code=0\nBEGIN\n/*!*/;\n# at 780\n#700101  0:00:01 server id 1  end_log_pos 850 CRC32 0x00000000 View_change\n# view_id=123456789:1\n# at 850\n#700101  0:00:01 server id 1  end_log_pos 900 CRC32 0x00000000 Query\tthread_id=0\texec_time=0\terror_code=0\nCOMMIT\n/*!*/;\n"
	rotate := "# at 900\n#700101  0:00:01 server id 1  end_log_pos 944 CRC32 0x00000000 Rotate to binlog.000002  pos: 4\n"
	for _, version := range []string{"80036", "80411"} {
		for name, text := range map[string]string{
			"multi-row":   previous + nativeGTIDHeader(version, 4, 280) + rows + rotate,
			"ddl":         previous + nativeGTIDHeader(version, 4, 566) + ddl + rotate,
			"view-change": previous + nativeGTIDHeader(version, 4, 730) + view + rotate,
			"mixed":       previous + nativeGTIDHeader(version, 4, 280) + rows + nativeGTIDHeader(version, 5, 566) + ddl + nativeGTIDHeader(version, 6, 730) + view + rotate,
			"many-events": previous + strings.Repeat(nativeGTIDHeader(version, 4, 280)+rows, 1000) + rotate,
		} {
			t.Run(version+"/"+name, func(t *testing.T) {
				old, err := frozenBinlogParser([]byte(text))
				if err != nil {
					t.Fatal(err)
				}
				got, err := parseBinlogStream(strings.NewReader(text), nil)
				if err != nil {
					t.Fatal(err)
				}
				if got.PreviousGTIDSet != recoverySID+":1-3,"+otherSID+":1-7" {
					t.Fatalf("lost previous UUIDs: %s", got.PreviousGTIDSet)
				}
				got.PreviousGTIDSet, old.PreviousGTIDSet = "", ""
				got.DecodePosition = 0
				if !reflect.DeepEqual(got, old) {
					t.Fatalf("transaction metadata differs:\nnew=%+v\nold=%+v", got, old)
				}
			})
		}
	}
}

func TestIncompleteNativeGTIDHeaderFails(t *testing.T) {
	text := "# original_commit_timestamp=1100000\n/*!80001 SET @@session.original_commit_timestamp=1100000*//*!*/;\n"
	if _, err := parseBinlogStream(strings.NewReader(text), nil); err == nil {
		t.Fatal("accepted partial GTID header")
	}
	for _, suffix := range []string{"# end_log_pos 200 GTID\tlast_committed=0\n", "# end_log_pos 200 Rotate to binlog.000002\n"} {
		if _, err := parseBinlogStream(strings.NewReader(text+suffix), nil); err == nil {
			t.Fatal("accepted missing GTID at event boundary")
		}
	}
}

func TestParsedPreviousGTIDsConstrainReplayCoverage(t *testing.T) {
	const otherSID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
	base := recoverySID + ":1-3," + otherSID + ":1-7"
	backup := myspec.BackupResult{GTIDSet: base, EndTime: time.UnixMicro(1000000),
		SourceUUID: recoverySID, BinlogFile: "binlog.000001", BinlogPosition: 4}
	makeLog := func(file, previous string, number int, stamp int64) archivedBinlog {
		t.Helper()
		text := "# Previous-GTIDs\n" + previous + "\n" +
			fmt.Sprintf("# original_commit_timestamp=%d\nSET @@SESSION.GTID_NEXT= '%s:%d'/*!*/;\n# end_log_pos 500 Xid = 1\nCOMMIT/*!*/;\n", stamp, recoverySID, number)
		meta, err := parseBinlogStream(strings.NewReader(text), nil)
		if err != nil {
			t.Fatal(err)
		}
		meta.ServerUUID, meta.File = recoverySID, file
		return archivedBinlog{key: file, meta: meta}
	}
	for _, tc := range []struct {
		name           string
		previous       string
		secondPrevious string
		includeFirst   bool
		wantLogs       int
		wantEnd        int64
	}{
		{"empty-previous-at-snapshot", "# [empty]", "", true, 2, 1200000},
		{"complete-multi-UUID", "# " + recoverySID + ":1-3,\n# " + otherSID + ":1-7", "", true, 2, 1200000},
		{"missing-first-file", "", "", false, 0, 1000000},
		{"empty-previous-after-snapshot", "", "# [empty]", false, 0, 1000000},
		{"missing-other-UUID-predecessor", "# " + recoverySID + ":1-3,\n# " + otherSID + ":1-7", "# " + recoverySID + ":1-4,\n# " + otherSID + ":1-8", true, 1, 1100000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			previous := tc.secondPrevious
			if previous == "" {
				previous = "# " + recoverySID + ":1-4,\n# " + otherSID + ":1-7"
			}
			logs := []archivedBinlog{makeLog("binlog.000002", previous, 5, 1200000)}
			if tc.includeFirst {
				logs = append([]archivedBinlog{makeLog("binlog.000001", tc.previous, 4, 1100000)}, logs...)
			}
			trusted, last, err := trustedReplayHistory(logs, backup)
			if err != nil || len(trusted) != tc.wantLogs || last != tc.wantEnd {
				t.Fatalf("trusted=%d last=%d error=%v", len(trusted), last, err)
			}
			coverage, err := replayCoverage(logs, backup)
			if err != nil || len(coverage.Windows) != 1 ||
				coverage.Windows[0].Start.UnixMicro() != 1000000 ||
				coverage.Windows[0].End.UnixMicro() != tc.wantEnd {
				t.Fatalf("coverage=%+v error=%v", coverage, err)
			}
		})
	}
}
