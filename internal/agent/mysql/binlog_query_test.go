// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestBinlogQueryHeaderWhitespace(t *testing.T) {
	for _, separator := range []string{" ", "\t", " \t", "\t "} {
		t.Run(strings.ReplaceAll(separator, "\t", "TAB"), func(t *testing.T) {
			text := nativeGTIDHeader("80411", 1, 280) + "# end_log_pos 400" + separator + "Query\tthread_id=1\texec_time=0\terror_code=0\nCREATE DATABASE example\n/*!*/;\n" + nativeGTIDHeader("80411", 2, 480) + "# end_log_pos 600" + separator + "Query\tthread_id=1\texec_time=0\terror_code=0\nCREATE TABLE example.items (id int)\n/*!*/;\n"
			text = strings.ReplaceAll(text, " GTID\t", separator+"GTID"+separator)
			out, err := parseBinlogStream(strings.NewReader(text), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Transactions) != 2 || out.Transactions[0].EndPosition != 400 || out.Transactions[1].EndPosition != 600 {
				t.Fatalf("incorrect Query boundaries: %+v", out.Transactions)
			}
		})
	}
}

// Captured with the release-locked Percona Server/mysqlbinlog 8.4.11-11.
// The adjacent SQL files reproduce the capture on a fresh GTID-enabled server.
// Only the server UUID and generated authentication hash were sanitized; header
// bytes, commit timestamps, CRCs and native positions are otherwise unchanged.
func TestRealBinlogQueryFixtures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		first     int
		positions []uint64
		shapes    []string
	}{
		{"mysqlbinlog-queries", 1,
			[]uint64{352, 565, 796, 1006, 1421, 1659, 1900, 2110, 2437, 2707, 3151, 3333, 3810, 4006, 4202, 4418, 4608},
			[]string{"create-database", "alter-database", "create-table", "alter-table", "row-dml", "create-user", "grant", "alter-user", "set-password", "compressed-multi-statement", "xa-prepare", "xa-commit", "xa-prepare-rollback", "xa-rollback", "drop-user", "drop-table", "drop-database"}},
		{"mysqlbinlog-query-noops", 18,
			[]uint64{392, 591, 802, 1018, 1256, 1510, 1925, 2380, 2576, 2792, 3009, 3199, 3390},
			[]string{"create-database", "noop-create-database", "create-table", "noop-create-table", "create-user", "noop-create-user", "statement-multi-dml", "xa-one-phase", "drop-user", "drop-table", "noop-drop-table", "drop-database", "noop-drop-database"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + tc.name + ".txt")
			if err != nil {
				t.Fatal(err)
			}
			out, err := parseBinlogOutput(data)
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Transactions) != len(tc.positions) {
				t.Fatalf("got %d transactions, want %d", len(out.Transactions), len(tc.positions))
			}
			for i, position := range tc.positions {
				t.Run(tc.shapes[i], func(t *testing.T) {
					tx := out.Transactions[i]
					if tx.GTID != fmt.Sprintf("%s:%d", recoverySID, tc.first+i) || tx.EndPosition != position || tx.OriginalCommitTimestamp == 0 {
						t.Fatalf("incorrect native boundary: %+v; want position %d", tx, position)
					}
				})
			}
			var emitted []binlogTransaction
			streamed, err := parseBinlogStream(strings.NewReader(string(data)), func(tx binlogTransaction) error { emitted = append(emitted, tx); return nil })
			if err != nil || !reflect.DeepEqual(emitted, out.Transactions) || streamed.GTIDSet != out.GTIDSet {
				t.Fatalf("streaming mismatch: %v", err)
			}
		})
	}
}
