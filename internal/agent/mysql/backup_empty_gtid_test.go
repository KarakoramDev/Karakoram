// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupMetadataOptionalGTID(t *testing.T) {
	const gtid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1-10"
	for _, test := range []struct {
		name, binlog, gtid string
		invalid            bool
	}{
		// XtraBackup 8.0/8.4 write filename<TAB>position<LF> when
		// gtid_executed is empty, and append <TAB>GTIDs otherwise.
		{"native_empty", "binlog.000003\t198\n", "", false},
		{"empty_column", "binlog.000003\t198\t\n", "", false},
		{"native_gtids", "binlog.000003\t198\t" + gtid + "\n", gtid, false},
		{"missing_coordinates", "\n", "", true},
		{"missing_position", "binlog.000003\t\n", "", true},
		{"invalid_position", "binlog.000003\tinvalid\n", "", true},
		{"zero_position", "binlog.000003\t0\n", "", true},
		{"short_position", "binlog.000003\t3\n", "", true},
		{"invalid_filename", "not-a-binlog\t198\n", "", true},
		{"invalid_gtid", "binlog.000003\t198\tinvalid\n", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			binlogPosition := "filename 'binlog.000003', position '198'"
			if test.gtid != "" {
				binlogPosition += ", GTID of the last change '" + test.gtid + "'"
			}
			for name, text := range map[string]string{
				"xtrabackup_checkpoints": "backup_type = full-backuped\nfrom_lsn = 0\nto_lsn = 99\n",
				"xtrabackup_binlog_info": test.binlog,
				"xtrabackup_info":        "tool_version = 8.4.0-7\nserver_version = 8.4.11-11\nend_time = 2026-10-21 09:07:58\nbinlog_pos = " + binlogPosition + "\n",
			} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			got, err := ParseBackupMetadata(dir)
			if test.invalid {
				if err == nil {
					t.Fatal("accepted invalid metadata")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.BinlogFile != "binlog.000003" || got.BinlogPosition != 198 || got.GTIDSet != test.gtid {
				t.Fatalf("unexpected snapshot coordinates: %+v", got)
			}
		})
	}
}
