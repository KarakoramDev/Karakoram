// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func streamTransaction() string {
	return "# original_commit_timestamp=1100000\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":4'/*!*/;\nBEGIN\n### INSERT INTO `test`.`rows`\n### SET\n###   @1='first line\nsecond line'\n# end_log_pos 486 CRC32 0 Xid = 10\nCOMMIT/*!*/;\n"
}

func TestStreamingBinlogMetadataEquivalence(t *testing.T) {
	// Expected metadata is frozen from the pre-streaming parser fixtures,
	// including its DDL and multiline row handling.
	for _, fixture := range []struct {
		name         string
		text         string
		transactions []binlogTransaction
		previous     string
	}{
		{"rows", "# Previous-GTIDs\n# " + recoverySID + ":1-3\n" + streamTransaction(), []binlogTransaction{{recoverySID + ":4", 1100000, 486}}, recoverySID + ":1-3"},
		{"ddl", "# original_commit_timestamp=1100000\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":1'/*!*/;\n# end_log_pos 300 Query\nCREATE DATABASE restored\n/*!*/;\n# end_log_pos 379 GTID\tlast_committed=1\n# original_commit_timestamp=1200000\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":2'/*!*/;\n# end_log_pos 500 Query\nCREATE TABLE restored.t(id int)\n/*!*/;\n# end_log_pos 544 Rotate to binlog.000002\n", []binlogTransaction{{recoverySID + ":1", 1100000, 300}, {recoverySID + ":2", 1200000, 500}}, ""},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			out, err := parseBinlogStream(strings.NewReader(fixture.text), nil)
			if err != nil || !reflect.DeepEqual(out.Transactions, fixture.transactions) || out.PreviousGTIDSet != fixture.previous {
				t.Fatalf("metadata=%+v error=%v", out, err)
			}
			if out.FirstCommitTimestamp != fixture.transactions[0].OriginalCommitTimestamp || out.LastCommitTimestamp != fixture.transactions[len(fixture.transactions)-1].OriginalCommitTimestamp {
				t.Fatalf("commit summary=%+v", out)
			}
		})
	}
}

// A lazy, fixed-storage source: the parser must not retain the repeated rows.
type repeatedBinlog struct {
	text      string
	remaining int
	offset    int
}

func (r *repeatedBinlog) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.text[r.offset:])
	r.offset += n
	if r.offset == len(r.text) {
		r.offset = 0
		r.remaining--
	}
	return n, nil
}

func TestStreamingBinlogDoesNotAccumulateRowsOrProbeRecords(t *testing.T) {
	const count = 100000
	text := strings.Replace(streamTransaction(), "### SET", "### SET\n### @2='"+strings.Repeat("x", 1024)+"'", 1)
	reader := &repeatedBinlog{text: text, remaining: count}
	seen := 0
	out, err := parseBinlogStream(reader, func(tx binlogTransaction) error {
		seen++
		if tx.EndPosition != 486 {
			return errors.New("wrong_event_boundary")
		}
		return nil
	})
	if err != nil || seen != count || len(out.Transactions) != 0 || out.GTIDSet != recoverySID+":4" {
		t.Fatalf("seen=%d retained=%d metadata=%+v error=%v", seen, len(out.Transactions), out, err)
	}
}

type failingBinlogReader struct{ err error }

func (r failingBinlogReader) Read([]byte) (int, error) { return 0, r.err }

func TestStreamingBinlogPropagatesFailures(t *testing.T) {
	failure := errors.New("source_read_failure")
	_, err := parseBinlogStream(io.MultiReader(strings.NewReader(streamTransaction()), failingBinlogReader{failure}), nil)
	if !errors.Is(err, failure) {
		t.Fatalf("lost read error: %v", err)
	}
	_, err = parseBinlogStream(strings.NewReader(streamTransaction()), func(binlogTransaction) error { return failure })
	if !errors.Is(err, failure) {
		t.Fatalf("lost record error: %v", err)
	}
	for _, text := range []string{
		"# original_commit_timestamp=1100000\nSET @@SESSION.GTID_NEXT= '" + recoverySID + ":4'/*!*/;\nBEGIN\n### incomplete row\n",
		strings.Repeat("x", (4<<20)+1),
		"# end_log_pos 184467440737095516160\n",
		"# original_commit_timestamp=9223372036854775808\n",
	} {
		if _, err := parseBinlogStream(strings.NewReader(text), nil); err == nil {
			t.Fatal("accepted incomplete or malformed decode")
		}
	}
}

func fakeMysqlbinlog(t *testing.T, script string) myspec.Spec {
	t.Helper()
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "bin"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "bin", "mysqlbinlog"), []byte("#!/bin/sh\n"+script), 0700); err != nil {
		t.Fatal(err)
	}
	return myspec.Spec{ServerDir: dir}
}

func TestDecodeBinlogExitAndBoundedDiagnostics(t *testing.T) {
	s := fakeMysqlbinlog(t, "printf '%s' "+shellQuote(streamTransaction())+"\nprintf 'decode failure' >&2\nexit 7\n")
	out, err := decodeBinlog(context.Background(), s, "ignored")
	if err == nil || !strings.Contains(err.Error(), "exit status 7") || !strings.Contains(err.Error(), "decode failure") || len(out.Transactions) != 0 {
		t.Fatalf("partial decode escaped: %+v %v", out, err)
	}
	var diagnostics boundedDiagnostics
	text := strings.Repeat("x", 1<<20)
	n, err := diagnostics.Write([]byte(text))
	if err != nil || n != len(text) || len(diagnostics.data) != 8192 {
		t.Fatal("unbounded diagnostics")
	}
}

func TestDecodeBinlogPassesResumeBoundary(t *testing.T) {
	s := fakeMysqlbinlog(t, "case \" $* \" in *' --start-position=486 '*) ;; *) exit 8;; esac\nprintf '%s' "+shellQuote(streamTransaction())+"\n")
	out, err := decodeBinlogFrom(context.Background(), s, "ignored", 486, nil)
	if err != nil || out.DecodePosition != 486 || len(out.Transactions) != 1 {
		t.Fatalf("%+v %v", out, err)
	}
}

func TestBinlogResumeIdentityAndRotation(t *testing.T) {
	name := filepath.Join(t.TempDir(), "binlog.000001")
	data := strings.Repeat("a", 512)
	if err := os.WriteFile(name, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	first, err := resumeBinlog(name, "source/binlog.000001", nil)
	if err != nil {
		t.Fatal(err)
	}
	first.Position = 400
	f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteString("tail"); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := resumeBinlog(name, first.Key, &first)
	if err != nil || next.Position != 400 {
		t.Fatalf("append lost resume: %+v %v", next, err)
	}
	for _, kind := range []string{"rotation", "shrink", "prefix", "inode"} {
		t.Run(kind, func(t *testing.T) {
			previous := first
			switch kind {
			case "rotation":
				previous.Key = "source/binlog.000000"
			case "shrink":
				previous.Position = 1000
			case "prefix":
				previous.Prefix = "different"
			case "inode":
				previous.Inode++
			}
			next, err := resumeBinlog(name, first.Key, &previous)
			if err != nil || next.Position != 4 {
				t.Fatalf("mismatch resumed: %+v %v", next, err)
			}
		})
	}
}

func TestArchiveIndexMigratesWithoutHistoricalReload(t *testing.T) {
	name := filepath.Join(t.TempDir(), "archive-index.json")
	old := archiveIndex{Files: map[string]binlogSidecar{}, Pending: map[string]bool{"source/binlog.000999": true}}
	old.Status.GTIDSet = recoverySID + ":1-200"
	for i := 1; i <= 200; i++ {
		key := fmt.Sprintf("source/binlog.%06d", i)
		old.Files[key] = binlogSidecar{File: key, Transactions: []binlogTransaction{{recoverySID + ":4", 1100000, 486}}}
	}
	if err := durableJSON(name, old); err != nil {
		t.Fatal(err)
	}
	idx, err := loadArchiveState(name)
	if err != nil || idx.Version != 1 || len(idx.Files) != 0 || idx.Status.GTIDSet != old.Status.GTIDSet || !idx.Pending["source/binlog.000999"] {
		t.Fatalf("migration=%+v %v", idx, err)
	}
	for key, want := range old.Files {
		b, err := os.ReadFile(archiveReceiptPath(name, key))
		if err != nil {
			t.Fatal(err)
		}
		var got binlogSidecar
		if err = json.Unmarshal(b, &got); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("history changed: %+v %v", got, err)
		}
		// Subsequent status/pass loads must not read historical receipts.
		if err = os.WriteFile(archiveReceiptPath(name, key), []byte("not JSON"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	idx.Active = &binlogResume{Key: "source/binlog.000999", Position: 400}
	if err = durableJSON(name, idx); err != nil {
		t.Fatal(err)
	}
	restarted, err := loadArchiveState(name)
	if err != nil || len(restarted.Files) != 0 || restarted.Active.Position != 400 {
		t.Fatalf("restart=%+v %v", restarted, err)
	}
	if ok, err := archiveRecorded(name, "source/binlog.000001"); err != nil || !ok {
		t.Fatalf("lost receipt: %v %v", ok, err)
	}
	if ok, err := archiveRecorded(name, "source/binlog.000999"); err != nil || ok {
		t.Fatalf("pending marked archived: %v %v", ok, err)
	}
}
