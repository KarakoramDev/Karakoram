// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func TestSQLiteFullPreservesAcceptedIntentAndRecovers(t *testing.T) {
	st, _ := testStore(t)
	if err := st.CreateCluster(t.Context(), &domain.Cluster{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	accepted := &domain.Operation{ID: "accepted", Cluster: "demo", Kind: domain.OperationRestart}
	if err := st.EnqueueOperation(t.Context(), accepted); err != nil {
		t.Fatal(err)
	}
	var pages int
	if err := st.writer.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err := st.writer.Exec("PRAGMA max_page_count=" + fmt.Sprint(pages)); err != nil {
		t.Fatal(err)
	}
	blocked := &domain.Operation{ID: "blocked", Cluster: "demo", Kind: domain.OperationRestart, Payload: json.RawMessage(`{"padding":"` + strings.Repeat("x", 1<<20) + `"}`)}
	err := st.EnqueueOperation(t.Context(), blocked)
	var sqliteErr interface{ Code() int }
	if err == nil || !errors.As(err, &sqliteErr) || sqliteErr.Code() != 13 {
		t.Fatalf("expected actual SQLite full failure, got %v", err)
	}
	if _, err := st.GetOperation(t.Context(), "blocked"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unaccepted operation leaked: %v", err)
	}
	got, err := st.GetOperation(t.Context(), "accepted")
	if err != nil || got.Phase != domain.PhasePending {
		t.Fatalf("accepted intent lost: %v", err)
	}
	if _, err := st.writer.Exec("PRAGMA max_page_count=1073741823"); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueOperation(t.Context(), blocked); err != nil {
		t.Fatal("storage recovery required process restart", err)
	}
	ops, err := st.ListOperations(t.Context(), domain.ClusterRef{Name: "demo"})
	if err != nil || len(ops) != 2 {
		t.Fatalf("unexpected durable intents: %d %v", len(ops), err)
	}
}

func TestSQLiteReadOnlyFilePreservesStateAndRecovers(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses read-only file permissions; exercise readonly mount in a disposable guest")
	}
	st, path := testStore(t)
	if err := st.CreateCluster(t.Context(), &domain.Cluster{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueOperation(t.Context(), &domain.Operation{ID: "accepted", Cluster: "demo", Kind: domain.OperationRestart}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0600)
	ro, openErr := Open(path, st.sealer)
	if openErr == nil {
		ro.Close()
		t.Error("startup accepted SQLite's permanent read-only fallback instead of reopening after storage recovery")
	} else if !strings.Contains(openErr.Error(), "preserve") {
		t.Fatalf("nonactionable startup error: %v", openErr)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(path, st.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if _, err := recovered.GetOperation(t.Context(), "accepted"); err != nil {
		t.Fatal("accepted intent lost", err)
	}
	if _, err := recovered.GetOperation(t.Context(), "blocked"); !errors.Is(err, ErrNotFound) {
		t.Fatal("failed mutation persisted", err)
	}
	if err := recovered.EnqueueOperation(t.Context(), &domain.Operation{ID: "blocked", Cluster: "demo", Kind: domain.OperationRestart}); err != nil {
		t.Fatal("write access recovery failed", err)
	}
}

func TestSQLiteWriterLockRecoversWithoutLosingIntent(t *testing.T) {
	st, path := testStore(t)
	if err := st.CreateCluster(t.Context(), &domain.Cluster{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.writer.Exec("PRAGMA busy_timeout=20"); err != nil {
		t.Fatal(err)
	}
	other, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("UPDATE clusters SET updated=updated WHERE name='demo'"); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "retry", Cluster: "demo", Kind: domain.OperationRestart}
	if err := st.EnqueueOperation(t.Context(), op); err == nil {
		t.Fatal("writer lock did not reject mutation")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal("writer lock recovery failed", err)
	}
	ops, err := st.ListOperations(t.Context(), domain.ClusterRef{Name: "demo"})
	if err != nil || len(ops) != 1 {
		t.Fatalf("operation duplicated or lost: %d %v", len(ops), err)
	}
}

func TestSQLiteCorruptionRefusesWithoutReplacingState(t *testing.T) {
	st, path := testStore(t)
	st.Close()
	damaged := []byte("not a SQLite database")
	if err := os.WriteFile(path, damaged, 0600); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path, st.sealer); err == nil {
		opened.Close()
		t.Fatal("corrupt database was accepted")
	} else if !strings.Contains(err.Error(), "restore verified control-plane state") {
		t.Fatalf("missing corruption recovery prerequisite: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != string(damaged) {
		t.Fatal("authoritative damaged state was replaced", err)
	}
}

func TestSQLiteEmptyExistingFileRefusesStateReset(t *testing.T) {
	st, path := testStore(t)
	st.Close()
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if opened, err := Open(path, st.sealer); err == nil {
		opened.Close()
		t.Fatal("empty existing database was silently reinitialized")
	} else if !strings.Contains(err.Error(), "restore verified control-plane state") {
		t.Fatalf("missing empty-database recovery prerequisite: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 0 {
		t.Fatal("empty authoritative file was rewritten", err)
	}
}

func TestSQLiteInterruptedFirstInitializationRecovers(t *testing.T) {
	for _, contents := range []string{"", "interrupted SQLite header"} {
		t.Run(fmt.Sprintf("stage-bytes-%d", len(contents)), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "state.db")
			// Reproduce a crash after the first-install stage exists but before
			// schema initialization or atomic publication completed.
			stage, err := os.CreateTemp(dir, ".state.db.init-*")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = stage.WriteString(contents); err != nil {
				t.Fatal(err)
			}
			if err = stage.Close(); err != nil {
				t.Fatal(err)
			}
			key, err := secrets.New(strings.Repeat("m", 32))
			if err != nil {
				t.Fatal(err)
			}
			st, err := Open(path, key)
			if err != nil {
				t.Fatal("interrupted first installation wedged retry", err)
			}
			defer st.Close()
			c := &domain.Cluster{Name: "accepted"}
			if err = st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			// A losing initializer must open, never replace, the published winner.
			other, err := initializeStore(path, key)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			if _, err = other.GetCluster(t.Context(), c.Ref()); err != nil {
				t.Fatal("publication overwrote accepted state", err)
			}
		})
	}
}

func TestSQLiteFirstInitializationWriteFailureRecovers(t *testing.T) {
	const childPath = "KK_TEST_FIRST_INIT_WRITE_FAILURE"
	path := os.Getenv(childPath)
	if path == "" {
		path = filepath.Join(t.TempDir(), "state.db")
		child := exec.Command(os.Args[0], "-test.run=^TestSQLiteFirstInitializationWriteFailureRecovers$")
		child.Env = append(os.Environ(), childPath+"="+path)
		if output, err := child.CombinedOutput(); err != nil {
			t.Fatalf("isolated first-install storage fault: %v\n%s", err, output)
		}
		return
	}
	key, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	var original syscall.Rlimit
	if err = syscall.Getrlimit(syscall.RLIMIT_FSIZE, &original); err != nil {
		t.Fatal(err)
	}
	limited := original
	limited.Cur = 0
	signal.Ignore(syscall.SIGXFSZ)
	if err = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited); err != nil {
		t.Fatal(err)
	}
	// Creating an empty staging file succeeds, but SQLite's first write is
	// refused by the kernel. Isolate this process-wide fault from other tests.
	failed, openErr := Open(path, key)
	restoreErr := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &original)
	if failed != nil {
		_ = failed.Close()
	}
	if restoreErr != nil {
		t.Fatal(restoreErr)
	}
	if openErr == nil {
		t.Fatal("kernel write refusal did not fail initialization")
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("failed initialization published an authoritative path", err)
	}
	recovered, err := Open(path, key)
	if err != nil {
		t.Fatal("storage repair could not retry first initialization", err)
	}
	defer recovered.Close()
	if err = recovered.CreateCluster(t.Context(), &domain.Cluster{Name: "recovered"}); err != nil {
		t.Fatal(err)
	}
}
