// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func validManifest() neo4jspec.Manifest {
	m := neo4jspec.Manifest{Format: neo4jspec.ManifestFormat, Edition: "community", Neo4jVersion: "2026.09.0"}
	for _, db := range neo4jspec.Databases {
		m.Databases = append(m.Databases, neo4jspec.DumpObject{Database: db, Key: "backup/op/" + db + ".dump", Size: 3, SHA256: strings.Repeat("0", 64)})
	}
	return m
}
func TestManifestValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*neo4jspec.Manifest)
	}{
		{"format", func(m *neo4jspec.Manifest) { m.Format = "unknown" }},
		{"missing", func(m *neo4jspec.Manifest) { m.Databases = m.Databases[:1] }},
		{"duplicate", func(m *neo4jspec.Manifest) { m.Databases[1] = m.Databases[0] }},
		{"newer", func(m *neo4jspec.Manifest) { m.Neo4jVersion = "2026.11.0" }},
		{"escape", func(m *neo4jspec.Manifest) { m.Databases[0].Key = "backup/op/../secret" }},
		{"prefix sibling", func(m *neo4jspec.Manifest) { m.Databases[0].Key = "backup/other/system.dump" }},
		{"hash", func(m *neo4jspec.Manifest) { m.Databases[0].SHA256 = "00" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := validManifest()
			tc.change(&m)
			if validateManifest(m, "backup/op", "2026.10.0") == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
	if err := validateManifest(validManifest(), "backup/op", "2026.10.0"); err != nil {
		t.Fatal(err)
	}
}
func TestDurableReceipt(t *testing.T) {
	root := t.TempDir()
	name, err := jobReceiptPath(root, "backup", "private receipt")
	if err != nil {
		t.Fatal(err)
	}
	q := neo4jspec.BackupRequest{Receipt: "private receipt", Target: neo4jspec.S3{SecretKey: "must-never-be-persisted"}}
	h, _ := requestHash(q)
	r := jobReceipt{RequestHash: h, JobResponse: neo4jspec.JobResponse{State: neo4jspec.JobRunning}}
	if err = durableJSON(name, &r); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(name)
	if bytes.Contains(b, []byte(q.Target.SecretKey)) {
		t.Fatal("secret persisted")
	}
	info, _ := os.Stat(name)
	if info.Mode().Perm() != 0600 {
		t.Fatal("receipt not owner-only")
	}
	loaded, err := readReceipt(name)
	if err != nil || loaded.RequestHash != h {
		t.Fatal("receipt replay failed", err)
	}
	q.Target.Bucket = "different"
	other, _ := requestHash(q)
	if other == loaded.RequestHash {
		t.Fatal("request mismatch not detectable")
	}
}
func TestVerifiedDumpReader(t *testing.T) {
	data := []byte("database dump")
	sum := sha256.Sum256(data)
	for _, mode := range []string{"valid", "sha", "short", "long"} {
		t.Run(mode, func(t *testing.T) {
			expected := neo4jspec.DumpObject{Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
			switch mode {
			case "sha":
				expected.SHA256 = strings.Repeat("0", 64)
			case "short":
				expected.Size++
			case "long":
				expected.Size--
			}
			aborted := false
			r := &verifiedDumpReader{source: bytes.NewReader(data), expected: expected, h: sha256.New(), abort: func() { aborted = true }}
			_, err := io.Copy(io.Discard, r)
			if mode == "valid" {
				if err != nil || !r.verified || aborted {
					t.Fatal("valid stream rejected")
				}
			} else if err == nil || !aborted || r.verified {
				t.Fatal("corrupt stream was not aborted")
			}
		})
	}
}
func TestSwapCrashRecovery(t *testing.T) {
	for _, phase := range []string{"staging", "moving-old-before", "moving-old-after", "installing-before", "installing-after", "installed", "committed"} {
		t.Run(phase, func(t *testing.T) {
			live := filepath.Join(t.TempDir(), "data")
			r := jobReceipt{Stage: live + ".restore-stage", Old: live + ".restore-old", Swap: strings.Split(phase, "-before")[0]}
			r.Swap = strings.TrimSuffix(r.Swap, "-after")
			write := func(dir, value string) {
				t.Helper()
				if err := os.MkdirAll(dir, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "identity"), []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch phase {
			case "staging", "moving-old-before":
				write(live, "old")
				write(r.Stage, "new")
			case "moving-old-after", "installing-before":
				write(r.Old, "old")
				write(r.Stage, "new")
			default:
				write(r.Old, "old")
				write(live, "new")
			}
			if err := recoverSwap(live, &r); err != nil {
				t.Fatal(err)
			}
			if err := recoverSwap(live, &r); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(filepath.Join(live, "identity"))
			if err != nil {
				t.Fatal(err)
			}
			want := "old"
			if phase == "committed" {
				want = "new"
			}
			if string(b) != want {
				t.Fatalf("got %s want %s", b, want)
			}
		})
	}
}
func TestStreamingMultipartS3(t *testing.T) {
	var uploaded []byte
	var manifest bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "POST" && r.URL.Query().Has("uploads"):
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<InitiateMultipartUploadResult><Bucket>backups</Bucket><Key>op/system.dump</Key><UploadId>test</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == "PUT" && r.URL.Query().Get("partNumber") != "":
			uploaded, _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", `"part"`)
		case r.Method == "POST" && r.URL.Query().Get("uploadId") != "":
			fmt.Fprint(w, `<CompleteMultipartUploadResult><Location>http://localhost/backups/op/system.dump</Location><Bucket>backups</Bucket><Key>op/system.dump</Key><ETag>"done"</ETag></CompleteMultipartUploadResult>`)
		case r.Method == "PUT" && strings.HasSuffix(r.URL.Path, "manifest.json"):
			manifest = true
			var m neo4jspec.Manifest
			if json.NewDecoder(r.Body).Decode(&m) != nil {
				t.Error("invalid manifest")
			}
			w.Header().Set("ETag", `"manifest"`)
		default:
			t.Errorf("unexpected S3 request %s %s", r.Method, r.URL)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	c, err := minio.New(strings.TrimPrefix(server.URL, "https://"), &minio.Options{Creds: credentials.NewStaticV4("test-access", "test-secret", ""), Region: "us-east-1", BucketLookup: minio.BucketLookupPath, Secure: true, Transport: server.Client().Transport})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("a real streamed dump")
	if _, err = c.PutObject(context.Background(), "backups", "op/system.dump", bytes.NewReader(payload), -1, minio.PutObjectOptions{PartSize: dumpPartSize, NumThreads: 1}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(uploaded, payload) {
		t.Fatalf("stream changed: %q", uploaded)
	}
	b, _ := json.Marshal(validManifest())
	if _, err = c.PutObject(context.Background(), "backups", "op/manifest.json", bytes.NewReader(b), int64(len(b)), minio.PutObjectOptions{DisableMultipart: true}); err != nil {
		t.Fatal(err)
	}
	if !manifest {
		t.Fatal("manifest absent")
	}
}

func TestInterruptedReceiptRecovery(t *testing.T) {
	name := filepath.Join(t.TempDir(), "receipt.json")
	r := &jobReceipt{RequestHash: "bound-request", JobResponse: neo4jspec.JobResponse{State: neo4jspec.JobRunning, Phase: neo4jspec.PhaseDumping}}
	if err := durableJSON(name, r); err != nil {
		t.Fatal(err)
	}
	if err := interruptReceipt(name, r, func() error { return errors.New("start failed") }); err == nil {
		t.Fatal("failed start was ignored")
	}
	stored, err := readReceipt(name)
	if err != nil || stored.State != neo4jspec.JobRunning {
		t.Fatalf("failed release changed receipt: %#v %v", stored, err)
	}
	released := false
	if err = interruptReceipt(name, r, func() error {
		before, err := readReceipt(name)
		if err != nil || before.State != neo4jspec.JobRunning {
			t.Fatal("failure published before release")
		}
		released = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stored, err = readReceipt(name)
	if err != nil || !released || stored.State != neo4jspec.JobFailed || stored.Message != "interrupted; retry with a new operation" || stored.Phase != "" {
		t.Fatalf("bad interrupted completion: %#v %v", stored, err)
	}
}

func TestReleaseFailureRemainsRecoveryPending(t *testing.T) {
	name := filepath.Join(t.TempDir(), "receipt.json")
	r := &jobReceipt{held: true, JobResponse: neo4jspec.JobResponse{State: neo4jspec.JobRunning}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	releaseErr := releaseJobHold(ctx, name, r, func(context.Context) error {
		pending, err := readReceipt(name)
		if err != nil || !pending.ReleasePending || pending.State != neo4jspec.JobRunning {
			t.Fatalf("release intent not durable before release: %#v %v", pending, err)
		}
		cancel()
		return errors.New("injected durable hold release failure")
	})
	if !errors.Is(releaseErr, context.Canceled) {
		t.Fatalf("release failure: %v", releaseErr)
	}
	if err := finishJobReceipt(name, r, releaseErr); err != nil {
		t.Fatal(err)
	}
	restarted, err := readReceipt(name)
	if err != nil || restarted.State != neo4jspec.JobRunning || !restarted.ReleasePending || restarted.Phase != neo4jspec.PhaseStarting {
		t.Fatalf("release failure became terminal: %#v %v", restarted, err)
	}
	if err = interruptReceipt(name, restarted, func() error { return errors.New("still cannot release") }); err == nil {
		t.Fatal("release failure ignored on restart")
	}
	stored, _ := readReceipt(name)
	if !stored.ReleasePending || stored.State != neo4jspec.JobRunning {
		t.Fatal("restart lost pending release")
	}
	if err = interruptReceipt(name, restarted, func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	stored, _ = readReceipt(name)
	if stored.ReleasePending || stored.State != neo4jspec.JobFailed {
		t.Fatalf("successful restart release not finalized: %#v", stored)
	}
}

func TestReleaseRetriesBeforePublishingTerminalOutcome(t *testing.T) {
	name := filepath.Join(t.TempDir(), "receipt.json")
	r := &jobReceipt{held: true, JobResponse: neo4jspec.JobResponse{State: neo4jspec.JobRunning}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	attempts := 0
	err := releaseJobHold(ctx, name, r, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("transient release failure")
		}
		return nil
	})
	if err != nil || attempts != 2 || r.held || r.ReleasePending {
		t.Fatalf("release retry failed: attempts=%d receipt=%#v err=%v", attempts, r, err)
	}
	if err = finishJobReceipt(name, r, errors.New("original dump failure")); err != nil {
		t.Fatal(err)
	}
	stored, err := readReceipt(name)
	if err != nil || stored.State != neo4jspec.JobFailed || stored.ReleasePending {
		t.Fatalf("original failure not finalized after release: %#v %v", stored, err)
	}
}

func TestTerminalPendingLatchRetriesPostRenameDirectorySync(t *testing.T) {
	name := filepath.Join(t.TempDir(), "receipts", "backup", "receipt.json")
	r := &jobReceipt{JobResponse: neo4jspec.JobResponse{State: neo4jspec.JobFailed, Message: "interrupted; retry with a new operation"}}
	m := &jobManager{pending: name}
	syncFailure := errors.New("injected post-rename directory sync failure")
	err := durableJSONWithSync(name, r, func(parent string) error {
		if parent != filepath.Dir(name) {
			t.Fatalf("unexpected sync directory %q", parent)
		}
		visible, readErr := readReceipt(name)
		if readErr != nil || visible.State != neo4jspec.JobFailed || visible.ReleasePending || visible.Swap != "" {
			t.Fatalf("failure not injected after terminal rename: %#v %v", visible, readErr)
		}
		return syncFailure
	})
	if !errors.Is(err, syncFailure) {
		t.Fatalf("post-rename failure: %v", err)
	}
	// The next pull observes the terminal JSON, but must not clear the latch
	// until its publication is durable. A further sync failure remains fenced.
	if err = m.clearTerminalPending(name, func(string) error { return syncFailure }); !errors.Is(err, syncFailure) || m.pending != name {
		t.Fatalf("failed durability retry released admission: pending=%q err=%v", m.pending, err)
	}
	if err = m.clearTerminalPending(name, syncDir); err != nil || m.pending != "" {
		t.Fatalf("durable terminal receipt left admission latched: pending=%q err=%v", m.pending, err)
	}
	// A terminal receipt belonging to another operation cannot clear a latch.
	m.pending = "another-operation"
	if err = m.clearTerminalPending(name, func(string) error { t.Fatal("unrelated receipt synced"); return nil }); err != nil || m.pending != "another-operation" {
		t.Fatalf("unrelated latch changed: pending=%q err=%v", m.pending, err)
	}
}
