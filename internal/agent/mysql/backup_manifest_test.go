// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

const physicalManifestTestDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func manifestS3Fixture(t *testing.T, objects map[string][]byte) myspec.Repository {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && r.URL.Query().Get("list-type") == "2" {
			type listed struct {
				Key          string
				Size         int64
				ETag         string
				LastModified string
			}
			response := struct {
				XMLName     xml.Name `xml:"ListBucketResult"`
				Name        string
				Prefix      string
				IsTruncated bool
				Contents    []listed
			}{Name: "backups", Prefix: r.URL.Query().Get("prefix")}
			var keys []string
			for key := range objects {
				if strings.HasPrefix(key, response.Prefix) {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			for _, key := range keys {
				sum := md5.Sum(objects[key])
				response.Contents = append(response.Contents, listed{key, int64(len(objects[key])), fmt.Sprintf("%x", sum), time.Unix(1, 0).UTC().Format(time.RFC3339)})
			}
			w.Header().Set("Content-Type", "application/xml")
			if err := xml.NewEncoder(w).Encode(response); err != nil {
				t.Error(err)
			}
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/backups/")
		data, ok := objects[key]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>")
			return
		}
		sum := md5.Sum(data)
		w.Header().Set("ETag", fmt.Sprintf("\"%x\"", sum))
		w.Header().Set("Last-Modified", time.Unix(1, 0).UTC().Format(http.TimeFormat))
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		if r.Method == "HEAD" {
			return
		}
		if r.Method != "GET" {
			t.Errorf("unexpected object method %s", r.Method)
			w.WriteHeader(405)
			return
		}
		w.Write(data)
	}))
	t.Cleanup(server.Close)
	repo := testBackupRepository()
	repo.S3.Endpoint = server.URL
	return repo
}

func TestPhysicalObjectManifestRejectsRepositorySubstitution(t *testing.T) {
	for _, mutation := range []string{"unchanged", "replacement", "extra", "missing", "manifest"} {
		t.Run(mutation, func(t *testing.T) {
			prefix := "repo/mysql/cluster/base/full"
			payload := []byte("canonical physical XBStream chunk")
			sum := sha256.Sum256(payload)
			manifest := myspec.BackupObjectManifest{Version: 1, ObjectPrefix: prefix, Objects: []myspec.BackupObject{{Key: prefix + "/ibdata1.00000000000000000000", Size: int64(len(payload)), SHA256: hex.EncodeToString(sum[:])}}}
			raw, digest, err := manifest.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			objects := map[string][]byte{manifest.Objects[0].Key: payload, metadataObject(prefix, physicalManifestName): raw}
			switch mutation {
			case "replacement":
				objects[manifest.Objects[0].Key] = bytes.Repeat([]byte("x"), len(payload))
			case "extra":
				objects[prefix+"/stale.ibd.00000000000000000000"] = []byte("stale")
			case "missing":
				delete(objects, manifest.Objects[0].Key)
			case "manifest":
				objects[metadataObject(prefix, physicalManifestName)] = []byte(`{"version":1,"objectPrefix":"substituted"}`)
			}
			repo := manifestS3Fixture(t, objects)
			root := t.TempDir()
			bin := filepath.Join(root, "tools", "bin")
			if err := os.MkdirAll(bin, 0700); err != nil {
				t.Fatal(err)
			}
			// Test-only extractor records exactly the verified stream it receives.
			if err := os.WriteFile(filepath.Join(bin, "xbstream"), []byte("#!/bin/sh\ncat > \"$3/assembled\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			stage := filepath.Join(root, "stage")
			if err := privateDir(stage); err != nil {
				t.Fatal(err)
			}
			err = extractVerifiedBackup(t.Context(), myspec.Spec{XtraBackupDir: filepath.Dir(bin)}, repo, myspec.BackupManifestRef{ObjectPrefix: prefix, Digest: digest, Size: int64(len(raw))}, stage, nil)
			if mutation == "unchanged" {
				if err != nil {
					t.Fatal(err)
				}
				if got, err := os.ReadFile(filepath.Join(stage, "assembled")); err != nil || string(got) != string(payload) {
					t.Fatalf("verified bytes=%q err=%v", got, err)
				}
			} else {
				if err == nil {
					t.Fatal("unsealed physical object state accepted")
				}
				if _, err := os.Stat(filepath.Join(stage, "assembled")); !os.IsNotExist(err) {
					t.Fatal("unverified bytes reached extractor")
				}
			}
		})
	}
}

func TestPreparedPhysicalTreeAndPinnedPlanRejectMutation(t *testing.T) {
	for _, mutation := range []string{"bytes", "extra", "missing", "symlink"} {
		t.Run(mutation, func(t *testing.T) {
			stage := t.TempDir()
			root := filepath.Join(stage, "0")
			if err := privateDir(root); err != nil {
				t.Fatal(err)
			}
			name := filepath.Join(root, "ibdata1")
			if err := os.WriteFile(name, []byte("canonical prepared data"), 0600); err != nil {
				t.Fatal(err)
			}
			physical, err := preparedPhysicalFiles(root)
			if err != nil {
				t.Fatal(err)
			}
			receipt := restoreReceipt{Hash: physicalManifestTestDigest, SafePreflight: true, Replay: &restoreReplayPlan{Target: recoverySID + ":1", Physical: physical}}
			receipt.Status.PlanDigest, err = frozenRestorePlanDigest(receipt)
			if err != nil {
				t.Fatal(err)
			}
			request := myspec.RestoreRequest{PlanDigest: receipt.Status.PlanDigest}
			if err := verifyFrozenRestorePlan(receipt, stage, true); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "bytes":
				err = os.WriteFile(name, []byte("substituted prepared data"), 0600)
			case "extra":
				err = os.WriteFile(filepath.Join(root, "stale.ibd"), []byte("stale"), 0600)
			case "missing":
				err = os.Remove(name)
			case "symlink":
				if err = os.Remove(name); err == nil {
					err = os.Symlink("/etc/passwd", name)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyFrozenRestorePlan(receipt, stage, true); err == nil {
				t.Fatal("changed physical staging passed pre-STOP verification")
			}
			if mutation != "symlink" {
				receipt.Replay.Physical, err = preparedPhysicalFiles(root)
				if err != nil {
					t.Fatal(err)
				}
				receipt.Status.PlanDigest, err = frozenRestorePlanDigest(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if err := requireFrozenPlanDigest(request, receipt, true); err == nil {
					t.Fatal("recomputed local plan replaced the CP-pinned plan")
				}
			}
		})
	}
}
