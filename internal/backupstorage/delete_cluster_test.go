// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestClusterDirectory(t *testing.T) {
	s := domain.BackupStorage{Prefix: "production/"}
	directory, err := ClusterDirectory(s, "demo", "incarnation")
	if err != nil || directory != "production/karakoram/demo-incarnation/" {
		t.Fatal(directory, err)
	}
	for _, test := range []struct{ prefix, cluster, uid string }{
		{"", "", "uid"}, {"", "demo", ""}, {"", "../demo", "uid"}, {"", "demo", "../uid"},
		{"", "demo", ".."}, {"", "demo", "u\\id"}, {"/production", "demo", "uid"}, {"production//nested", "demo", "uid"}, {"production/../other", "demo", "uid"},
	} {
		if _, err := ClusterDirectory(domain.BackupStorage{Prefix: test.prefix}, test.cluster, test.uid); err == nil {
			t.Fatal("unsafe namespace accepted", test)
		}
	}
}

type storedVersion struct {
	Key       string
	VersionID string `xml:"VersionId,omitempty"`
	Marker    bool   `xml:"-"`
}

type versionedS3 struct {
	mu                sync.Mutex
	objects           map[string]storedVersion
	deleted           []storedVersion
	denied            string
	foreign           string
	pageSize          int
	batches           int
	blockNextPage     bool
	versionCode       string
	versionStatus     int
	versionLists      int
	plainLists        int
	deleteCode        string
	hidden            map[string]bool
	confirmationModes []bool
}

func versionKey(v storedVersion) string { return v.Key + "\x00" + v.VersionID }
func newVersionedS3(objects ...storedVersion) *versionedS3 {
	f := &versionedS3{objects: map[string]storedVersion{}, hidden: map[string]bool{}, pageSize: 2}
	for _, v := range objects {
		f.objects[versionKey(v)] = v
	}
	return f
}
func (f *versionedS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/xml")
	switch r.Method {
	case http.MethodGet:
		if r.URL.Query().Get("max-keys") == "1" {
			f.confirmationModes = append(f.confirmationModes, r.URL.Query().Has("versions"))
		}
		if !r.URL.Query().Has("versions") {
			f.plainLists++
			f.servePlainList(w, r)
			return
		}
		f.versionLists++
		if f.versionCode != "" {
			status := f.versionStatus
			if status == 0 {
				status = http.StatusForbidden
			}
			w.WriteHeader(status)
			_ = xml.NewEncoder(w).Encode(struct {
				XMLName xml.Name `xml:"Error"`
				Code    string
				Message string
			}{Code: f.versionCode, Message: "version listing is unavailable"})
			return
		}
		marker := r.URL.Query().Get("key-marker") + "\x00" + r.URL.Query().Get("version-id-marker")
		if f.blockNextPage && r.URL.Query().Get("key-marker") != "" {
			f.mu.Unlock()
			<-r.Context().Done()
			f.mu.Lock()
			return
		}
		prefix := r.URL.Query().Get("prefix")
		keys := []string{}
		for key, v := range f.objects {
			if strings.HasPrefix(v.Key, prefix) && key > marker {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		result := struct {
			XMLName     xml.Name        `xml:"ListVersionsResult"`
			Truncated   bool            `xml:"IsTruncated"`
			NextKey     string          `xml:"NextKeyMarker,omitempty"`
			NextVersion string          `xml:"NextVersionIdMarker,omitempty"`
			Versions    []storedVersion `xml:"Version"`
			Markers     []storedVersion `xml:"DeleteMarker"`
		}{}
		pageSize := f.pageSize
		if max, _ := strconv.Atoi(r.URL.Query().Get("max-keys")); max > 0 && max < pageSize {
			pageSize = max
		}
		if len(keys) > pageSize {
			result.Truncated = true
			keys = keys[:pageSize]
			last := f.objects[keys[len(keys)-1]]
			result.NextKey = last.Key
			result.NextVersion = last.VersionID
		}
		for _, key := range keys {
			v := f.objects[key]
			if v.Marker {
				result.Markers = append(result.Markers, v)
			} else {
				result.Versions = append(result.Versions, v)
			}
		}
		if f.foreign != "" {
			result.Versions = append(result.Versions, storedVersion{Key: f.foreign})
		}
		_ = xml.NewEncoder(w).Encode(result)
	case http.MethodPost:
		if !r.URL.Query().Has("delete") {
			http.Error(w, "multi-delete required", http.StatusBadRequest)
			return
		}
		var request struct {
			Objects []storedVersion `xml:"Object"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.batches++
		result := struct {
			XMLName xml.Name        `xml:"DeleteResult"`
			Deleted []storedVersion `xml:"Deleted"`
			Errors  []struct {
				Key       string
				VersionID string `xml:"VersionId"`
				Code      string
				Message   string
			} `xml:"Error"`
		}{}
		for _, v := range request.Objects {
			if f.denied != "" && strings.Contains(v.Key, f.denied) {
				code := f.deleteCode
				if code == "" {
					code = "AccessDenied"
				}
				result.Errors = append(result.Errors, struct {
					Key       string
					VersionID string `xml:"VersionId"`
					Code      string
					Message   string
				}{v.Key, v.VersionID, code, "denied"})
				continue
			}
			f.deleted = append(f.deleted, v)
			if v.VersionID == "" {
				// A key-only delete on a versioned bucket hides the current
				// object but leaves its historical versions in storage.
				f.hidden[v.Key] = true
			}
			delete(f.objects, versionKey(v))
			result.Deleted = append(result.Deleted, v)
		}
		_ = xml.NewEncoder(w).Encode(result)
	default:
		http.Error(w, "unsupported operation", http.StatusMethodNotAllowed)
	}
}

// servePlainList runs with the fake's mutex held and exposes current keys only.
func (f *versionedS3) servePlainList(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("continuation-token")
	latest := map[string]storedVersion{}
	for _, v := range f.objects {
		if old, ok := latest[v.Key]; !ok || v.VersionID > old.VersionID {
			latest[v.Key] = v
		}
	}
	keys := []string{}
	for key, v := range latest {
		if strings.HasPrefix(key, prefix) && key > marker && !f.hidden[key] && !v.Marker {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	result := struct {
		XMLName   xml.Name               `xml:"ListBucketResult"`
		Truncated bool                   `xml:"IsTruncated"`
		Next      string                 `xml:"NextContinuationToken,omitempty"`
		Contents  []struct{ Key string } `xml:"Contents"`
	}{}
	pageSize := f.pageSize
	if max, _ := strconv.Atoi(r.URL.Query().Get("max-keys")); max > 0 && max < pageSize {
		pageSize = max
	}
	if len(keys) > pageSize {
		result.Truncated = true
		keys = keys[:pageSize]
		result.Next = keys[len(keys)-1]
	}
	for _, key := range keys {
		result.Contents = append(result.Contents, struct{ Key string }{key})
	}
	_ = xml.NewEncoder(w).Encode(result)
}

func storageFixture(t *testing.T, f *versionedS3) (domain.BackupStorage, http.RoundTripper) {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	t.Cleanup(transport.CloseIdleConnections)
	return domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", Prefix: "production/", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}, noRedirectTransport{transport}
}

func TestDeleteClusterRemovesOnlyExactIncarnationAndRetries(t *testing.T) {
	const directory = "production/karakoram/demo-incarnation/"
	f := newVersionedS3(
		storedVersion{Key: directory + "snapshot/rs0/data", VersionID: "v1"}, storedVersion{Key: directory + "snapshot/rs0/data", VersionID: "v2"},
		storedVersion{Key: directory + "snapshot/rs0/data", VersionID: "v3", Marker: true}, storedVersion{Key: directory + "snapshot.pbm.json", VersionID: "meta"},
		storedVersion{Key: directory + "pbmPitr/rs0/chunk"}, storedVersion{Key: directory + ".pbm.init"},
		storedVersion{Key: "production/karakoram/demo-incarnation-other/snapshot/data"}, storedVersion{Key: "production/karakoram/demo-old/snapshot/data"}, storedVersion{Key: "production/karakoram/other-incarnation/snapshot/data"},
	)
	f.denied = "pbmPitr"
	s, transport := storageFixture(t, f)
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err == nil {
		t.Fatal("object deletion failure was ignored")
	}
	f.mu.Lock()
	f.denied = ""
	f.mu.Unlock()
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	if len(f.objects) != 3 || len(f.deleted) != 6 || f.batches != 2 {
		t.Error("unexpected remaining objects, versions or batches", f.objects, f.deleted, f.batches)
	}
	f.mu.Unlock()
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err != nil {
		t.Fatal("empty namespace is not idempotent", err)
	}
}

func TestDeleteClusterRejectsObjectsOutsideNamespace(t *testing.T) {
	f := newVersionedS3()
	f.foreign = "karakoram/foreign/data"
	s, transport := storageFixture(t, f)
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err == nil {
		t.Fatal("foreign namespace accepted")
	}
	if len(f.deleted) != 0 {
		t.Fatal("foreign object deleted", f.deleted)
	}
}

func TestDeleteClusterBudgetAfterProgressRequeues(t *testing.T) {
	f := newVersionedS3()
	f.pageSize = 1000
	f.blockNextPage = true
	for i := range 1001 {
		v := storedVersion{Key: fmt.Sprintf("production/karakoram/demo-incarnation/data/%04d", i), VersionID: "version"}
		f.objects[versionKey(v)] = v
	}
	s, transport := storageFixture(t, f)
	err := deleteClusterPass(t.Context(), s, "demo", "incarnation", transport, time.Second)
	if !errors.Is(err, ErrDeletionInProgress) {
		t.Fatal("productive bounded pass must requeue normally", err)
	}
	f.mu.Lock()
	if len(f.deleted) != 1000 {
		t.Error("progress was not confirmed by batch results", len(f.deleted))
	}
	f.blockNextPage = false
	f.mu.Unlock()
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err != nil {
		t.Fatal(err)
	}
	if len(f.objects) != 0 {
		t.Fatal("retry left object versions", f.objects)
	}
}

func TestDeleteSnapshotRemovesAllVersionsAndMetadataLast(t *testing.T) {
	const base = "production/karakoram/demo-old/snapshot"
	f := newVersionedS3(
		storedVersion{Key: base + "/data", VersionID: "v1"}, storedVersion{Key: base + "/data", VersionID: "v2"}, storedVersion{Key: base + "/data", VersionID: "marker", Marker: true},
		storedVersion{Key: base + ".pbm.json", VersionID: "m1"}, storedVersion{Key: base + ".pbm.json", VersionID: "m2"}, storedVersion{Key: base + ".pbm.json", VersionID: "marker", Marker: true},
		storedVersion{Key: base + ".pbm.json.other", VersionID: "keep"}, storedVersion{Key: base + "-other/data", VersionID: "keep"}, storedVersion{Key: "production/karakoram/demo-old/pbmPitr/chunk", VersionID: "keep"},
	)
	s, transport := storageFixture(t, f)
	b := domain.Backup{Storage: s.Name, PBMPrefix: "production/karakoram/demo-old", PBMName: "snapshot"}
	f.denied = "/data"
	if err := deleteSnapshot(context.Background(), s, b, transport); err == nil {
		t.Fatal("snapshot deletion failure ignored")
	}
	for _, v := range f.deleted {
		if strings.HasSuffix(v.Key, ".pbm.json") {
			t.Fatal("metadata removed before data", f.deleted)
		}
	}
	f.mu.Lock()
	f.denied = ""
	f.mu.Unlock()
	if err := deleteSnapshot(t.Context(), s, b, transport); err != nil {
		t.Fatal(err)
	}
	if len(f.objects) != 3 || len(f.deleted) != 6 {
		t.Fatal("snapshot versions or siblings mishandled", f.objects, f.deleted)
	}
	for _, v := range f.deleted[:3] {
		if v.Key != base+"/data" {
			t.Fatal("metadata deleted before data versions", f.deleted)
		}
	}
	if err := deleteSnapshot(t.Context(), s, b, transport); err != nil {
		t.Fatal("missing snapshot must succeed", err)
	}
}

func TestClusterDeletionFallsBackWhenVersionListingUnavailable(t *testing.T) {
	for _, test := range []struct {
		code   string
		status int
	}{
		{"AccessDenied", http.StatusForbidden},
		{"NotImplemented", http.StatusNotImplemented},
		{"MethodNotAllowed", http.StatusMethodNotAllowed},
		{"InvalidRequest", http.StatusBadRequest},
		{"UnsupportedOperation", http.StatusBadRequest},
	} {
		t.Run(test.code, func(t *testing.T) {
			const key = "production/karakoram/demo-incarnation/snapshot/data"
			f := newVersionedS3(storedVersion{Key: key, VersionID: "old"}, storedVersion{Key: key, VersionID: "latest"}, storedVersion{Key: "production/karakoram/other-incarnation/data", VersionID: "keep"})
			f.versionCode, f.versionStatus = test.code, test.status
			s, transport := storageFixture(t, f)
			if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err != nil {
				t.Fatal("ordinary list/delete credentials could not delete current objects", err)
			}
			f.mu.Lock()
			if len(f.objects) != 3 || !f.hidden[key] || len(f.deleted) != 1 || f.deleted[0].VersionID != "" {
				t.Error("fallback must delete only current keys, preserving historical versions", f.objects, f.deleted)
			}
			if f.versionLists != 1 || f.plainLists < 2 || len(f.confirmationModes) != 1 || f.confirmationModes[0] {
				t.Error("fallback did not confirm absence using plain listing", f.versionLists, f.plainLists, f.confirmationModes)
			}
			// Once version-aware access is available, a later cleanup can
			// remove the historical versions left by key-only deletion.
			f.versionCode = ""
			f.mu.Unlock()
			if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.objects) != 1 {
				t.Fatal("version-aware cleanup left historical versions", f.objects)
			}
		})
	}
}

func TestSnapshotDeletionFallsBackToOrdinaryPermissions(t *testing.T) {
	const base = "production/karakoram/demo-old/snapshot"
	f := newVersionedS3(storedVersion{Key: base + "/data"}, storedVersion{Key: base + ".pbm.json"}, storedVersion{Key: base + ".pbm.json.other"})
	f.versionCode = "AccessDenied"
	s, transport := storageFixture(t, f)
	b := domain.Backup{Storage: s.Name, PBMPrefix: "production/karakoram/demo-old", PBMName: "snapshot"}
	if err := deleteSnapshot(t.Context(), s, b, transport); err != nil {
		t.Fatal("retained snapshot required version permissions", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.objects) != 1 || len(f.deleted) != 2 {
		t.Fatal("fallback removed sibling metadata or retained snapshot objects", f.objects, f.deleted)
	}
	if len(f.confirmationModes) != 2 || f.confirmationModes[0] || f.confirmationModes[1] {
		t.Fatal("snapshot fallback confirmed with the wrong listing mode", f.confirmationModes)
	}
}

func TestDeletionConfirmsAbsenceAfterSDKSuppressesObjectErrors(t *testing.T) {
	for _, code := range []string{"InvalidArgument", "NoSuchVersion"} {
		for _, versions := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/versions=%v", code, versions), func(t *testing.T) {
				const key = "production/karakoram/demo-incarnation/data"
				v := storedVersion{Key: key}
				if versions {
					v.VersionID = "version"
				}
				f := newVersionedS3(v)
				f.denied, f.deleteCode = "/data", code
				if !versions {
					f.versionCode = "AccessDenied"
				}
				s, transport := storageFixture(t, f)
				if err := deleteCluster(t.Context(), s, "demo", "incarnation", transport); err == nil {
					t.Fatal("suppressed object error incorrectly completed cleanup")
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				if len(f.objects) != 1 || len(f.deleted) != 0 || len(f.confirmationModes) != 1 || f.confirmationModes[0] != versions {
					t.Fatal("cleanup did not confirm the still-present selected object", f.objects, f.deleted, f.confirmationModes)
				}
			})
		}
	}
}
