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
	mu            sync.Mutex
	objects       map[string]storedVersion
	deleted       []storedVersion
	denied        string
	foreign       string
	pageSize      int
	batches       int
	blockNextPage bool
}

func versionKey(v storedVersion) string { return v.Key + "\x00" + v.VersionID }
func newVersionedS3(objects ...storedVersion) *versionedS3 {
	f := &versionedS3{objects: map[string]storedVersion{}, pageSize: 2}
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
		if !r.URL.Query().Has("versions") {
			http.Error(w, "version listing required", http.StatusBadRequest)
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
		if len(keys) > f.pageSize {
			result.Truncated = true
			keys = keys[:f.pageSize]
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
				result.Errors = append(result.Errors, struct {
					Key       string
					VersionID string `xml:"VersionId"`
					Code      string
					Message   string
				}{v.Key, v.VersionID, "AccessDenied", "denied"})
				continue
			}
			f.deleted = append(f.deleted, v)
			delete(f.objects, versionKey(v))
			result.Deleted = append(result.Deleted, v)
		}
		_ = xml.NewEncoder(w).Encode(result)
	default:
		http.Error(w, "unsupported operation", http.StatusMethodNotAllowed)
	}
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
