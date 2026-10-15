// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

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

func TestDeleteClusterRemovesOnlyExactIncarnationAndRetries(t *testing.T) {
	const directory = "production/karakoram/demo-incarnation/"
	objects := map[string]bool{
		directory + "snapshot/rs0/data": true, directory + "snapshot.pbm.json": true,
		directory + "pbmPitr/rs0/chunk": true, directory + ".pbm.init": true,
		"production/karakoram/demo-incarnation-other/snapshot/data": true,
		"production/karakoram/demo-old/snapshot/data":               true,
		"production/karakoram/other-incarnation/snapshot/data":      true,
	}
	denied := true
	var deleted []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256") {
			t.Error("unsigned S3 request")
		}
		switch r.Method {
		case http.MethodGet:
			if got := r.URL.Query().Get("prefix"); got != directory {
				t.Errorf("unexpected list prefix %q", got)
			}
			keys := []string{}
			marker := r.URL.Query().Get("continuation-token")
			for key := range objects {
				if strings.HasPrefix(key, directory) && key > marker {
					keys = append(keys, key)
				}
			}
			sort.Strings(keys)
			result := struct {
				XMLName   xml.Name `xml:"ListBucketResult"`
				Truncated bool     `xml:"IsTruncated"`
				Next      string   `xml:"NextContinuationToken,omitempty"`
				Contents  []struct {
					Key string `xml:"Key"`
				} `xml:"Contents"`
			}{}
			if len(keys) > 2 {
				result.Truncated = true
				keys = keys[:2]
				result.Next = keys[1]
			}
			for _, key := range keys {
				result.Contents = append(result.Contents, struct {
					Key string `xml:"Key"`
				}{key})
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(result)
		case http.MethodDelete:
			key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
			if denied && strings.Contains(key, "pbmPitr") {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte("<Error><Code>AccessDenied</Code><Message>denied</Message></Error>"))
				return
			}
			if !strings.HasPrefix(key, directory) {
				t.Errorf("deleted unrelated object %q", key)
			}
			deleted = append(deleted, key)
			delete(objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected S3 request %s", r.Method)
		}
	}))
	defer server.Close()
	s := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", Prefix: "production/", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", noRedirectTransport{transport}); err == nil {
		t.Fatal("object deletion failure was ignored")
	}
	denied = false
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", noRedirectTransport{transport}); err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 || len(deleted) != 4 {
		t.Fatal("unexpected remaining objects or deletion count", objects, deleted)
	}
	if err := deleteCluster(t.Context(), s, "demo", "incarnation", noRedirectTransport{transport}); err != nil {
		t.Fatal("empty namespace is not idempotent", err)
	}
}

func TestDeleteClusterRejectsObjectsOutsideNamespace(t *testing.T) {
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleted = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte("<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>karakoram/foreign/data</Key></Contents></ListBucketResult>"))
	}))
	defer server.Close()
	s := domain.BackupStorage{Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	defer transport.CloseIdleConnections()
	if err := deleteCluster(t.Context(), s, "demo", "uid", noRedirectTransport{transport}); err == nil || deleted {
		t.Fatal("foreign namespace accepted", err, deleted)
	}
}
