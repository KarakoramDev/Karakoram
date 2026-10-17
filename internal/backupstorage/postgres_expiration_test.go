// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package backupstorage

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// postgresS3 adds metadata reads/writes to the versioned deletion fixture.
type postgresS3 struct {
	*versionedS3
	data    map[string][]byte
	failPut string
}

func (f *postgresS3) put(key string, data []byte) {
	f.data[key] = data
	v := storedVersion{Key: key}
	f.objects[versionKey(v)] = v
	delete(f.hidden, key)
}

func (f *postgresS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
	if key != "" && !r.URL.Query().Has("prefix") && (r.Method == http.MethodGet || r.Method == http.MethodPut) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Method == http.MethodPut {
			if key == f.failPut {
				f.failPut = ""
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(http.StatusForbidden)
				_ = xml.NewEncoder(w).Encode(struct {
					XMLName xml.Name `xml:"Error"`
					Code    string
				}{Code: "AccessDenied"})
				return
			}
			data, err := readProbeBody(r)
			if err != nil {
				http.Error(w, "cannot read object", http.StatusBadRequest)
				return
			}
			f.put(key, data)
			w.Header().Set("ETag", `"test-etag"`)
			return
		}
		data, exists := f.data[key]
		_, present := f.objects[versionKey(storedVersion{Key: key})]
		if !exists || !present {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_ = xml.NewEncoder(w).Encode(struct {
				XMLName xml.Name `xml:"Error"`
				Code    string
			}{Code: "NoSuchKey"})
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", `"test-etag"`)
		w.Header().Set("Last-Modified", "Sat, 17 Oct 2026 00:00:00 GMT")
		_, _ = w.Write(data)
		return
	}
	f.versionedS3.ServeHTTP(w, r)
}

func TestPostgresExpirationRebasesInterruptedPlan(t *testing.T) {
	for _, interruption := range []string{"before-inventory-publish", "during-object-deletion"} {
		t.Run(interruption, func(t *testing.T) {
			const directory = "archive/postgres/prod/orders-incarnation/"
			const a = "20261017-021859F"
			const b = "20261018-021859F"
			const c = "20261019-021859F"
			entries, err := parsePostgresInfo(postgresInfoFixture())
			if err != nil {
				t.Fatal(err)
			}
			// Keep a third independent full so B's deletion leaves inventory.
			entries = append(entries[:7], append([]infoEntry{{"backup:current", c, `{"backup-archive-start":"000000030000000000000030","db-id":1}`}}, entries[7:]...)...)
			f := &postgresS3{versionedS3: newVersionedS3(), data: map[string][]byte{}}
			for _, file := range []string{"backup.info", "backup.info.copy"} {
				f.put(directory+"backup/kk/"+file, renderPostgresInfo(entries))
			}
			for _, entry := range entries {
				if entry.section == "backup:current" {
					f.put(directory+"backup/kk/"+entry.key+"/backup.manifest", []byte("manifest"))
				}
			}
			server := httptest.NewServer(f)
			t.Cleanup(server.Close)
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.Proxy = nil
			t.Cleanup(transport.CloseIdleConnections)
			storage := domain.BackupStorage{Name: "archive", Namespace: "prod", Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", Prefix: "new-prefix", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			backup := domain.Backup{Namespace: "prod", Cluster: "orders", Storage: "archive", Postgres: &domain.PostgresBackup{Label: a, Repository: strings.TrimSuffix(directory, "/")}}
			if interruption == "before-inventory-publish" {
				f.failPut = directory + "backup/kk/backup.info"
			} else {
				f.denied = a
			}
			err = deletePostgresBackup(t.Context(), storage, backup, transport)
			if err == nil {
				t.Fatal("expiration A was not interrupted")
			}
			if _, exists := f.objects[versionKey(storedVersion{Key: directory + ".karakoram-expire/" + a + ".json"})]; !exists {
				t.Fatal("expiration A did not leave a durable plan", err)
			}
			f.denied = ""
			backup.Postgres.Label = b
			if err := deletePostgresBackup(t.Context(), storage, backup, transport); err != nil {
				t.Fatal("expiration B did not complete", err)
			}
			backup.Postgres.Label = a
			if err := deletePostgresBackup(t.Context(), storage, backup, transport); err != nil {
				t.Fatal("expiration A did not resume", err)
			}
			for _, file := range []string{"backup.info", "backup.info.copy"} {
				info := f.data[directory+"backup/kk/"+file]
				if _, err := parsePostgresInfo(info); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(info), a) || strings.Contains(string(info), b) || !strings.Contains(string(info), c) {
					t.Fatal("resumed expiration resurrected a deleted set or removed the retained full")
				}
			}
			for _, object := range f.objects {
				if strings.Contains(object.Key, a) || strings.Contains(object.Key, b) {
					t.Fatal("deleted sets or expiration journals remain", object.Key)
				}
			}
		})
	}
}

func TestPostgresExpirationPrunesRetiredDatabaseHistory(t *testing.T) {
	for _, archiveStart := range []string{"000000020000000000000020", ""} {
		t.Run("surviving-archive-start-"+archiveStart, func(t *testing.T) {
			const directory = "archive/postgres/prod/orders-incarnation/"
			const expired = "20261017-021859F"
			const surviving = "20261018-021859F"
			info := renderPostgresInfo([]infoEntry{
				{"backrest", "backrest-checksum", `""`},
				{"backrest", "backrest-format", `5`},
				{"backrest", "backrest-version", `"2.59.1"`},
				{"backup:current", expired, `{"backup-archive-start":"00000001000000000000000A","db-id":1}`},
				{"backup:current", surviving, `{"backup-archive-start":"` + archiveStart + `","db-id":2}`},
				{"db", "db-id", `2`},
				{"db:history", "1", `{"db-system-id":7620575555724598831,"db-version":"17"}`},
				{"db:history", "2", `{"db-system-id":7620575555724598832,"db-version":"18"}`},
			})
			f := &postgresS3{versionedS3: newVersionedS3(), data: map[string][]byte{}}
			for _, file := range []string{"backup.info", "backup.info.copy"} {
				f.put(directory+"backup/kk/"+file, info)
			}
			oldWAL := "archive/kk/17-1/0000000100000000/00000001000000000000000A"
			oldHistory := "archive/kk/17-1/00000002.history"
			beforeStart := "archive/kk/18-2/0000000200000000/000000020000000000000010"
			atStart := "archive/kk/18-2/0000000200000000/000000020000000000000020"
			afterStart := "archive/kk/18-2/0000000200000000/000000020000000000000030"
			invalidID := "archive/kk/18-2junk/0000000200000000/000000020000000000000010"
			for _, key := range []string{"backup/kk/" + expired + "/backup.manifest", "backup/kk/" + surviving + "/backup.manifest", oldWAL, oldHistory, beforeStart, atStart, afterStart, invalidID} {
				f.put(directory+key, []byte("retained object"))
			}
			server := httptest.NewServer(f)
			t.Cleanup(server.Close)
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.Proxy = nil
			t.Cleanup(transport.CloseIdleConnections)
			storage := domain.BackupStorage{Name: "archive", Namespace: "prod", Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
			backup := domain.Backup{Namespace: "prod", Cluster: "orders", Storage: "archive", Postgres: &domain.PostgresBackup{Label: expired, Repository: strings.TrimSuffix(directory, "/")}}
			for range 2 {
				if err := deletePostgresBackup(t.Context(), storage, backup, transport); err != nil {
					t.Fatal("expiration failed", err)
				}
			}
			for _, key := range []string{oldWAL, oldHistory, "backup/kk/" + expired + "/backup.manifest"} {
				if _, exists := f.objects[versionKey(storedVersion{Key: directory + key})]; exists {
					t.Fatal("retired database history remains", key)
				}
			}
			for _, key := range []string{atStart, afterStart, invalidID, "backup/kk/" + surviving + "/backup.manifest"} {
				if _, exists := f.objects[versionKey(storedVersion{Key: directory + key})]; !exists {
					t.Fatal("surviving backup object was removed", key)
				}
			}
			_, beforeExists := f.objects[versionKey(storedVersion{Key: directory + beforeStart})]
			if beforeExists != (archiveStart == "") {
				t.Fatal("surviving database history WAL boundary was not respected")
			}
			for _, file := range []string{"backup.info", "backup.info.copy"} {
				plan, err := planPostgresExpire(f.data[directory+"backup/kk/"+file], expired)
				if err != nil || plan.Surviving[1] || !plan.Surviving[2] {
					t.Fatal("surviving database history was not preserved", plan, err)
				}
			}
		})
	}
}
