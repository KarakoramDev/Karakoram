// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package backupstorage

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func mysqlBackupFixture(name, parent string) domain.Backup {
	kind := "incremental"
	if parent == "" {
		kind = "full"
	}
	repository := "archive/mysql/prod/orders-incarnation"
	return domain.Backup{Namespace: "prod", Cluster: "orders", Name: name, Storage: "s3", Provider: "mysql", Type: kind, MySQL: &domain.MySQLBackup{Label: name, ParentLabel: parent, Type: kind, Repository: repository, ClusterUID: "incarnation", ObjectPaths: []string{repository + "/base/" + name}}}
}
func TestMySQLRetainedDeletionClosure(t *testing.T) {
	base := mysqlBackupFixture("full", "")
	first := mysqlBackupFixture("first", "full")
	leaf := mysqlBackupFixture("leaf", "first")
	independent := mysqlBackupFixture("other", "")
	foreign := mysqlBackupFixture("foreign", "full")
	foreign.MySQL.Repository = "archive/mysql/prod/orders-another"
	foreign.MySQL.ClusterUID = "another"
	items := []domain.Backup{leaf, independent, first, foreign, base}
	removed := MySQLDeletionClosure(items, base)
	labels := map[string]bool{}
	for _, b := range removed {
		labels[b.MySQL.Label] = true
	}
	if len(labels) != 3 || !labels["full"] || !labels["first"] || !labels["leaf"] {
		t.Fatalf("incorrect closure: %v", labels)
	}
	removed = MySQLDeletionClosure(items, first)
	if len(removed) != 2 {
		t.Fatalf("incremental deletion removed wrong closure: %+v", removed)
	}
}
func TestMySQLRepositoryProvenance(t *testing.T) {
	storage := domain.BackupStorage{Namespace: "prod", Prefix: "archive"}
	b := mysqlBackupFixture("full", "")
	if err := ValidateMySQLBackup(storage, b); err != nil {
		t.Fatal(err)
	}
	storage.Prefix = "changed"
	if err := ValidateMySQLBackup(storage, b); err != nil {
		t.Fatal("mutable prefix invalidated provenance", err)
	}
	for _, repository := range []string{"archive/mysql/dev/orders-incarnation", "archive/mysql/prod/orders-", "archive/mysql/prod/other-incarnation", "archive/mysql/prod/orders-incarnation/..", "/archive/mysql/prod/orders-incarnation", "archive/../mysql/prod/orders-incarnation"} {
		if _, err := MySQLDirectory(storage, "mysql", "prod", "orders", repository); err == nil {
			t.Fatalf("unsafe repository accepted: %s", repository)
		}
	}
	b.MySQL.ClusterUID = "another"
	if err := ValidateMySQLBackup(storage, b); err == nil {
		t.Fatal("mismatched incarnation accepted")
	}
	b = mysqlBackupFixture("full", "")
	b.MySQL.ObjectPaths = []string{"archive/mysql/prod/orders-incarnation/base/full-other"}
	if err := ValidateMySQLBackup(storage, b); err == nil {
		t.Fatal("foreign objects accepted")
	}
}

func TestMySQLProviderProvenance(t *testing.T) {
	storage := domain.BackupStorage{Namespace: "prod", Prefix: "archive"}
	for _, provider := range []string{"mysql", "pxc"} {
		base := mysqlBackupFixture("full", "")
		base.Provider = provider
		base.MySQL.Repository = strings.Replace(base.MySQL.Repository, "/mysql/", "/"+provider+"/", 1)
		base.MySQL.ObjectPaths = []string{base.MySQL.Repository + "/base/" + base.MySQL.Label}
		if err := ValidateMySQLBackup(storage, base); err != nil {
			t.Fatalf("%s provenance rejected: %v", provider, err)
		}
		foreign := mysqlBackupFixture("foreign", "full")
		foreign.Provider = "pxc"
		if provider == "pxc" {
			foreign.Provider = "mysql"
		}
		if got := MySQLDeletionClosure([]domain.Backup{base, foreign}, base); len(got) != 1 || got[0].Name != base.Name {
			t.Fatalf("deletion crossed provider boundary: %+v", got)
		}
	}
	for _, provider := range []string{"", "postgres", "psmdb"} {
		b := mysqlBackupFixture("full", "")
		b.Provider = provider
		if err := ValidateMySQLBackup(storage, b); err == nil {
			t.Fatalf("foreign provider %q accepted", provider)
		}
	}
}

func TestMySQLBinlogPruningUsesGTIDsNotCompletionTime(t *testing.T) {
	const prefix = "archive/mysql/prod/orders-incarnation/binlogs/"
	const sid = "11111111-2222-3333-4444-000000000001"
	f := &postgresS3{versionedS3: newVersionedS3(), data: map[string][]byte{}}
	f.put(prefix+"writer/old", []byte("old log"))
	f.put(prefix+"writer/old.json", []byte(`{"gtid_set":"`+sid+`:1-5"}`))
	f.put(prefix+"writer/needed", []byte("needed log"))
	f.put(prefix+"writer/needed.json", []byte(`{"gtid_set":"`+sid+`:9-15"}`))
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	t.Cleanup(transport.CloseIdleConnections)
	storage := domain.BackupStorage{Type: "s3", EndpointURL: server.URL, Bucket: "test-bucket", ForcePathStyle: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	client, closeClient, err := newClient(storage, storage.Credentials, transport)
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()
	if err = pruneMySQLBinlogs(t.Context(), client, storage.Bucket, prefix, []domain.MySQLBackup{{GTIDSet: sid + ":1-10"}}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{prefix + "writer/old", prefix + "writer/old.json"} {
		if _, exists := f.objects[versionKey(storedVersion{Key: key})]; exists {
			t.Fatal("redundant binlog retained", key)
		}
	}
	for _, key := range []string{prefix + "writer/needed", prefix + "writer/needed.json"} {
		if _, exists := f.objects[versionKey(storedVersion{Key: key})]; !exists {
			t.Fatal("needed binlog deleted", key)
		}
	}
}
