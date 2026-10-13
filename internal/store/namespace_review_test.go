// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStorageAADSeparatesLegacyAndNamespacedIdentities(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	legacy := domain.BackupStorage{Name: "prod:archive", Credentials: domain.StorageCredentials{SecretAccessKey: "legacy-secret"}}
	sealed, err := s.seal(storageAAD("default", legacy.Name), legacy.Credentials)
	if err != nil {
		t.Fatal(err)
	}
	if err = put(ctx, s.writer, "storage", "", legacy.Name, &legacy, sealed, "", "", true); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBackupStorage(ctx, "default", legacy.Name)
	if err != nil || got.Credentials.SecretAccessKey != "legacy-secret" {
		t.Fatal(got, err)
	}
	if storageAAD("default", legacy.Name) == storageAAD("prod", "archive") {
		t.Fatal("AAD domains alias")
	}
	if err = s.PutNamespace(ctx, &domain.Namespace{Name: "prod", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	modern := domain.BackupStorage{Namespace: "prod", Name: "archive", Credentials: domain.StorageCredentials{SecretAccessKey: "modern-secret"}}
	if err = s.CreateBackupStorage(ctx, &modern); err != nil {
		t.Fatal(err)
	}
	if err = s.unseal(storageAAD("prod", "archive"), sealed, &modern.Credentials); err == nil {
		t.Fatal("legacy ciphertext accepted in namespaced domain")
	}
	for _, name := range []string{"prod:archive", "prod/archive"} {
		if err = s.CreateBackupStorage(ctx, &domain.BackupStorage{Name: name}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("accepted storage %q: %v", name, err)
		}
	}
}

func TestStoreRejectsAmbiguousClusterReferences(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	good := domain.Backup{Namespace: "prod", Cluster: "orders", Name: "saved"}
	if err := s.CreateBackup(ctx, &good); err != nil {
		t.Fatal(err)
	}
	bad := domain.ClusterRef{Name: "prod:orders"}
	if _, err := s.GetBackup(ctx, bad, "saved"); err == nil {
		t.Fatal("aliased backup returned")
	}
	if _, err := s.ListBackups(ctx, bad); err == nil {
		t.Fatal("aliased inventory returned")
	}
	if err := s.DeleteBackup(ctx, bad, "saved"); err == nil {
		t.Fatal("aliased backup deleted")
	}
	if err := s.SetClusterSecret(ctx, bad, "password", []byte("secret")); err == nil {
		t.Fatal("aliased secret written")
	}
}

func TestNamespaceDeletionInventoryAndInsertGuards(t *testing.T) {
	for _, kind := range []string{"cluster", "storage", "backup", "orphan"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := testStore(t)
			ctx := t.Context()
			if err := s.PutNamespace(ctx, &domain.Namespace{Name: "prod", Phase: "Ready"}); err != nil {
				t.Fatal(err)
			}
			var err error
			switch kind {
			case "cluster":
				err = s.CreateCluster(ctx, &domain.Cluster{Namespace: "prod", Name: "orders", Deleting: true})
			case "storage":
				err = s.CreateBackupStorage(ctx, &domain.BackupStorage{Namespace: "prod", Name: "archive"})
			case "backup":
				err = s.CreateBackup(ctx, &domain.Backup{Namespace: "prod", Cluster: "orders", Name: "saved"})
			case "orphan":
				err = s.PutOrphanedVolume(ctx, &domain.OrphanedVolume{Namespace: "prod", Hostname: "ordersm0v"})
			}
			if err != nil {
				t.Fatal(err)
			}
			var conflict *NamespaceConflictError
			if err = s.BeginNamespaceDeletion(ctx, "prod"); !errors.As(err, &conflict) || !errors.Is(err, ErrConflict) || len(conflict.Blockers) != 1 {
				t.Fatal(err)
			}
			n, err := s.GetNamespace(ctx, "prod")
			if err != nil || n.Phase == "Deleting" {
				t.Fatal(n, err)
			}
		})
	}
	s, _ := testStore(t)
	ctx := t.Context()
	if err := s.PutNamespace(ctx, &domain.Namespace{Name: "prod", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	if err := s.BeginNamespaceDeletion(ctx, "prod"); err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"prod", "missing"} {
		if err := s.CreateCluster(ctx, &domain.Cluster{Namespace: ns, Name: "orders"}); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		if err := s.CreateClusterWithSecrets(ctx, &domain.Cluster{Namespace: ns, Name: "orders"}, map[string][]byte{"password": []byte("secret")}); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
		if err := s.CreateBackupStorage(ctx, &domain.BackupStorage{Namespace: ns, Name: "archive"}); !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
}

func TestNamespaceCreateAndDeletionAreSerialized(t *testing.T) {
	for _, kind := range []string{"cluster", "cluster-secrets", "storage"} {
		t.Run(kind, func(t *testing.T) {
			for range 10 {
				s, _ := testStore(t)
				ctx := t.Context()
				if err := s.PutNamespace(ctx, &domain.Namespace{Name: "prod", Phase: "Ready"}); err != nil {
					t.Fatal(err)
				}
				start := make(chan struct{})
				var createErr, deleteErr error
				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); <-start; deleteErr = s.BeginNamespaceDeletion(ctx, "prod") }()
				go func() {
					defer wg.Done()
					<-start
					switch kind {
					case "cluster":
						createErr = s.CreateCluster(ctx, &domain.Cluster{Namespace: "prod", Name: "orders"})
					case "cluster-secrets":
						createErr = s.CreateClusterWithSecrets(ctx, &domain.Cluster{Namespace: "prod", Name: "orders"}, map[string][]byte{"password": []byte("secret")})
					case "storage":
						createErr = s.CreateBackupStorage(ctx, &domain.BackupStorage{Namespace: "prod", Name: "archive"})
					}
				}()
				close(start)
				wg.Wait()
				if (createErr == nil) == (deleteErr == nil) {
					t.Fatalf("create=%v delete=%v", createErr, deleteErr)
				}
				loser := createErr
				if loser == nil {
					loser = deleteErr
				}
				if !errors.Is(loser, ErrConflict) {
					t.Fatal(loser)
				}
				n, err := s.GetNamespace(ctx, "prod")
				if err != nil || (n.Phase == "Deleting") != (deleteErr == nil) {
					t.Fatal(n, err)
				}
				if deleteErr != nil && !strings.Contains(deleteErr.Error(), "1 ") {
					t.Fatal(deleteErr)
				}
			}
		})
	}
}
