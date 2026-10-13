//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	api "github.com/KarakoramDev/Karakoram/internal/apiclient"
	"os"
	"strings"
	"time"
)

func (s *suite) storage() string {
	s.t.Helper()
	name := os.Getenv("KARAKORAM_E2E_STORAGE_NAME")
	// Namespace teardown requires a test-owned storage, not a shared object.
	if s.namespace != nil {
		name = ""
	}
	if name == "" {
		keys := []string{"ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET", "REGION"}
		values := make(map[string]string, len(keys))
		for _, key := range keys {
			v := os.Getenv("KARAKORAM_E2E_STORAGE_" + key)
			if v == "" {
				s.t.Skip("set KARAKORAM_E2E_STORAGE_NAME or all KARAKORAM_E2E_STORAGE_{ENDPOINT,BUCKET,ACCESS_KEY,SECRET,REGION}")
			}
			values[key] = v
		}
		name = s.name()
		s.t.Cleanup(func() {
			if s.keep {
				s.t.Logf("KEEP backup storage=%s", name)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			s.expect(nil, 204, 404)(s.client.DeleteBackupStorage(ctx, name, &api.DeleteBackupStorageParams{Namespace: s.namespace}))
		})
		var created api.BackupStorage
		s.expect(&created, 201)(s.client.CreateBackupStorage(s.ctx, &api.CreateBackupStorageParams{Namespace: s.namespace}, api.CreateBackupStorageRequest{Name: name, Type: api.CreateBackupStorageRequestTypeS3, EndpointURL: values["ENDPOINT"], Region: values["REGION"], Bucket: values["BUCKET"], Prefix: ptr("karakoram-e2e/" + name), ForcePathStyle: ptr(true), VerifyTLS: ptr(true), Credentials: api.S3Credentials{AccessKeyId: ptr(values["ACCESS_KEY"]), SecretAccessKey: ptr(values["SECRET"])}}))
	}
	var configured api.BackupStorage
	s.expect(&configured)(s.client.GetBackupStorage(s.ctx, name, &api.GetBackupStorageParams{Namespace: s.namespace}))
	if configured.Name != name {
		s.t.Fatal("storage name mismatch")
	}
	if !configured.ForcePathStyle || !configured.VerifyTLS || !strings.HasPrefix(configured.EndpointURL, "https://") {
		s.t.Fatal("backup tests require HTTPS S3 with forcePathStyle and verifyTLS enabled")
	}
	s.snapshot(name+"-storage", configured)
	var verified api.StorageVerification
	s.expect(&verified)(s.client.VerifyBackupStorage(s.ctx, name, &api.VerifyBackupStorageParams{Namespace: s.namespace}))
	if !verified.Verified {
		s.t.Fatal("storage verification failed")
	}
	s.t.Logf("storage=%s verified=%t checkedAt=%s", name, verified.Verified, verified.CheckedAt)
	s.snapshot(name+"-storage-verification", verified)
	return name
}
func (s *suite) backup(name, storage string, kind api.CreateBackupRequestType) api.Backup {
	backup := s.name()
	var b api.Backup
	s.expect(&b, 202)(s.client.CreateBackup(s.ctx, name, &api.CreateBackupParams{Namespace: s.namespace}, api.CreateBackupRequest{Name: backup, Storage: storage, Type: kind}))
	return s.waitBackup(name, backup)
}
func (s *suite) waitBackup(name, backup string) api.Backup {
	var b api.Backup
	s.poll("backup "+backup, operationTimeout, func() bool {
		s.expect(&b)(s.client.GetBackup(s.ctx, name, backup, &api.GetBackupParams{Namespace: s.namespace}))
		s.t.Logf("cluster=%s backup=%s phase=%s type=%s", name, b.Name, b.Phase, b.Type)
		s.backupOperationProgress(name)
		if b.Phase == api.BackupPhaseFailed {
			s.snapshot(name+"-backup-failed", b)
			s.t.Fatalf("backup %s failed", backup)
		}
		if b.Phase == api.BackupPhaseSucceeded {
			s.snapshot(name+"-backup", b)
			return true
		}
		return false
	})
	return b
}
func (s *suite) inventory(name string) []api.Backup {
	var all []api.Backup
	var cursor *string
	for {
		var list api.BackupList
		s.expect(&list)(s.client.ListBackups(s.ctx, name, &api.ListBackupsParams{Namespace: s.namespace, Limit: ptr(100), Cursor: cursor}))
		all = append(all, list.Items...)
		if list.NextCursor == nil {
			return all
		}
		cursor = list.NextCursor
	}
}
func (s *suite) restore(name string, source api.RestoreSource) {
	restore := s.name()
	var r api.Restore
	s.expect(&r, 202)(s.client.CreateRestore(s.ctx, name, &api.CreateRestoreParams{Namespace: s.namespace}, api.CreateRestoreRequest{Name: restore, Source: source}))
	s.waitRestore(name, restore)
}

func (s *suite) waitRestore(name, restore string) {
	var r api.Restore
	s.poll("restore "+restore, operationTimeout, func() bool {
		s.expect(&r)(s.client.GetRestore(s.ctx, name, restore, &api.GetRestoreParams{Namespace: s.namespace}))
		s.t.Logf("cluster=%s restore=%s phase=%s", name, r.Name, r.Phase)
		s.backupOperationProgress(name)
		if r.Phase == api.RestorePhaseFailed {
			s.snapshot(name+"-restore-failed", r)
			s.t.Fatalf("restore %s failed", restore)
		}
		if r.Phase == api.RestorePhaseSucceeded {
			s.snapshot(name+"-restore", r)
			return true
		}
		return false
	})
	s.ready(name)
}

func (s *suite) backupOperationProgress(name string) {
	var operations api.OperationList
	s.expect(&operations)(s.client.ListClusterOperations(s.ctx, name, &api.ListClusterOperationsParams{Namespace: s.namespace, Limit: ptr(20)}))
	for _, op := range operations.Items {
		if op.Type == api.OperationTypeBackup || op.Type == api.OperationTypeRestore {
			s.t.Logf("cluster=%s operation=%s type=%s phase=%s", name, op.Id, op.Type, op.Phase)
		}
	}
}
