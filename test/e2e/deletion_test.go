//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	api "github.com/KarakoramDev/Karakoram/internal/apiclient"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestDeletionChoices(t *testing.T) {
	cases := []struct {
		name             string
		volumes, backups bool
	}{
		{"KeepVolumesKeepBackups", true, true},
		{"KeepVolumesDeleteBackups", true, false},
		{"DeleteVolumesKeepBackups", false, true},
		{"DeleteAll", false, false},
	}
	for _, choice := range cases {
		t.Run(choice.name, func(t *testing.T) {
			s := newSuite(t, true)
			// A unique test-created storage prefix makes direct object inspection
			// and cleanup safe; never list/delete an existing storage's prefix.
			for _, key := range []string{"ENDPOINT", "BUCKET", "ACCESS_KEY", "SECRET", "REGION"} {
				if os.Getenv("KARAKORAM_E2E_STORAGE_"+key) == "" {
					t.Skip("deletion choices require all five S3 creation variables, even when STORAGE_NAME is configured")
				}
			}
			t.Setenv("KARAKORAM_E2E_STORAGE_NAME", "")
			storage := s.storage()
			var configured api.BackupStorage
			s.expect(&configured)(s.client.GetBackupStorage(s.ctx, storage, nil))
			if configured.Prefix == nil || *configured.Prefix != "karakoram-e2e/"+storage {
				t.Fatal("refusing S3 inspection outside the test-created prefix")
			}
			endpoint, err := url.Parse(configured.EndpointURL)
			if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") {
				t.Fatal("invalid S3 endpoint")
			}
			objects, err := minio.New(endpoint.Host, &minio.Options{
				Creds:  credentials.NewStaticV4(os.Getenv("KARAKORAM_E2E_STORAGE_ACCESS_KEY"), os.Getenv("KARAKORAM_E2E_STORAGE_SECRET"), ""),
				Secure: endpoint.Scheme == "https", Region: configured.Region, BucketLookup: minio.BucketLookupPath,
			})
			if err != nil {
				t.Fatal("create S3 object inspection client failed")
			}
			prefix := strings.TrimRight(*configured.Prefix, "/") + "/"
			list := func(ctx context.Context, snapshot string) map[string]int64 {
				t.Helper()
				found := map[string]int64{}
				for object := range objects.ListObjects(ctx, configured.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
					if object.Err != nil {
						t.Fatal("list test-created S3 prefix failed")
					}
					if !strings.HasPrefix(object.Key, prefix) {
						t.Fatal("S3 returned object outside test prefix")
					}
					found[object.Key] = object.Size
				}
				if snapshot != "" {
					s.snapshot(snapshot, found)
				}
				return found
			}
			t.Cleanup(func() {
				if s.keep {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()
				for key := range list(ctx, "") {
					if err := objects.RemoveObject(ctx, configured.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
						t.Error("cleanup test-created S3 object failed")
					}
				}
			})
			spec := s.spec(1)
			spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
			name := s.create(spec)
			s.put(name, "suspend-proof", "durable")
			backup := s.backup(name, storage, api.CreateBackupRequestTypeLogical)
			if backup.PbmName == nil || *backup.PbmName == "" {
				t.Fatal("backup omitted PBM snapshot identity")
			}
			before := list(s.ctx, name+"-s3-before-delete")
			snapshotKeys := map[string]int64{}
			for key, size := range before {
				if strings.Contains(key, "/"+*backup.PbmName) {
					snapshotKeys[key] = size
				}
			}
			if len(snapshotKeys) == 0 {
				t.Fatal("successful backup has no S3 snapshot objects")
			}
			s.deleteClusterOptions(name, choice.volumes, choice.backups)
			after := list(s.ctx, name+"-s3-after-delete")
			for key, size := range snapshotKeys {
				got, exists := after[key]
				if exists != choice.backups || (exists && got != size) {
					t.Fatalf("snapshot object preservation=%t want=%t", exists, choice.backups)
				}
			}
			if choice.volumes {
				s.phase(name, api.ClusterStatusPhaseSuspended)
				var resumed api.Cluster
				s.expect(&resumed, 202)(s.client.ResumeCluster(s.ctx, name, &api.ResumeClusterParams{}))
				s.ready(name)
				s.proof(name, "suspend-proof", "durable")
				if choice.backups {
					s.restore(name, api.RestoreSource{Backup: ptr(backup.Name), SourceCluster: name})
					s.proof(name, "suspend-proof", "durable")
				} else {
					s.expect(nil, 404)(s.client.GetBackup(s.ctx, name, backup.Name, nil))
				}
			} else {
				s.expect(nil, 404)(s.client.GetCluster(s.ctx, name, nil))
				if choice.backups {
					var retained api.BackupList
					s.expect(&retained)(s.client.ListBackups(s.ctx, name, &api.ListBackupsParams{Limit: ptr(100)}))
					found := false
					for _, item := range retained.Items {
						if item.Name == backup.Name && item.Phase == api.BackupPhaseSucceeded {
							found = true
						}
					}
					if !found {
						t.Fatal("deleted source's retained backup is not listed")
					}
					var inspected api.Backup
					s.expect(&inspected)(s.client.GetBackup(s.ctx, name, backup.Name, nil))
					if inspected.PbmName == nil || *inspected.PbmName != *backup.PbmName {
						t.Fatal("retained backup lost its PBM identity")
					}
					cloneSpec := spec
					cloneSpec.DataSource = &api.DataSource{Cluster: name, Backup: ptr(backup.Name)}
					clone := s.create(cloneSpec)
					s.proof(clone, "suspend-proof", "durable")
					// Remove the clone's own storage references and post-restore
					// base before checking that the original retained record no
					// longer prevents storage deletion.
					s.deleteClusterOptions(clone, false, false)
					s.expect(nil, 204)(s.client.DeleteBackup(s.ctx, name, backup.Name, &api.DeleteBackupParams{}))
					s.expect(nil, 404)(s.client.GetBackup(s.ctx, name, backup.Name, nil))
					removed := list(s.ctx, name+"-s3-after-retained-delete")
					for key := range snapshotKeys {
						if _, exists := removed[key]; exists {
							t.Fatal("retained snapshot object remains after API deletion")
						}
					}
					s.expect(nil, 204)(s.client.DeleteBackupStorage(s.ctx, storage, &api.DeleteBackupStorageParams{}))
				}
			}
		})
	}
}
