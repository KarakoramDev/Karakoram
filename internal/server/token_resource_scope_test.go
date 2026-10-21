// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestClusterRestrictionsDoNotMatchStorageNames(t *testing.T) {
	s, _ := namespaceTestServer(t)
	if err := s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: "other", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []domain.ClusterRef{{Namespace: "default", Name: "archive"}, {Namespace: "default", Name: "prod"}, {Namespace: "other", Name: "prod"}} {
		storage := domain.BackupStorage{Namespace: ref.Namespace, Name: ref.Name, Type: "s3", EndpointURL: "https://s3.example.com", Bucket: "backups", Region: "us-east-1", Credentials: domain.StorageCredentials{AccessKeyID: "synthetic", SecretAccessKey: "synthetic"}}
		if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
			t.Fatal(err)
		}
	}
	for _, cluster := range []string{"prod", "dev"} {
		t.Run(cluster, func(t *testing.T) {
			restrictions := &domain.TokenRestrictions{Clusters: []string{"default/" + cluster}}
			token := restrictedAdmin(t, s, restrictions)
			for _, name := range []string{"archive", "prod"} {
				if w := call(s, "GET", "/api/v1/backup-storages/"+name, "", token); w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
			}
			if w := call(s, "GET", "/api/v1/backup-storages/prod?namespace=other", "", token); w.Code != 403 {
				t.Fatal(w.Code, w.Body.String())
			}
			for _, name := range []string{"prod", "dev"} {
				want := 403
				if name == cluster {
					want = 200
				}
				if w := call(s, "GET", "/api/v1/clusters/"+name, "", token); w.Code != want {
					t.Fatal(w.Code, w.Body.String())
				}
			}
			w := call(s, "GET", "/api/v1/backup-storages", "", token)
			var list api.BackupStorageList
			if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
				t.Fatal(err)
			}
			if w.Code != 200 || len(list.Items) != 2 {
				t.Fatal(w.Code, w.Body.String())
			}
			objects := restrictedPermissionObjects(restrictions, "backup-storages", "**")
			if !slices.Equal(objects, []string{"default/**"}) {
				t.Fatal("storage projection used cluster names", objects)
			}
			objects = restrictedPermissionObjects(restrictions, "clusters", "**")
			if !slices.Equal(objects, []string{"default/" + cluster}) {
				t.Fatal("cluster projection widened", objects)
			}
		})
	}
}
