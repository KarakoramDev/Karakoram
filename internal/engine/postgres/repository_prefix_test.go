// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRepositoryPrefixUsesCanonicalS3Keys(t *testing.T) {
	c := &domain.Cluster{Name: "demo", Namespace: "prod"}
	s := &state{UID: "incarnation"}
	for _, prefix := range []string{"archive", "/archive", "archive/", "/archive/", "//archive//"} {
		t.Run(prefix, func(t *testing.T) {
			storage := &domain.BackupStorage{Namespace: "prod", Prefix: prefix, EndpointURL: "https://objects.example.com"}
			key := repoPrefix(storage, c, s)
			if key != "archive/postgres/prod/demo-incarnation" {
				t.Fatal("noncanonical repository provenance", key)
			}
			repo, err := repository(storage, key)
			if err != nil || repo.Path != "/"+key {
				t.Fatal("pgBackRest path disagrees with repository provenance", err)
			}
			if directory, err := backupstorage.PostgresDirectory(*storage, "prod", "demo", key); err != nil || directory != key+"/" {
				t.Fatal("created repository cannot be cleaned up", err)
			}
		})
	}
}
