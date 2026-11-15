// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRetainedDeletionPreflightRefusals(t *testing.T) {
	p := &Core{}
	storage := domain.BackupStorage{Name: "archive"}
	set := domain.PostgresBackup{Label: "20260101-010101F", Repository: "prefix/postgres/default/gone-0001"}
	valid := domain.Backup{Cluster: "gone", Storage: "archive", Postgres: &set}
	with := func(change func(*domain.Backup)) domain.Backup {
		b := valid
		copy := set
		b.Postgres = &copy
		change(&b)
		return b
	}
	citus := func(repository string) domain.Backup {
		return with(func(b *domain.Backup) {
			b.Postgres.Groups = []domain.PostgresBackupGroup{{Group: 0, Backup: set}, {Group: 1, Backup: domain.PostgresBackup{Label: set.Label, Repository: repository}}}
		})
	}
	for _, tc := range []struct {
		name    string
		backup  domain.Backup
		refused bool
	}{
		{"incomplete", domain.Backup{Cluster: "gone", Storage: "archive", Postgres: &domain.PostgresBackup{Repository: set.Repository}}, true},
		{"missing-provenance", domain.Backup{}, true},
		{"malformed-label", with(func(b *domain.Backup) { b.Postgres.Label = "full" }), true},
		{"other-storage", with(func(b *domain.Backup) { b.Storage = "other" }), true},
		{"foreign-repository", with(func(b *domain.Backup) { b.Postgres.Repository = "repo" }), true},
		{"other-incarnation", with(func(b *domain.Backup) { b.Cluster = "other" }), true},
		{"citus-foreign-group", citus("prefix/postgres/other/gone-0001/g1"), true},
		{"unencrypted-full", valid, false},
		{"citus-full", citus("prefix/postgres/default/gone-0001/g1"), false},
		{"citus-point", domain.Backup{Trigger: "pitr", Postgres: &domain.PostgresBackup{Groups: []domain.PostgresBackupGroup{{Group: 0}}}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := p.ValidateRetainedBackupDeletion(t.Context(), storage, tc.backup)
			if (err != nil) != tc.refused {
				t.Fatal("unexpected preflight result", err)
			}
			if !tc.refused {
				return
			}
			var refusal *engine.RetainedBackupDeletionRefusal
			var resource *domain.ResourceError
			if !errors.As(err, &refusal) || !errors.As(err, &resource) || !errors.Is(err, engine.ErrConflict) {
				t.Fatal("deterministic refusal lost its contract", err)
			}
			// Rejection must precede all storage access and mutation in the
			// deleter too: this storage has no endpoint or credentials.
			if err := p.DeleteRetainedBackup(t.Context(), storage, tc.backup); !errors.As(err, &refusal) {
				t.Fatal("deleter did not repeat preflight", err)
			}
		})
	}
}
