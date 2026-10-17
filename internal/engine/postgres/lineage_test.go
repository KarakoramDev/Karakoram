// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestRestoreRepositoryGenerationIsDurable(t *testing.T) {
	p, rc, s := backupFixture(t, "single")
	storage, err := p.backupStorage(t.Context(), rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		t.Fatal(err)
	}
	original := repoPrefix(storage, rc.Cluster, s)
	stamp := time.Now().UTC().Add(-time.Hour)
	source := &domain.Backup{Namespace: "default", Cluster: "other", Name: "source", Provider: "postgres", Storage: "archive", Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", CompletedAt: &stamp, Postgres: &domain.PostgresBackup{Repository: "prefix/postgres/default/other-source", Label: "20261017-021859F"}}
	old := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "before-restore", Provider: "postgres", Storage: "archive", Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", CompletedAt: &stamp, Postgres: &domain.PostgresBackup{Repository: original, Label: "20261017-021859F"}}
	for _, b := range []*domain.Backup{source, old} {
		if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
	previous := original
	for iteration := 1; iteration <= 2; iteration++ {
		name := fmt.Sprintf("restore-%d", iteration)
		r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: name, Source: domain.RestoreSource{SourceCluster: "other", Backup: "source"}, Phase: domain.PhasePending}
		if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
			t.Fatal(err)
		}
		payload, _ := json.Marshal(map[string]string{"restore": name})
		op := &domain.Operation{ID: name, Kind: domain.OperationRestore, Payload: payload}
		result, err := p.runRestore(t.Context(), rc, op, backupCheckpoint{})
		if err != nil {
			t.Fatal(err)
		}
		var cp backupCheckpoint
		if err = json.Unmarshal(result.Checkpoint, &cp); err != nil {
			t.Fatal(err)
		}
		if cp.RepositoryGeneration == 0 {
			t.Fatal("external recovery did not reserve a fresh repository")
		}
		// Changing the durable path is deferred until the stop/fence checkpoint.
		current, _ := load(rc.Cluster)
		if repoPrefix(storage, rc.Cluster, current) != previous {
			t.Fatal("repository changed before fencing")
		}
		cp.Step = "credentials"
		for range 2 { // Repeat the same checkpoint, as after a crash before publication.
			p = New(Options{Store: rc.Store})
			if _, err = p.runRestore(t.Context(), rc, op, cp); err != nil {
				t.Fatal(err)
			}
		}
		current, _ = load(rc.Cluster)
		prefix := repoPrefix(storage, rc.Cluster, current)
		if prefix == original || prefix == previous {
			t.Fatal("restore reused a repository lineage", prefix)
		}
		if len(current.RetiredRepositoryGenerations) != iteration {
			t.Fatal("checkpoint retry duplicated retired generation", current.RetiredRepositoryGenerations)
		}
		bs, _ := loadBackupState(rc.Cluster)
		if bs.Restore == nil || bs.Generation != cp.Generation || !bs.ManagedRolesHeld {
			t.Fatal("recovery config or access hold not published durably")
		}
		previous = prefix
	}
	kept, err := rc.Store.GetBackup(t.Context(), rc.Cluster.Ref(), old.Name)
	if err != nil || !reflect.DeepEqual(kept.Postgres, old.Postgres) || kept.Phase != domain.PhaseSucceeded {
		t.Fatal("prior target backup was not retained with its exact repository provenance", err)
	}
	items, err := rc.Store.ListBackups(t.Context(), rc.Cluster.Ref())
	if err != nil || len(items) != 1 {
		t.Fatal("prior generation disappeared from backup inventory", err)
	}
}

func TestRestoreCurrentRepositoryKeepsGeneration(t *testing.T) {
	p, rc, s := backupFixture(t, "single")
	storage, err := p.backupStorage(t.Context(), rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "same", Provider: "postgres", Storage: "archive", Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Repository: repoPrefix(storage, rc.Cluster, s), Label: "20261017-021859F"}}
	if err = rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "same", Source: domain.RestoreSource{Backup: b.Name}}
	if err = rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	result, err := p.runRestore(t.Context(), rc, &domain.Operation{Payload: json.RawMessage(`{"restore":"same"}`)}, backupCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	var cp backupCheckpoint
	_ = json.Unmarshal(result.Checkpoint, &cp)
	if cp.RepositoryGeneration != 0 {
		t.Fatal("same-repository recovery unnecessarily retired its lineage")
	}
	if _, err = p.runRestore(t.Context(), rc, &domain.Operation{Payload: json.RawMessage(`{"restore":"same"}`)}, backupCheckpoint{Step: "credentials", Generation: cp.Generation}); err != nil {
		t.Fatal(err)
	}
	current, _ := load(rc.Cluster)
	if current.RepositoryGeneration != 0 || len(current.RetiredRepositoryGenerations) != 0 {
		t.Fatal("same-repository recovery changed repository")
	}
}

func TestRestoreDifferentStorageSamePathGeneratesNewRepository(t *testing.T) {
	p, rc, s := backupFixture(t, "single")
	storage, err := p.backupStorage(t.Context(), rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		t.Fatal(err)
	}
	alternate := *storage
	alternate.Name = "elsewhere"
	alternate.EndpointURL = "https://different.example"
	if err = rc.Store.(*store.Store).CreateBackupStorage(t.Context(), &alternate); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "elsewhere", Provider: "postgres", Storage: alternate.Name, Phase: domain.PhaseSucceeded, SourceVersion: "17.11.1", Postgres: &domain.PostgresBackup{Repository: repoPrefix(storage, rc.Cluster, s), Label: "20261017-021859F"}}
	if err = rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "elsewhere", Source: domain.RestoreSource{Backup: b.Name}}
	if err = rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	result, err := p.runRestore(t.Context(), rc, &domain.Operation{Payload: json.RawMessage(`{"restore":"elsewhere"}`)}, backupCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	var cp backupCheckpoint
	if err = json.Unmarshal(result.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	if cp.RepositoryGeneration == 0 {
		t.Fatal("same path on another storage endpoint was mistaken for the current repository")
	}
}
