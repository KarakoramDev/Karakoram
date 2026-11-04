// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRestoreSecretFailureRollsBackAdmission(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "openbao"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.ExecContext(ctx, `CREATE TRIGGER reject_restore_secret BEFORE INSERT ON records WHEN NEW.kind='secret' BEGIN SELECT RAISE(ABORT,'test secret failure'); END`); err != nil {
		t.Fatal(err)
	}
	restore := domain.Restore{Namespace: c.Namespace, Cluster: c.Name, Name: "restore"}
	op := domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: domain.OperationRestore}
	if err := s.EnqueueRestoreWithSecrets(ctx, &restore, &op, map[string][]byte{"openbao/restore-unseal/restore": []byte("private-share")}); err == nil {
		t.Fatal("secret insertion failure accepted")
	}
	if _, err := s.GetRestore(ctx, c.Ref(), restore.Name); !errors.Is(err, ErrNotFound) {
		t.Fatal("restore survived rollback", err)
	}
	operations, err := s.ListOperations(ctx, c.Ref())
	if err != nil || len(operations) != 0 {
		t.Fatal("operation survived rollback", operations, err)
	}
}
