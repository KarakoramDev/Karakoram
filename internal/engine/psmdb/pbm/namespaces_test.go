// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestCrossNamespaceRestoreUsesSourceStorageAndRecoveryIdentity(t *testing.T) {
	env, st, fake, _ := setup(t)
	ctx := context.Background()
	if err := st.PutNamespace(ctx, &domain.Namespace{Name: "production", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	source := &domain.Cluster{Namespace: "production", Name: env.RC.Cluster.Name, Spec: domain.ClusterSpec{Backup: domain.BackupConfig{Storage: "storage"}}, Internal: json.RawMessage(`{"uid":"source-id","pbmPrefix":"source-prefix"}`)}
	if err := st.CreateCluster(ctx, source); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBackupStorage(ctx, &domain.BackupStorage{Namespace: "production", Name: "storage", Type: "s3", Bucket: "source-bucket", EndpointURL: "https://s3.example.com", Credentials: domain.StorageCredentials{AccessKeyID: "source-access", SecretAccessKey: "source-secret"}}); err != nil {
		t.Fatal(err)
	}
	if err := st.SetClusterSecret(ctx, source.Ref(), "users/userAdmin", []byte("source-snapshot-password")); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Namespace: "production", Name: "base", Cluster: source.Name, Storage: "storage", Type: "physical", Phase: domain.PhaseSucceeded, PBMName: "2024-01-05T11:10:54Z", PBMPrefix: "source-prefix"}
	if err := st.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRestore(ctx, &domain.Restore{Name: "clone", Cluster: env.RC.Cluster.Name, SourceNamespace: "production", Source: domain.RestoreSource{SourceCluster: source.Name, Backup: b.Name}}); err != nil {
		t.Fatal(err)
	}
	hooks := &credentialCheckingHooks{rc: env.RC}
	env.Hooks = hooks
	op := &domain.Operation{ID: "cross-namespace-restore", Payload: json.RawMessage(`{"restore":"clone"}`)}
	first, err := RunRestore(ctx, env, op)
	if err != nil {
		t.Fatal(err)
	}
	var cp checkpoint
	if err := json.Unmarshal(first.Checkpoint, &cp); err != nil || cp.SourceNamespace != "production" || cp.Source != source.Name || cp.RecoveryIdentity == "" {
		t.Fatal(cp, err)
	}
	if err := st.DeleteCluster(ctx, source.Ref()); err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = first.Checkpoint
	drive(t, env, op, RunRestore, false)
	wantIdentity := (domain.ClusterRef{Namespace: "production", Name: cp.RecoveryIdentity}).Key()
	if len(hooks.reset) == 0 {
		t.Fatal("restore did not reset system users")
	}
	for _, identity := range hooks.reset {
		if identity != wantIdentity {
			t.Fatalf("recovery identity = %s, want %s", identity, wantIdentity)
		}
	}
	configuredSource := false
	for _, req := range fake.requests {
		var config string
		_ = json.Unmarshal(req.Config, &config)
		if strings.Contains(config, "source-prefix") {
			if !strings.Contains(config, "source-secret") || !strings.Contains(config, "source-bucket") {
				t.Fatal("target PBM did not receive source namespace credentials")
			}
			configuredSource = true
		}
	}
	if !configuredSource {
		t.Fatal("source namespace PBM configuration was not applied")
	}
}
