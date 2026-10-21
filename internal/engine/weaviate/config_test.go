// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"path"
	"reflect"
	"testing"
)

func TestExactClusterMemberSpecAndStorageRolling(t *testing.T) {
	f := coreSetup(t, 3)
	ctx := context.Background()
	storage := &domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.test:9444", Region: "region", Bucket: "bucket", Prefix: "prefix", Credentials: domain.StorageCredentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}}
	st := f.rc.Store.(*store.Store)
	if e := st.CreateBackupStorage(ctx, storage); e != nil {
		t.Fatal(e)
	}
	f.rc.Cluster.Spec.Backup.Storage = storage.Name
	s := coreConverge(t, f)
	m := s.Members[1]
	cfg, e := f.p.MemberConfig(ctx, f.rc, m.Name)
	if e != nil {
		t.Fatal(e)
	}
	var got wvspec.MemberSpec
	if e = json.Unmarshal(cfg.EngineSpec, &got); e != nil {
		t.Fatal(e)
	}
	admin, _ := secret(ctx, f.rc, secretAdmin)
	pw, _ := secret(ctx, f.rc, secretClusterPassword)
	want := wvspec.MemberSpec{Version: "1.40.1", NodeName: m.Name, Ports: wvspec.Ports{HTTP: 8080, GRPC: 50051, Gossip: 7946, Data: 7947, Raft: 8300, RaftRPC: 8301, Metrics: 2112}, BindHost: "::", BootstrapExpect: 3, TLS: true, RootUser: AdminUser, AllowedKeys: []string{string(admin)}, ClusterUsername: ClusterUser, ClusterPassword: string(pw), Modules: []string{"backup-s3"}, ReplicationMinimumFactor: 3, AsyncReplication: true, QueryDefaultsLimit: 25, DiskWarningPercent: 80, DiskReadonlyPercent: 90, MemoryLimitMiB: 1740, GOMAXPROCS: 1, Backup: &wvspec.BackupS3{Bucket: "bucket", Path: path.Join("prefix", f.rc.Cluster.Ref().Namespace, "demo", s.UID), Endpoint: "s3.example.test:9444", UseSSL: true, Region: "region", AccessKey: "test-access", SecretKey: "test-secret"}}
	for _, name := range s.Voters {
		want.RaftJoin = append(want.RaftJoin, name+":8301")
		want.GossipJoin = append(want.GossipJoin, name+":7946")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatal("member specification differs from exact expected wire intent (secret values omitted)")
	}
	storage.EndpointURL = "http://s3.example.test:9000"
	if e = st.UpdateBackupStorage(ctx, storage); e != nil {
		t.Fatal(e)
	}
	if _, e = f.p.Sync(ctx, f.rc); e != nil {
		t.Fatal(e)
	}
	s, _ = load(f.rc.Cluster)
	if s.Rolling == nil {
		t.Fatal("storage endpoint change bypassed durable rolling")
	}
	coreConverge(t, f)
	cfg, e = f.p.MemberConfig(ctx, f.rc, m.Name)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(cfg.EngineSpec, &got); e != nil {
		t.Fatal(e)
	}
	if got.Backup.Endpoint != "s3.example.test:9000" || got.Backup.UseSSL {
		t.Fatal("bound storage change not applied")
	}
}

func TestInferenceModulesAreEnabledDeterministically(t *testing.T) {
	f := coreSetup(t, 1)
	ctx := context.Background()
	f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"modules":["text2vec-openai","generative-openai"],"inference":{"text2vec-transformers":{"url":"http://inference.example.test:8080"},"reranker-transformers":{"url":"https://rerank.example.test"}}}`)
	s := coreConverge(t, f)
	var first []byte
	for i := range 5 {
		cfg, e := f.p.MemberConfig(ctx, f.rc, s.Members[0].Name)
		if e != nil {
			t.Fatal(e)
		}
		var got wvspec.MemberSpec
		if e = json.Unmarshal(cfg.EngineSpec, &got); e != nil {
			t.Fatal(e)
		}
		if e = got.Validate(); e != nil {
			t.Fatalf("inference options produce an invalid member spec: %v", e)
		}
		want := []string{"backup-s3", "generative-openai", "reranker-transformers", "text2vec-openai", "text2vec-transformers"}
		if !reflect.DeepEqual(got.Modules, want) {
			t.Fatalf("modules = %v, want %v", got.Modules, want)
		}
		if got.InferenceEndpoints["TRANSFORMERS_INFERENCE_API"] != "http://inference.example.test:8080" || got.InferenceEndpoints["RERANKER_INFERENCE_API"] != "https://rerank.example.test" {
			t.Fatalf("inference endpoints = %v", got.InferenceEndpoints)
		}
		if i == 0 {
			first = cfg.EngineSpec
		} else if string(first) != string(cfg.EngineSpec) {
			t.Fatal("member spec is not deterministic across renders")
		}
	}
}
