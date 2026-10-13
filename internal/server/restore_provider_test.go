// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type restoreTestProvider struct {
	*fakeProvider
	name string
}

func (p *restoreTestProvider) Name() string { return p.name }

func TestRestoreAndCloneRequireMatchingBackupProvider(t *testing.T) {
	for _, target := range []string{"psmdb", "test-first", "test-second"} {
		for _, source := range []string{"", "psmdb", "test-first", "test-second"} {
			t.Run(target+"/"+source, func(t *testing.T) {
				s, _, token := testServer(t)
				if err := s.Registry.Register(&restoreTestProvider{fakeProvider: &fakeProvider{}, name: target}); err != nil {
					t.Fatal(err)
				}
				c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
				if err != nil {
					t.Fatal(err)
				}
				c.Spec.Provider = target
				if err = s.Store.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
					t.Fatal(err)
				}
				backup := domain.Backup{Name: "saved", Cluster: c.Name, Provider: source, Type: "snapshot", Phase: domain.PhaseSucceeded}
				if source == "rabbitmq" {
					backup.Type = "definitions"
					backup.Location = "saved.json.gz"
				}
				if err = s.Store.CreateBackup(t.Context(), &backup); err != nil {
					t.Fatal(err)
				}
				matches := source == target || source == "" && target == "psmdb"
				for _, clone := range []bool{false, true} {
					path, body, want := "/api/v1/clusters/prod/restores", `{"name":"restore","source":{"sourceCluster":"prod","backup":"saved"}}`, 202
					if clone {
						path, want = "/api/v1/clusters", 201
						body = fmt.Sprintf(`{"name":"copy","spec":{"provider":%q,"version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":1}},"dataSource":{"cluster":"prod","backup":"saved"}}}`, target)
					}
					if !matches {
						want = 400
					}
					w := call(s, "POST", path, body, token)
					if w.Code != want {
						t.Fatalf("clone=%t status=%d want=%d body=%s", clone, w.Code, want, w.Body.String())
					}
				}
				if !matches {
					ops, err := s.Store.ListOperations(t.Context(), c.Ref())
					if err != nil || len(ops) != 0 {
						t.Fatalf("incompatible restore enqueued operations: %+v error=%v", ops, err)
					}
					if _, err = s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "copy"}); err == nil {
						t.Fatal("incompatible clone was persisted")
					}
				}
			})
		}
	}
}
