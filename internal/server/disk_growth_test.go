// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestDiskGrowthAdmission(t *testing.T) {
	for _, kind := range []string{"zerops", "dokploy", "nomad"} {
		for _, supported := range []bool{false, true} {
			for _, method := range []string{"PATCH", "PUT"} {
				for _, bound := range []string{"minDisk", "maxDisk"} {
					t.Run(fmt.Sprintf("%s/%t/%s/%s", kind, supported, method, bound), func(t *testing.T) {
						s, _, token := testServer(t)
						caps := infra.Capabilities{Logs: true, DiskGrowth: supported}
						var err error
						s.Backends, err = infra.NewRegistry(fake.NewFactory(fake.Options{Name: kind, Capabilities: caps}))
						if err != nil {
							t.Fatal(err)
						}
						if err := s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: "target", Backend: kind, Phase: "Ready"}); err != nil {
							t.Fatal(err)
						}
						s.namespaces = exposureNamespaces{backends: map[string]infra.Backend{"target": fake.New(fake.Options{Name: kind, Capabilities: caps})}}
						initial := `{"name":"disk","spec":{"provider":"fake","components":{"engine":{"replicas":1,"resources":{"cpuMode":"SHARED","minCpu":1,"maxCpu":1,"minRam":1,"maxRam":1,"minDisk":5,"maxDisk":10}}}}}`
						if w := call(s, "POST", "/api/v1/clusters?namespace=target", initial, token); w.Code != 201 {
							t.Fatal(w.Code, w.Body.String())
						}
						ref := domain.ClusterRef{Namespace: "target", Name: "disk"}
						before, err := s.Store.GetCluster(t.Context(), ref)
						if err != nil {
							t.Fatal(err)
						}
						ops, err := s.Store.ListOperations(t.Context(), ref)
						if err != nil {
							t.Fatal(err)
						}
						minDisk, maxDisk := 5, 10
						if bound == "minDisk" {
							minDisk++
						} else {
							maxDisk++
						}
						body := fmt.Sprintf(`{"spec":{"provider":"fake","components":{"engine":{"replicas":2,"resources":{"cpuMode":"SHARED","minCpu":1,"maxCpu":1,"minRam":1,"maxRam":1,"minDisk":%d,"maxDisk":%d}}}}}`, minDisk, maxDisk)
						for _, prefix := range []string{"/api/v1/dry-run/clusters/", "/api/v1/clusters/"} {
							w := call(s, method, prefix+"disk?namespace=target", body, token)
							if supported {
								if w.Code != 200 {
									t.Fatal(w.Code, w.Body.String())
								}
							} else if w.Code != 400 || !strings.Contains(w.Body.String(), "invalid_request") || !strings.Contains(w.Body.String(), "backend "+kind+" does not provide diskGrowth") {
								t.Fatal(w.Code, w.Body.String())
							}
						}
						if !supported {
							after, err := s.Store.GetCluster(t.Context(), ref)
							if err != nil || !reflect.DeepEqual(before, after) {
								t.Fatalf("rejected growth changed cluster: %v", err)
							}
							afterOps, err := s.Store.ListOperations(t.Context(), ref)
							if err != nil || !reflect.DeepEqual(ops, afterOps) {
								t.Fatalf("rejected growth queued work: %v", err)
							}
							if w := call(s, "PATCH", "/api/v1/clusters/disk?namespace=target", `{"spec":{"components":{"engine":{"replicas":2}}}}`, token); w.Code != 200 {
								t.Fatal("scale without growth rejected", w.Code, w.Body.String())
							}
						}
					})
				}
			}
		}
	}
}
