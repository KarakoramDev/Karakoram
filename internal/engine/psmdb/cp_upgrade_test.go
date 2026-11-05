// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
)

func TestCPUpgradeRetainsExistingPSMDBRuntime(t *testing.T) {
	p, rc, agents, _ := renewalCluster(t, 1)
	backend := rc.Backend.(*fakeInfra)
	f := enginetest.RestoreCPUpgrade(t, rc, "testdata/cp_upgrade_ed9541a6.json")
	for i := range f.Services {
		service := f.Services[i]
		backend.services[service.Hostname] = &service
	}
	p.ensureManagedUsers = func(context.Context, *engine.Context, *internalState) error { return nil }
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	agents[s.Members[0].Name].version.Store(s.Members[0].ConfigVersion)
	before, err := p.MemberConfig(t.Context(), rc, s.Members[0].Name)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture really predates the standalone locked shell. Today's runtime
	// differs, but that preparation is not permission to replace running members.
	if !strings.Contains(strings.Join(f.Runtime.Prepare, "\n"), "percona-mongodb-mongosh") || reflect.DeepEqual(f.Runtime.Prepare, PrepareCommands(rc.Cluster.Spec.Version)) {
		t.Fatal("fixture does not cross the mongosh preparation change")
	}
	for range 12 {
		p.ApplyDefaults(&rc.Cluster.Spec)
		if _, err = p.sync(t.Context(), rc,
			func(context.Context, *engine.Context, *internalState, *member) (bool, error) {
				t.Fatal("CP-only upgrade requested a disruptive member rollout")
				return false, nil
			},
			func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (mongoadmin.Status, error) {
				return mongoadmin.Status{Set: m.Set, Members: []mongoadmin.Member{{Name: m.Name + ":27017", State: "PRIMARY", Health: 1, OptimeDate: time.Now()}}}, nil
			}); err != nil {
			t.Fatal(err)
		}
		got, err := p.MemberConfig(t.Context(), rc, s.Members[0].Name)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, before) {
			t.Fatal("CP-only upgrade activated a new member config")
		}
	}
	if rc.Cluster.Spec.Version != "8.0.32-14" || len(backend.deployments) != 0 {
		t.Fatalf("CP-only upgrade replaced existing server/shell: version=%s deployments=%d", rc.Cluster.Spec.Version, len(backend.deployments))
	}
}
