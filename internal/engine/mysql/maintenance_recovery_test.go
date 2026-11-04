// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"testing"
	"time"
)

func TestMaintenanceResourceWaitDoesNotBlockGroupHealing(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	before, _ := load(rc.Cluster)
	old := before.data()[0].Resources
	generation := rc.Cluster.Status.ObservedGeneration
	rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(time.Now().Month())%12+1), Duration: "1h"}}}
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM++
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	rc.Cluster.Generation++
	for _, m := range before.data() {
		st := b.native[m.Name]
		st.MemberState = "OFFLINE"
		b.native[m.Name] = st
		b.offline[m.Name] = myspec.OfflineGTIDResponse{Stopped: true, ServerUUID: st.ServerUUID, GTIDExecuted: "11111111-2222-3333-4444-555555555555:1-3", DataIdentity: myspec.DataIdentity{Initialized: true}}
	}
	b.leader = ""
	recovered := false
	for range 100 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		s, _ := load(rc.Cluster)
		recovered = true
		for _, m := range s.data() {
			if b.native[m.Name].MemberState != "ONLINE" {
				recovered = false
			}
			if m.Resources != old {
				t.Fatal("resource change escaped closed gate")
			}
		}
		if recovered {
			break
		}
	}
	if !recovered {
		t.Fatal("maintenance wait starved group reformation")
	}
	if rc.Cluster.Status.ObservedGeneration != generation {
		t.Fatal("deferred desired resources acknowledged")
	}
}
