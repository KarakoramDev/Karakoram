// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package elastic

import "testing"

func TestSyncSteady(t *testing.T) {
	p, rc, b, _ := coreSetup(t, 1)
	r, err := p.Sync(t.Context(), rc)
	if err != nil || r.Steady {
		t.Fatalf("provisioning must not be steady: %+v %v", r, err)
	}
	coreConverge(t, p, rc, b)
	r, err = p.Sync(t.Context(), rc)
	if err != nil || !r.Steady || r.RequeueAfter != 0 {
		t.Fatalf("idle sync: %+v %v", r, err)
	}
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	r, err = p.Sync(t.Context(), rc)
	if err != nil || r.Steady {
		t.Fatalf("pending resources must not be steady: %+v %v", r, err)
	}
}
