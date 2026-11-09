// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import "testing"

func TestSyncSteady(t *testing.T) {
	f := coreSetup(t, 1)
	r, err := f.p.Sync(t.Context(), f.rc)
	if err != nil || r.Steady {
		t.Fatalf("provisioning must not be steady: %+v %v", r, err)
	}
	coreConverge(t, f)
	r, err = f.p.Sync(t.Context(), f.rc)
	if err != nil || !r.Steady || r.RequeueAfter != 0 {
		t.Fatalf("idle sync: %+v %v", r, err)
	}
	f.rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	r, err = f.p.Sync(t.Context(), f.rc)
	if err != nil || r.Steady {
		t.Fatalf("pending resources must not be steady: %+v %v", r, err)
	}
}
