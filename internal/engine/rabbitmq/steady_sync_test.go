// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import "testing"

func TestSyncSteady(t *testing.T) {
	p, rc, _, _ := setup(t)
	r, err := p.Sync(t.Context(), rc)
	if err != nil || r.Steady {
		t.Fatalf("provisioning must not be steady: %+v %v", r, err)
	}
	ready(t, p, rc)
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
