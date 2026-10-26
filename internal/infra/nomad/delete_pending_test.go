// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestFencedDeleteSupersedesPlacementBlockedDeploy(t *testing.T) {
	f, b, deploy := blockedDeployment(t, false)
	if err := b.SetEnvVars(t.Context(), "runtime", map[string]string{"KARAKORAM_RELAY_OWNER": "test-owner"}); err != nil {
		t.Fatal(err)
	}
	r, err := b.load(t.Context(), "runtime")
	if err != nil || r == nil {
		t.Fatalf("missing owned service: %v", err)
	}
	for _, path := range []string{recordPrefix + "runtime", "nomad/jobs/runtime"} {
		v, err := b.readVariable(t.Context(), path)
		if err != nil || v.ModifyIndex == 0 {
			t.Fatalf("missing initial variable %s: %v", path, err)
		}
	}
	if _, err = b.DeleteServiceByID(t.Context(), "runtime", r.ID); err != nil {
		t.Fatal(err)
	}
	old, err := b.OpStatus(t.Context(), deploy)
	if err != nil || !old.Done || !old.Expired {
		t.Fatalf("old deploy was not superseded: %+v %v", old, err)
	}
	// Reconstruct as after a crash before the CP persisted the delete receipt.
	// A second fenced delete must finish the same owned cleanup safely.
	b = fakeBackend(t, f, nil)
	op, err := b.DeleteServiceByID(t.Context(), "runtime", r.ID)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, roundTripOp(t, op))
	var job map[string]any
	if _, err = b.c.get(t.Context(), "/v1/job/runtime", nil, &job); !errors.Is(err, infra.ErrNotFound) {
		t.Fatalf("job not purged: %v", err)
	}
	for _, path := range []string{recordPrefix + "runtime", "nomad/jobs/runtime"} {
		v, err := b.readVariable(t.Context(), path)
		if err != nil || v.ModifyIndex != 0 {
			t.Fatalf("variable %s survived deletion: index=%d error=%v", path, v.ModifyIndex, err)
		}
	}
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service != nil {
		t.Fatalf("deleted service remains visible: %+v %v", service, err)
	}
}
