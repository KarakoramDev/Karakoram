// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import "testing"

// This regression uses only the pre-feature backend/fake API surface, so it also
// exercises the old implementation without a compile-only failure.
func TestMarkedProxyCountUpdateInstallsServingHealthGate(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("proxy", ""))
	d := testDeployment(t, "")
	if d.Runtime.Env == nil {
		d.Runtime.Env = map[string]string{}
	}
	d.Runtime.Env["KARAKORAM_PROXY_SERVING_PROBE"] = "1"
	op, err := b.Deploy(t.Context(), "proxy", d)
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	if _, err = b.SetContainerCount(t.Context(), "proxy", 2, 2); err != nil {
		t.Fatal(err)
	}
	row := fakeAppSnapshot(t, f, "proxy")
	if row["healthCheckSwarm"] == nil {
		t.Fatal("proxy count update replaced tasks without a serving healthcheck")
	}
	policy, _ := row["updateConfigSwarm"].(map[string]any)
	if fakeString(policy, "Order") != "start-first" || fakeInt(policy, "Parallelism") != 1 || fakeString(policy, "FailureAction") != "pause" {
		t.Fatalf("unsafe proxy update policy: %+v", policy)
	}
}
