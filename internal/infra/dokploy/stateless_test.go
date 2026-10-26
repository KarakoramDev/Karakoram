// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import "testing"

func TestDeploymentUpdateOrderDependsOnPersistentVolume(t *testing.T) {
	for _, volume := range []string{"", "volume"} {
		name := "stateless"
		want := "start-first"
		if volume != "" {
			name, want = "persistent", "stop-first"
		}
		t.Run(name, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			if volume != "" {
				importFake(t, b, volumeSpec(volume))
			}
			importFake(t, b, runtimeSpec("runtime", volume))
			deployFake(t, b, "runtime", volume)
			app := fakeAppSnapshot(t, f, "runtime")
			for _, key := range []string{"updateConfigSwarm", "rollbackConfigSwarm"} {
				policy, ok := app[key].(map[string]any)
				if !ok || policy["Order"] != want || fakeInt(policy, "Parallelism") != 1 {
					t.Fatalf("%s=%+v; want %s serial update", key, policy, want)
				}
			}
		})
	}
}
