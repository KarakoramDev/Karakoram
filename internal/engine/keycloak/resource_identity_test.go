// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"reflect"
	"testing"
)

func TestStartCPUValueSurvivesReconciliation(t *testing.T) {
	p, rc, bindings, _ := setup(t)
	start := 1.0
	rc.Cluster.Spec.Components.Engine.Resources.StartCPU = &start
	converge(t, p, rc)
	converge(t, p, rc)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.Pending != nil || !s.Deployed || bindings.backend.deploys != 1 || bindings.backend.resizes != 0 {
		t.Fatalf("unchanged resources caused a rollout: pending=%v deploys=%d resizes=%d", s.Pending, bindings.backend.deploys, bindings.backend.resizes)
	}
	// A distinct pointer with the same value is not new desired intent.
	same := start
	rc.Cluster.Spec.Components.Engine.Resources.StartCPU = &same
	converge(t, p, rc)
	if bindings.backend.resizes != 0 {
		t.Fatal("equal start CPU values resized the runtime")
	}
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM++
	converge(t, p, rc)
	s, err = load(rc.Cluster)
	if err != nil || s.Pending != nil || !reflect.DeepEqual(s.Resources, rc.Cluster.Spec.Components.Engine.Resources) || bindings.backend.resizes != 1 {
		t.Fatalf("real resource change did not converge exactly once: resizes=%d err=%v", bindings.backend.resizes, err)
	}
}

func TestFailedResizeReceiptRecovery(t *testing.T) {
	for _, kind := range []string{"redundant", "real-change", "missing-runtime", "different-runtime-resources"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, bindings, _ := setup(t)
			start := 1.0
			rc.Cluster.Spec.Components.Engine.Resources.StartCPU = &start
			converge(t, p, rc)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.Pending = &pending{Action: "resize", Resources: rc.Cluster.Spec.Components.Engine.Resources}
			switch kind {
			case "real-change":
				s.Pending.Resources.MaxRAM++
			case "missing-runtime":
				bindings.backend.service = nil
			case "different-runtime-resources":
				bindings.backend.service.Resources.MaxRAM++
			}
			if err = save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			bindings.backend.fail = true
			_, err = p.Sync(t.Context(), rc)
			if kind != "redundant" {
				if err == nil {
					t.Fatal("retired a failed resize without its accepted owned runtime")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			s, err = load(rc.Cluster)
			if err != nil || s.Pending != nil || !s.Deployed {
				t.Fatalf("redundant failed receipt remained pinned: %v", err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil || len(status.Members) != 1 || status.Phase == "Ready" {
				t.Fatal("receipt retirement must restore observation without inventing native readiness", status, err)
			}
			converge(t, p, rc)
			if bindings.backend.resizes != 0 || bindings.backend.deploys != 1 {
				t.Fatal("recovery issued another rollout")
			}
		})
	}
}
