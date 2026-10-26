//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// TestNativeStorageDeploymentBoundaries is local/native + a fake platform seam,
// never real-platform acceptance. It runs serially on ONE exclusively owned
// standard HA3 strict-sync fixture with two Auth and two REST native instances.
// Each independent sequence uses a genuine new Storage incarnation on the same
// cluster/PG/S3 fixture. Within a sequence its exact native target, selected
// version and SHA-bound runtime stay immutable across failure/reconstruction/
// recovery; later sequences must also read the original acknowledged object.
// Assertions inspect the real reconciler.Manager's durable publication, never
// raw provider health generations or manually normalized status snapshots.
func TestNativeStorageDeploymentBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	f := openNativeStorageFixture(t, ctx)

	t.Run("DEPLOY_FAILED_same_uploaded_version_retry_cap", func(t *testing.T) {
		f.t = t
		f.begin(infra.OpState{Status: "PREPARING_RUNTIME"})
		_ = f.step()
		f.diagnose("ComponentDeploymentPending", "PREPARING_RUNTIME")
		selected := f.state().Pending.Op
		version := f.b.r.Version
		f.b.r.Outcome = infra.OpState{Done: true, Failed: true, Status: "DEPLOY_FAILED"}
		nativeStorageMust(t, "persist exact terminal outcome", f.b.persist())
		if f.step() == nil {
			t.Fatal("terminal Storage deployment was not actionable")
		}
		f.diagnose("ComponentDeploymentFailed", "DEPLOY_FAILED")
		failure := *f.state().Members["storage"].DeploymentFailure
		f.reopen()
		if got := f.state().Members["storage"].DeploymentFailure; got == nil || !reflect.DeepEqual(*got, failure) {
			t.Fatal("CP reconstruction lost exact terminal failure")
		}
		if !reflect.DeepEqual(failure.Op, selected) {
			t.Fatal("terminal evidence replaced selected uploaded-version handle")
		}
		// Four accepted synthetic submissions: initial plus three retries. CP
		// reconstruction after every one must not allocate a successor version.
		for expected := 2; expected <= 4; expected++ {
			f.untilPending()
			if f.b.r.Version != version || !reflect.DeepEqual(f.state().Pending.Op, selected) || f.b.r.Accepted != expected {
				t.Fatal("same-version retry identity/count changed")
			}
			before := f.b.r
			f.reopen()
			if !reflect.DeepEqual(before, f.b.r) {
				t.Fatal("reconstruction changed selected attempt")
			}
			if f.step() == nil {
				t.Fatal("accepted terminal retry was not actionable")
			}
			f.diagnose("ComponentDeploymentFailed", "DEPLOY_FAILED")
		}
		_ = f.step() // backend cap becomes the product's persisted deployment guard
		guard, ok := f.rc.Cluster.Status.DeploymentFailures[f.b.member]
		if !ok || guard.Attempts != 4 || guard.Version != version {
			t.Fatal("durable same-version retry cap not recorded by actual Core/component caller")
		}
		f.reopen()
		requests := f.b.r.Requests
		for range 3 {
			_ = f.step()
		}
		if f.b.r.Requests != requests || f.b.r.Accepted != 4 || f.b.r.NativeDeploys != 0 {
			t.Fatal("exhausted deployment replayed after CP reconstruction")
		}
		status := f.status()
		if status.Phase == domain.PhaseReady || status.ObservedGeneration >= f.rc.Cluster.Generation {
			t.Fatal("exhausted native target advertised current-generation readiness")
		}
		for _, typ := range []string{"Ready", "ComponentsReady"} {
			found := false
			for _, condition := range status.Conditions {
				if condition.Type == typ {
					found = condition.Status == "False"
				}
			}
			if !found {
				t.Fatal("published exhaustion lost a combined nonReady condition")
			}
		}
		// Explicit operator epoch, not an automatic/new-version retry.
		nativeStorageMust(t, "persist explicit operator re-arm", f.st.IncrementClusterRetryEpoch(ctx, f.cfg.Ref))
		f.reopen()
		f.heal()
		if f.b.r.Version != version || f.b.r.NativeDeploys != 1 {
			t.Fatal("native recovery replaced the selected upload or duplicated submission")
		}
	})
	if t.Failed() {
		return
	}

	for _, prior := range []bool{false, true} {
		name := "initial_pre_send_RETRY_REQUIRED"
		if prior {
			name = "prior_terminal_pre_send_RETRY_REQUIRED"
		}
		t.Run(name, func(t *testing.T) {
			f.t = t
			outcome := infra.OpState{Done: true, Failed: true, Status: "RETRY_REQUIRED"}
			if prior {
				outcome.Status = "DEPLOY_FAILED"
			}
			f.begin(outcome)
			var failure *componentDeploymentFailure
			if prior {
				if f.step() == nil {
					t.Fatal("prior terminal failure not observed")
				}
				copy := *f.state().Members["storage"].DeploymentFailure
				failure = &copy
				f.b.r.Outcome.Status = "RETRY_REQUIRED"
				nativeStorageMust(t, "persist pre-send retry marker", f.b.persist())
				f.untilPending()
			}
			selected := f.state().Pending.Op
			receipt := f.b.r
			f.reopen() // durable request marker exists, no HTTP send was accepted
			if !reflect.DeepEqual(f.state().Pending.Op, selected) || !reflect.DeepEqual(receipt, f.b.r) {
				t.Fatal("pre-send reconstruction changed immutable attempt")
			}
			_ = f.step()
			f.roundtrip() // The pre-send classification cannot lose acknowledged data.
			s := f.state()
			if s.Pending != nil {
				t.Fatal("definitively unsubmitted attempt was not retired")
			}
			got := s.Members["storage"].DeploymentFailure
			if !reflect.DeepEqual(got, failure) {
				t.Fatal("RETRY_REQUIRED invented or replaced terminal evidence")
			}
			wantAccepted := 0
			if prior {
				wantAccepted = 1
				f.diagnose("ComponentDeploymentFailed", "DEPLOY_FAILED")
			}
			if f.b.r.Accepted != wantAccepted || f.b.r.NativeDeploys != 0 {
				t.Fatal("pre-send caller accepted a duplicate mutation")
			}
			if !prior {
				status := f.status()
				if status.ObservedGeneration == f.rc.Cluster.Generation {
					t.Fatal("unsubmitted initial deployment advertised current generation")
				}
				for _, c := range status.Conditions {
					if c.Reason == "ComponentDeploymentFailed" {
						t.Fatal("unsubmitted initial request became terminal platform failure")
					}
				}
			}
			f.assertTarget()
			f.reopen()
			if !reflect.DeepEqual(f.state().Members["storage"].DeploymentFailure, failure) {
				t.Fatal("retired pre-send diagnosis did not survive reconstruction")
			}
			f.heal()
			if f.b.r.Accepted != wantAccepted+1 || f.b.r.NativeDeploys != 1 {
				t.Fatal("pre-send recovery accepted more than one native mutation")
			}
		})
		if t.Failed() {
			return
		}
	}

	for _, terminal := range []bool{false, true} {
		name, reason, phase := "pending_inventory_read_outage", "ComponentDeploymentPending", "PREPARING_RUNTIME"
		if terminal {
			name, reason, phase = "terminal_inventory_read_outage", "ComponentDeploymentFailed", "DEPLOY_FAILED"
		}
		t.Run(name, func(t *testing.T) {
			f.t = t
			f.begin(infra.OpState{Done: terminal, Failed: terminal, Status: phase})
			_ = f.step()
			before := f.state()
			receipt := f.b.r
			f.b.r.InventoryError = true
			f.persist()
			f.observeInventory()
			f.diagnose(reason, phase)
			f.reopen()
			if !reflect.DeepEqual(before, f.state()) {
				t.Fatal("inventory outage reconstruction changed immutable deployment checkpoint")
			}
			if f.b.r.Version != receipt.Version || f.b.r.Accepted != receipt.Accepted || f.b.r.Requests != receipt.Requests {
				t.Fatal("inventory outage replayed selected request")
			}
			f.diagnose(reason, phase)
			f.b.r.InventoryError = false
			f.persist()
			f.observeInventory()
			f.diagnose(reason, phase) // a healed inventory is not deployment success
			f.heal()
			if f.b.r.NativeDeploys != 1 || f.b.r.Accepted != receipt.Accepted+1 {
				t.Fatal("inventory recovery duplicated the native mutation")
			}
		})
		if t.Failed() {
			return
		}
	}
}
