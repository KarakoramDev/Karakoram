// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type reconcileTrigger struct {
	forced   []domain.ClusterRef
	ordinary int
}

func (f *reconcileTrigger) Trigger(domain.ClusterRef)            { f.ordinary++ }
func (f *reconcileTrigger) ForceReconcile(ref domain.ClusterRef) { f.forced = append(f.forced, ref) }

func TestReconcileClusterNamespaceAndAudit(t *testing.T) {
	s, admin := namespaceServer(t)
	f := &reconcileTrigger{}
	s.Trigger = f
	for _, ns := range []string{"default", "dev", "dev"} {
		ref := domain.ClusterRef{Namespace: ns, Name: "prod"}
		before, err := s.Store.GetCluster(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		opsBefore, err := s.Store.ListOperations(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		w := call(s, "POST", "/api/v1/clusters/prod/reconcile?namespace="+ns, "", admin)
		if w.Code != 202 || w.Header().Get("Karakoram-Operation-Id") != "" {
			t.Fatal(w.Code, w.Header(), w.Body.String())
		}
		var body api.Cluster
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Name != "prod" || value(body.Namespace) != ns {
			t.Fatal(body, err)
		}
		after, err := s.Store.GetCluster(t.Context(), ref)
		if err != nil || after.Generation != before.Generation || after.Spec.RetryEpoch != before.Spec.RetryEpoch {
			t.Fatal(after, err)
		}
		opsAfter, err := s.Store.ListOperations(t.Context(), ref)
		if err != nil || len(opsAfter) != len(opsBefore) {
			t.Fatal("created operation", opsAfter, err)
		}
	}
	if len(f.forced) != 3 || f.ordinary != 0 || f.forced[0].Namespace != "default" || f.forced[1].Namespace != "dev" || f.forced[2].Namespace != "dev" {
		t.Fatal(f)
	}
	for ns, count := range map[string]int{"default": 1, "dev": 2} {
		events, err := s.Store.ListEvents(t.Context(), store.EventFilter{Namespace: ns, Cluster: "prod"})
		if err != nil {
			t.Fatal(err)
		}
		found := 0
		for _, e := range events {
			if e.Reason == "ReconcileClusterSucceeded" {
				found++
				if e.Actor == "" || e.Operation != "" || e.Namespace != ns {
					t.Fatal(e)
				}
			}
		}
		if found != count {
			t.Fatal(ns, "audit count", found)
		}
	}
	if w := call(s, "POST", "/api/v1/clusters/missing/reconcile?namespace=dev", "", admin); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "POST", "/api/v1/clusters/prod/reconcile?namespace=missing", "", admin); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(f.forced) != 3 {
		t.Fatal("unknown cluster triggered", f)
	}
}

func TestReconcileClusterRBAC(t *testing.T) {
	s, _ := namespaceServer(t)
	f := &reconcileTrigger{}
	s.Trigger = f
	for _, name := range []string{"viewer", "updater"} {
		if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: name}, "long-test-password"); err != nil {
			t.Fatal(err)
		}
	}
	policy := s.RBAC.Policy() + "\np, user:viewer, clusters, read, */prod, allow\np, user:updater, clusters, update, dev/prod, allow\n"
	if err := s.RBAC.ReplacePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, ns string
		status   int
	}{{"viewer", "dev", 403}, {"updater", "default", 403}, {"updater", "dev", 202}} {
		tok, _, err := s.Auth.CreateToken(t.Context(), tc.name, "test", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		w := call(s, "POST", "/api/v1/clusters/prod/reconcile?namespace="+tc.ns, "", tok)
		if w.Code != tc.status {
			t.Fatal(tc, w.Code, w.Body.String())
		}
	}
	if len(f.forced) != 1 || f.forced[0].Namespace != "dev" {
		t.Fatal(f)
	}
}

func TestReconcileClusterLifecycle(t *testing.T) {
	for _, phase := range []domain.Phase{domain.PhaseSuspended, domain.PhaseDeleting} {
		t.Run(string(phase), func(t *testing.T) {
			s, _, admin := testServer(t)
			f := &reconcileTrigger{}
			s.Trigger = f
			ref := domain.ClusterRef{Name: "prod"}
			c, err := s.Store.GetCluster(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			c.Status.Phase = phase
			if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			w := call(s, "POST", "/api/v1/clusters/prod/reconcile", "", admin)
			if phase == domain.PhaseDeleting {
				if w.Code != 409 || len(f.forced) != 0 {
					t.Fatal(w.Code, w.Body.String(), f)
				}
			} else {
				if w.Code != 202 || len(f.forced) != 1 {
					t.Fatal(w.Code, w.Body.String(), f)
				}
				c, err = s.Store.GetCluster(t.Context(), ref)
				if err != nil || c.Status.Phase != domain.PhaseSuspended {
					t.Fatal("resumed suspended cluster", c, err)
				}
			}
		})
	}
}
