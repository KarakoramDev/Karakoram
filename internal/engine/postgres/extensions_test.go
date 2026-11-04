// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestAvailableExtensionValidation(t *testing.T) {
	p := &Core{}
	available, err := p.availableExtensions(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateExtensions([]string{"pgcrypto", "uuid-ossp"}, available); err != nil {
		t.Fatal(err)
	}
	for _, names := range [][]string{{"vector"}, {"pgcrypto; SELECT 1"}, make([]string, 33)} {
		if err := validateExtensions(names, available); err == nil {
			t.Fatalf("accepted %q", names)
		}
	}
	p.options.AvailableExtensions = func(context.Context, *engine.Context) ([]string, error) { return []string{"vector"}, nil }
	available, err = p.availableExtensions(t.Context(), nil)
	if err != nil || validateExtensions([]string{"vector"}, available) != nil {
		t.Fatal("hook ignored", err)
	}
	sentinel := errors.New("profile unavailable")
	p.options.AvailableExtensions = func(context.Context, *engine.Context) ([]string, error) { return nil, sentinel }
	if err := p.CreateDatabase(t.Context(), nil, Database{Name: "app", Owner: "app", AllowConnections: true, Extensions: []string{"vector"}}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	p.options.AvailableExtensions = nil
	if err := p.CreateDatabase(t.Context(), nil, Database{Name: "app", Owner: "app", Extensions: []string{"pgcrypto"}}); err == nil {
		t.Fatal("accepted extensions on disabled database")
	}
	if err := p.updateProfileExtensions(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeExtensionFailureRetainsCompletion(t *testing.T) {
	for _, kind := range []string{"minor", "major"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			s := converge(t, p, rc)
			if kind == "minor" {
				s.ExtensionsPending = true
			} else {
				s.Upgrade = &upgradeState{From: "16.15.1", To: rc.Cluster.Spec.Version, Stage: "finalize"}
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			sentinel := errors.New("extension availability failed")
			p.options.ProfileExtensions = func(*engine.Context) []string { return []string{"pgcrypto"} }
			p.options.AvailableExtensions = func(context.Context, *engine.Context) ([]string, error) { return nil, sentinel }
			if _, err := p.Sync(t.Context(), rc); !errors.Is(err, sentinel) {
				t.Fatalf("failure not surfaced: %v", err)
			}
			s, _ = load(rc.Cluster)
			if kind == "minor" && !s.ExtensionsPending || kind == "major" && s.Upgrade == nil {
				t.Fatal("failed update lost its completion checkpoint")
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase == domain.PhaseReady {
				t.Fatal("failed extension update reported ready", status)
			}
			readyFalse, extensionPending := false, false
			for _, condition := range status.Conditions {
				if condition.Type == "Ready" {
					readyFalse = condition.Status == "False"
				}
				if condition.Type == "ExtensionsUpdated" && condition.Reason == "ExtensionUpdatePending" {
					extensionPending = condition.Status == "False"
				}
			}
			if !readyFalse || kind == "minor" && !extensionPending {
				t.Fatal("failed update missing readiness/pending condition", status.Conditions)
			}
			p.options.ProfileExtensions = func(*engine.Context) []string { return nil }
			if _, err := New(p.options).Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			if s.ExtensionsPending || s.Upgrade != nil {
				t.Fatal("successful retry did not release completion")
			}
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady {
				t.Fatal("successful retry did not restore readiness", status, err)
			}
		})
	}
}
