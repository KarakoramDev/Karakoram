// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type tombstoneCalls struct {
	mu      sync.Mutex
	entries []string
}

func (c *tombstoneCalls) add(entry string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, entry)
}

func (c *tombstoneCalls) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.entries...)
}

type testTombstoneWriter struct {
	calls              *tombstoneCalls
	preflight, install error
	onInstall          func()
}

func (w testTombstoneWriter) Kind() string { return "hosts-helper" }
func (w testTombstoneWriter) Preflight(context.Context) error {
	w.calls.add("preflight")
	return w.preflight
}
func (w testTombstoneWriter) Install(_ context.Context, hosts []string) error {
	w.calls.add("install")
	if w.onInstall != nil {
		w.onInstall()
	}
	if !reflect.DeepEqual(hosts, []string{"retired"}) {
		return errors.New("incomplete set")
	}
	return w.install
}

func TestResolutionTombstoneApplication(t *testing.T) {
	for _, stage := range []string{"success", "preflight", "install", "verify", "unsupported"} {
		t.Run(stage, func(t *testing.T) {
			var calls tombstoneCalls
			writer := testTombstoneWriter{calls: &calls}
			fail := errors.New("blocked")
			if stage == "preflight" {
				writer.preflight = fail
			}
			if stage == "install" {
				writer.install = fail
			}
			verify := func(context.Context, []string) error {
				calls.add("verify")
				if stage == "verify" {
					return fail
				}
				return nil
			}
			status := installResolutionTombstones(t.Context(), writer, []string{"retired"}, verify)
			if stage == "unsupported" {
				status = installResolutionTombstones(t.Context(), nil, []string{"retired"}, verify)
			}
			if status.Supported != (stage != "unsupported") {
				t.Fatal(status)
			}
			if status.VerifiedAt.IsZero() != (stage != "success") {
				t.Fatal(status)
			}
			if stage == "success" && !reflect.DeepEqual(calls.snapshot(), []string{"preflight", "install", "verify"}) {
				t.Fatal(calls.snapshot())
			}
			if stage != "success" && len(status.Hosts) > 0 {
				t.Fatal("failed proof advertised hosts", status)
			}
			a := &nodeAgent{resolutionTombstones: &status}
			var reported agentapi.Status
			a.attachResolutionTombstones(&reported)
			if !reflect.DeepEqual(reported.ResolutionTombstones, &status) {
				t.Fatal(reported)
			}
		})
	}
}

func TestResolutionTombstoneOnlySkipsNativeApply(t *testing.T) {
	a, c, e := fakeNode(t, "/bin/true")
	c.ConfigVersion = 1
	a.c = c
	e.onApply = func(*Node, agentapi.MemberConfig) error { return errors.New("native renderer invoked") }
	next := c
	next.ConfigVersion++
	next.ResolutionTombstones = []string{"retired"}
	if _, err := a.applyEngineWithTombstones(t.Context(), next); err != nil {
		t.Fatal(err)
	}
	next.EngineSpec = []byte(`{"changed":true}`)
	if _, err := a.applyEngineWithTombstones(t.Context(), next); err == nil {
		t.Fatal("native change skipped")
	}
	a.c = agentapi.MemberConfig{}
	if _, err := a.applyEngineWithTombstones(t.Context(), c); err == nil {
		t.Fatal("startup must reconstruct supervision")
	}
}
