// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent/tombstone"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

// nomadReleaseAgent models a config-pull worker, not a status-triggered writer.
// Install is the real Nomad writer: it cannot acknowledge until the watched
// Variable has been rendered to this allocation's temporary hosts file.
type nomadReleaseAgent struct {
	mu        sync.Mutex
	writer    tombstone.Writer
	desired   []string
	version   int64
	sequence  int
	running   bool
	installs  int
	completed int
	status    agentapi.Status
	wg        sync.WaitGroup
}

func (a *nomadReleaseAgent) apply(ctx context.Context, cfg agentapi.MemberConfig) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sequence != 0 && a.version == cfg.ConfigVersion && slices.Equal(a.desired, cfg.ResolutionTombstones) {
		return
	}
	a.desired = slices.Clone(cfg.ResolutionTombstones)
	a.version = cfg.ConfigVersion
	a.sequence++
	a.status = agentapi.Status{ConfigVersion: a.status.ConfigVersion, ResolutionTombstones: &agentapi.ResolutionTombstoneStatus{
		Supported: true, Hosts: slices.Clone(a.desired), Reason: "waiting for Nomad hosts projection",
		Generation: "nomad-release/launch-1", ProcessGeneration: agentapi.ResolutionProcessGeneration(agentapi.ProcessStatus{}),
	}}
	if a.running {
		return
	}
	a.running = true
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			a.mu.Lock()
			hosts, sequence := slices.Clone(a.desired), a.sequence
			a.installs++
			a.mu.Unlock()
			err := a.writer.Install(ctx, hosts)
			a.mu.Lock()
			if sequence != a.sequence {
				a.mu.Unlock()
				continue
			}
			if err != nil {
				a.status.ResolutionTombstones.Reason = err.Error()
			} else {
				a.status.ResolutionTombstones = resolutionTestProof(hosts, time.Now().UTC(), "nomad-release/launch-1")
				a.status.ConfigVersion = a.version
				a.completed++
			}
			a.running = false
			a.mu.Unlock()
			return
		}
	}()
}

func (a *nomadReleaseAgent) snapshot() agentapi.Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.status
	if s.ResolutionTombstones != nil {
		proof := *s.ResolutionTombstones
		proof.Hosts = slices.Clone(proof.Hosts)
		s.ResolutionTombstones = &proof
	}
	return s
}

// This watcher consumes only actual Items accepted by the fixture's Nomad API.
// Pausing delivery lets the test distinguish projection from writer completion.
type nomadReleaseRenderer struct {
	mu     sync.Mutex
	paused bool
	err    error
	bridge *realNomadResolutionBackend
	paths  map[string]string
}

func (r *nomadReleaseRenderer) render() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.paused || r.err != nil {
		return
	}
	r.bridge.variableMu.Lock()
	defer r.bridge.variableMu.Unlock()
	for name, path := range r.paths {
		hosts := []string{}
		for key, host := range r.bridge.variables[name] {
			if tombstone.ValidHost(host) && key == "karakoram_tombstone_"+host {
				hosts = append(hosts, host)
			}
		}
		sort.Strings(hosts)
		var contents strings.Builder
		contents.WriteString("127.0.0.1 localhost\n# karakoram resolution tombstones: enabled\n")
		for _, host := range hosts {
			fmt.Fprintf(&contents, "%s %s\n%s %s\n", tombstone.SentinelIPv4, host, tombstone.SentinelIPv6, host)
		}
		current, err := os.ReadFile(path)
		if err != nil {
			r.err = err
			return
		}
		if string(current) == contents.String() {
			continue
		}
		// Rename, rather than truncate, matches replacement of a watched template.
		file, err := os.CreateTemp(filepath.Dir(path), "hosts-render-")
		if err != nil {
			r.err = err
			return
		}
		_, writeErr := file.WriteString(contents.String())
		closeErr := file.Close()
		if writeErr != nil {
			r.err = writeErr
		} else if closeErr != nil {
			r.err = closeErr
		} else {
			r.err = os.Rename(file.Name(), path)
		}
		if r.err != nil {
			return
		}
	}
}

func TestResolutionTombstoneRealNomadAsyncReleaseOrdering(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	deadline := at.Add(2 * time.Second)
	rc.Clock = agentRolloutClock{at: at}
	// Resume an established survivor cohort near the end of its original hold.
	// Reinstalling the same launch must not restart or shorten that hold.
	generations := make(map[string]string, len(s.data()))
	for _, m := range s.data() {
		generations[m.Name] = "nomad-release/launch-1"
	}
	s.ResolutionTombstones = []resolutionTombstone{{Host: "retired", Epoch: "nomad-release", CreatedAt: at.Add(-time.Hour), ReleaseNotBefore: deadline, Generations: generations}}
	bridge := realNomadResolutionFixture(t, b)
	rc.Backend = bridge
	ctx, cancel := context.WithCancel(t.Context())
	agents := map[string]*nomadReleaseAgent{}
	renderer := &nomadReleaseRenderer{bridge: bridge, paths: map[string]string{}, paused: true}
	for _, m := range s.data() {
		path := filepath.Join(t.TempDir(), "hosts")
		if err := os.WriteFile(path, []byte("127.0.0.1 localhost\n# karakoram resolution tombstones: enabled\n"), 0600); err != nil {
			t.Fatal(err)
		}
		renderer.paths[m.Name] = path
		agents[m.Name] = &nomadReleaseAgent{writer: tombstone.NewNomadVariableWriter(path)}
	}
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renderer.render()
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-watchDone
		for _, a := range agents {
			a.wg.Wait()
		}
	})
	unreachable := ""
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/status" {
			return false
		}
		if name == unreachable {
			http.Error(w, "agent unavailable", http.StatusServiceUnavailable)
			return true
		}
		if a := agents[name]; a != nil {
			json.NewEncoder(w).Encode(a.snapshot())
			return true
		}
		return false
	}
	checkpoint := func() {
		t.Helper()
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		var err error
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
	}
	applyDesired := func(empty bool) {
		t.Helper()
		for name, a := range agents {
			cfg, err := p.MemberConfig(t.Context(), rc, name)
			if err != nil {
				t.Fatal(err)
			}
			if empty && len(cfg.ResolutionTombstones) != 0 || !empty && !slices.Equal(cfg.ResolutionTombstones, []string{"retired"}) {
				t.Fatalf("unexpected desired set for %s: %v", name, cfg.ResolutionTombstones)
			}
			a.apply(ctx, cfg)
		}
	}
	assertProjected := func(empty bool) {
		t.Helper()
		bridge.variableMu.Lock()
		defer bridge.variableMu.Unlock()
		for name := range agents {
			if (bridge.variables[name]["karakoram_tombstone_retired"] == "retired") == empty {
				t.Fatalf("wrong Nomad projection for %s: %v", name, bridge.variables[name])
			}
		}
	}
	assertPending := func() {
		t.Helper()
		if len(s.ResolutionTombstones) != 1 || !s.ResolutionTombstones[0].ReleasePending || !s.ResolutionTombstones[0].ReleaseNotBefore.Equal(deadline) {
			t.Fatalf("release checkpoint lost or deadline changed: %+v", s.ResolutionTombstones)
		}
	}
	assertResponsivePending := func() {
		t.Helper()
		for name := range agents {
			client, err := rc.Agents.Dial(t.Context(), rc.Cluster.Ref(), name)
			if err != nil {
				t.Fatal(err)
			}
			requestCtx, stop := context.WithTimeout(t.Context(), 500*time.Millisecond)
			start := time.Now()
			status, err := client.Status(requestCtx)
			stop()
			if err != nil || time.Since(start) >= 500*time.Millisecond {
				t.Fatalf("pending status blocked on Nomad writer for %s: %v", name, err)
			}
			a := agents[name]
			a.mu.Lock()
			desiredVersion := a.version
			a.mu.Unlock()
			if status.ConfigVersion >= desiredVersion {
				t.Fatalf("pending writer prematurely applied config for %s: applied=%d desired=%d", name, status.ConfigVersion, desiredVersion)
			}
			proof := status.ResolutionTombstones
			if proof == nil || !proof.Supported || proof.Reason == "" || proof.SetHash != "" || !proof.VerifiedAt.IsZero() {
				t.Fatalf("status manufactured a pending proof for %s: %+v", name, proof)
			}
		}
	}
	awaitProof := func(hosts []string) {
		t.Helper()
		end := time.Now().Add(3 * time.Second)
		for {
			renderer.mu.Lock()
			err := renderer.err
			renderer.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			complete := true
			for _, a := range agents {
				complete = complete && currentResolutionProof(a.snapshot(), hosts, now(rc))
			}
			if complete {
				return
			}
			if time.Now().After(end) {
				t.Fatal("real Nomad writer did not observe rendered Variable within bound")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	checkpoint()
	applyDesired(false)
	assertResponsivePending()
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	assertProjected(false)
	if s.ResolutionTombstones[0].ReleasePending {
		t.Fatal("release preceded installation or original hold deadline")
	}
	renderer.mu.Lock()
	renderer.paused = false
	renderer.mu.Unlock()
	awaitProof([]string{"retired"})
	// Qualify the installed cohort before reaching the original hold deadline.
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.ResolutionTombstones[0].Generations) != len(agents) || s.ResolutionTombstones[0].ReleasePending {
		t.Fatal("installation did not qualify the full cohort before release")
	}
	rc.Clock = agentRolloutClock{at: deadline}
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	assertPending()
	checkpoint()
	assertPending()
	assertProjected(false)
	renderer.mu.Lock()
	renderer.paused = true
	renderer.mu.Unlock()
	applyDesired(true)
	// Repeated pulls coalesce; neither pulls nor status may invent verification.
	applyDesired(true)
	assertResponsivePending()
	assertProjected(false)

	// An actual status transport failure must defer projection, not lose intent.
	b.mu.Lock()
	unreachable = s.data()[0].Name
	b.mu.Unlock()
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	checkpoint()
	assertPending()
	assertProjected(false)
	b.mu.Lock()
	unreachable = ""
	b.mu.Unlock()
	// A successful pending snapshot, unlike a transport error, must let the
	// control plane project the reduced keys while Install is still waiting.
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	assertProjected(true)
	assertPending()
	assertResponsivePending()
	checkpoint()
	assertPending()
	renderer.mu.Lock()
	renderer.paused = false
	renderer.mu.Unlock()
	awaitProof(nil)
	if err := reconcileResolutionTombstones(freshMySQLReads(t.Context()), rc, s); err != nil {
		t.Fatal(err)
	}
	checkpoint()
	if len(s.ResolutionTombstones) != 0 {
		t.Fatalf("verified reduced projection did not clear durable release journal: %+v", s.ResolutionTombstones)
	}
	for name, a := range agents {
		a.mu.Lock()
		installs, completed := a.installs, a.completed
		a.mu.Unlock()
		if installs != 2 || completed != 2 {
			t.Fatalf("%s did not singleflight exactly install then release: installs=%d completed=%d", name, installs, completed)
		}
	}
}
