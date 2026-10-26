// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func TestKibanaHeapFailureQualificationAndBounds(t *testing.T) {
	now := time.Now()
	process := agentapi.ProcessStatus{State: "failed", SupervisorSession: "session", Launch: 2}
	exitCode := 1
	base := agentapi.LogLine{Timestamp: now, Process: "kibana", Stream: "stderr", SupervisorSession: "session", Launch: 2, ExitCode: &exitCode}
	for _, marker := range []string{"JavaScript heap out of memory", "Ineffective mark-compacts near heap limit", "Reached heap limit"} {
		line := base
		line.Message = "FATAL ERROR: " + marker
		for _, state := range []string{"failed", "running"} {
			p := process
			p.State = state
			got := kibanaHeapFailure([]agentapi.LogLine{line}, p, now.Add(-time.Second), 614)
			if (got != nil) != (state != "running") {
				t.Fatalf("state %s marker %s: %+v", state, marker, got)
			}
			p.Launch++
			if got := kibanaHeapFailure([]agentapi.LogLine{line}, p, now.Add(-time.Second), 614); got == nil || got.Launch != 2 || got.OldSpaceMiB != 614 {
				t.Fatalf("retry lost previous exit: %+v", got)
			}
		}
	}
	for _, change := range []func(*agentapi.LogLine){
		func(l *agentapi.LogLine) { l.Message = "OOM allocation failed GC near limit" },
		func(l *agentapi.LogLine) { l.Message = `warning: "JavaScript heap out of memory"` },
		func(l *agentapi.LogLine) { l.Message = `quoted: "FATAL ERROR: Reached heap limit"` },
		func(l *agentapi.LogLine) { l.Message = "ordinary JavaScript heap out of memory" },
		func(l *agentapi.LogLine) { code := 0; l.ExitCode = &code },
		func(l *agentapi.LogLine) { l.ExitCode = nil },
		func(l *agentapi.LogLine) { l.Stream = "stdout" },
		func(l *agentapi.LogLine) { l.Process = "logstash" },
		func(l *agentapi.LogLine) { l.SupervisorSession = "old" },
		func(l *agentapi.LogLine) { l.Launch = 3 },
		func(l *agentapi.LogLine) { l.Launch = 0 },
		func(l *agentapi.LogLine) { l.Timestamp = now.Add(-time.Hour) },
	} {
		line := base
		line.Message = "FATAL ERROR: JavaScript heap out of memory"
		change(&line)
		if kibanaHeapFailure([]agentapi.LogLine{line}, process, now.Add(-time.Second), 614) != nil {
			t.Fatalf("unqualified line: %+v", line)
		}
	}
	line := base
	line.Message = "FATAL ERROR: Reached heap limit"
	if kibanaHeapFailure([]agentapi.LogLine{line}, process, now.Add(time.Second), 614) != nil {
		t.Fatal("healthy/config cutoff did not expire failure")
	}
	if kibanaHeapFailure([]agentapi.LogLine{line}, process, now.Add(-time.Second), 0) != nil {
		t.Fatal("invented rendered cap")
	}
	clean := process
	clean.State = "stopped"
	cleanCode := 0
	clean.ExitCode = &cleanCode
	line.ExitCode = &cleanCode
	if kibanaHeapFailure([]agentapi.LogLine{line}, clean, now.Add(-time.Second), 614) != nil {
		t.Fatal("clean stop was classified as heap exhaustion")
	}
	line.ExitCode = &exitCode
	lines := []agentapi.LogLine{line}
	for range heapTailLines {
		lines = append(lines, agentapi.LogLine{})
	}
	if kibanaHeapFailure(lines, process, now.Add(-time.Second), 614) != nil {
		t.Fatal("scan exceeded line bound")
	}
	if kibanaHeapFailure([]agentapi.LogLine{line, {Message: strings.Repeat("x", heapTailBytes)}}, process, now.Add(-time.Second), 614) != nil {
		t.Fatal("scan exceeded byte bound")
	}
}

func TestRenderedOldSpaceMiBBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.options")
	for _, tc := range []struct {
		text string
		want int
	}{{"--max-old-space-size=614\n", 614}, {"--max-old-space-size=921\n", 921}, {"--max-old-space-size=-1", 0}, {"--max-old-space-size=614\nsecret", 0}, {"--max-old-space-size=" + strings.Repeat("9", 100), 0}, {strings.Repeat("x", 1025), 0}} {
		if err := os.WriteFile(path, []byte(tc.text), 0600); err != nil {
			t.Fatal(err)
		}
		if got := renderedOldSpaceMiB(path); got != tc.want {
			t.Fatalf("cap=%d want %d", got, tc.want)
		}
	}
}

func TestKibanaExitedChildReadinessCarriesHeapFailure(t *testing.T) {
	r, _ := fixture(t)
	e := NewEngine()
	e.runner = r
	spec := esSpec()
	spec.Component = agentspec.ComponentKibana
	spec.HeapMB = 614
	spec.Kibana = &agentspec.KibanaSpec{ESHosts: []string{"https://testn0:9200"}, ServiceAccountToken: "token", SecurityKey: "security", SavedObjectsKey: "saved", ReportingKey: "reporting"}
	binary := filepath.Join(r.home(spec), "bin", spec.Component)
	if err := os.MkdirAll(filepath.Dir(binary), 0700); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nif [ ! -f '" + binary + ".started' ]; then\n  touch '" + binary + ".started'\n  while [ ! -f '" + binary + ".release' ]; do sleep 0.01; done\n  echo 'FATAL ERROR: Reached heap limit Allocation failed - JavaScript heap out of memory private-value' >&2\n  exit 1\nfi\nexec sleep 60\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	result := runAdapter(t, e, t.TempDir(), spec, []string{spec.Component})
	if result.err != nil {
		t.Fatal(result.err)
	}
	states, _ := result.node.Snapshot()
	first := states[spec.Component]
	if err := os.WriteFile(binary+".release", nil, 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		states, _ := result.node.Snapshot()
		process := states[spec.Component]
		// Reaping is durable evidence even if scheduling skips the brief backoff.
		if process.SupervisorSession == first.SupervisorSession &&
			(process.Launch > first.Launch || process.Launch == first.Launch && process.ExitCode != nil) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first child was not reaped")
		}
		time.Sleep(5 * time.Millisecond)
	}
	mux := http.NewServeMux()
	e.Routes(mux, result.node)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/elasticsearch/readiness", nil))
	var got agentspec.Readiness
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Ready || got.HeapExhaustion == nil || got.HeapExhaustion.OldSpaceMiB != 614 || got.MemoryPressure || !strings.Contains(got.Message, "minRam and maxRam together") || !strings.Contains(got.Message, "1.5 GiB") || strings.Contains(w.Body.String(), "private-value") {
		t.Fatalf("incorrect reaped-child diagnostic during backoff or retry: %+v", got)
	}
	for {
		states, _ := result.node.Snapshot()
		if states[spec.Component].State == "running" && states[spec.Component].SupervisorSession == first.SupervisorSession && states[spec.Component].Launch > first.Launch {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not retry")
		}
		time.Sleep(5 * time.Millisecond)
	}
	probe := func() agentspec.Readiness {
		t.Helper()
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/elasticsearch/readiness", nil))
		var observation agentspec.Readiness
		if err := json.Unmarshal(w.Body.Bytes(), &observation); err != nil {
			t.Fatal(err)
		}
		return observation
	}
	if observation := probe(); observation.Ready || observation.HeapExhaustion == nil {
		t.Fatalf("running retry lost failed launch: %+v", observation)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:5601")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	if observation := probe(); !observation.Ready || observation.HeapExhaustion != nil {
		server.Close()
		t.Fatalf("healthy native readiness did not clear failure: %+v", observation)
	}
	server.Close()
	if observation := probe(); observation.Ready || observation.HeapExhaustion != nil {
		t.Fatalf("old failure resurfaced after healthy readiness: %+v", observation)
	}
	// Reapply a different rendered cap and restart token without a new failure.
	c := result.node.Config()
	spec.RestartToken += "-resized"
	spec.HeapMB = 921
	c.EngineSpec, err = json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	c.ConfigVersion++
	if _, err := e.Apply(t.Context(), result.node, c); err != nil {
		t.Fatal(err)
	}
	if observation := probe(); observation.HeapExhaustion != nil || observation.ConfigVersion != c.ConfigVersion {
		t.Fatalf("old launch survived configuration change: %+v", observation)
	}
}
