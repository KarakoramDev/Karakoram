// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

func TestLogstashReadinessRequiresEveryConfiguredPipeline(t *testing.T) {
	for _, tc := range []struct {
		name, loaded, stats, message string
		pipelines                    []agentspec.Pipeline
	}{
		{name: "default", loaded: `{"pipelines":{"main":{}}}`, stats: `{"pipelines":{"main":{"events":{"in":0,"out":0}}}}`},
		{name: "all running without traffic", loaded: `{"pipelines":{"logs":{},"audit":{}}}`, stats: `{"pipelines":{"logs":{"events":{}},"audit":{"events":{}}}}`, pipelines: []agentspec.Pipeline{{ID: "logs"}, {ID: "audit"}}},
		{name: "compile failure", loaded: `{"pipelines":{"logs":{}}}`, stats: `{"pipelines":{"logs":{"events":{}}}}`, pipelines: []agentspec.Pipeline{{ID: "logs"}, {ID: "broken"}}, message: `pipeline "broken" is not loaded`},
		{name: "startup failure", loaded: `{"pipelines":{"logs":{},"broken":{}}}`, stats: `{"pipelines":{"logs":{"events":{}}}}`, pipelines: []agentspec.Pipeline{{ID: "logs"}, {ID: "broken"}}, message: `pipeline "broken" is not running`},
		{name: "missing running stats", loaded: `{"pipelines":{"main":{}}}`, stats: `{"pipelines":{"main":{}}}`, message: `pipeline "main" is not running`},
		{name: "no pipelines", loaded: `{"pipelines":{}}`, stats: `{"pipelines":{}}`, message: `pipeline "main" is not loaded`},
		{name: "reload error", loaded: `{"pipelines":{}}`, stats: `{"pipelines":{"main":{"reloads":{"last_error":{"type":"LogStash::ConfigurationError","message":"Expected token at line 1, column 8; secret pipeline source"}}}}}`, message: `configuration failed: LogStash::ConfigurationError at line 1, column 8`},
		{name: "reload error without inventory", loaded: `not JSON`, stats: `{"pipelines":{"main":{"reloads":{"last_error":{"type":"LogStash::ConfigurationError"}}}}}`, message: `configuration failed: LogStash::ConfigurationError`},
		{name: "repaired reload", loaded: `{"pipelines":{"main":{}}}`, stats: `{"pipelines":{"main":{"events":{},"reloads":{"last_error":{"type":"LogStash::ConfigurationError"},"last_failure_timestamp":"2026-01-01T00:00:00Z","last_success_timestamp":"2026-01-01T00:01:00Z"}}}}`},
		{name: "invalid API response", loaded: `not JSON`, stats: `{}`, message: "pipeline inventory unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			paths := map[string]bool{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				paths[r.URL.Path] = true
				switch r.URL.Path {
				case "/_node/pipelines":
					_, _ = io.WriteString(w, tc.loaded)
				case "/_node/stats/pipelines":
					_, _ = io.WriteString(w, tc.stats)
				default:
					t.Errorf("unexpected probe %s", r.URL.Path)
				}
			}))
			defer server.Close()
			port := strconv.Itoa(server.Listener.Addr().(*net.TCPAddr).Port)
			err := New().Readiness(t.Context(), []string{"logstash", port}, tc.pipelines...)
			if tc.message == "" {
				if err != nil || !paths["/_node/pipelines"] || !paths["/_node/stats/pipelines"] {
					t.Fatalf("healthy pipelines: %v, probes %v", err, paths)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("want %q, got %v", tc.message, err)
			}
		})
	}
}

func TestLogstashReadinessLoadsConfiguredPipelineIDs(t *testing.T) {
	dir := t.TempDir()
	if _, err := atomicWrite(filepath.Join(dir, "karakoram", "config", "pipelines.yml"), []byte("- pipeline.id: broken\n  path.config: /unused\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"pipelines":{"main":{"events":{}}}}`)
	}))
	defer server.Close()
	port := strconv.Itoa(server.Listener.Addr().(*net.TCPAddr).Port)
	err := NewEngine().Readiness(t.Context(), agent.Options{DataDir: dir}, []string{"logstash", port})
	if err == nil || !strings.Contains(err.Error(), `pipeline "broken" is not loaded`) {
		t.Fatalf("configured pipeline was ignored: %v", err)
	}
}

func TestLogstashStartupErrorIsCurrentAndDoesNotExposePipelineSource(t *testing.T) {
	at := time.Now()
	pipelines := []agentspec.Pipeline{{ID: "broken"}}
	lines := []agentapi.LogLine{{Timestamp: at, Message: `[ERROR][logstash.javapipeline][broken] LogStash::ConfigurationError: Expected input at line 1, column 8, after password=secret`}}
	message := logstashStartupError(lines, at, pipelines)
	if !strings.Contains(message, `pipeline "broken" configuration failed: LogStash::ConfigurationError at line 1, column 8`) || strings.Contains(message, "secret") {
		t.Fatalf("unsafe or missing startup diagnostic: %s", message)
	}
	if message := logstashStartupError(lines, at.Add(time.Second), pipelines); message != "" {
		t.Fatal("stale configuration failure survived a new apply", message)
	}
	if message := pipelineConfigurationError("request failed password=secret", "broken"); message != "" {
		t.Fatal("unclassified error escaped", message)
	}
}
