// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
	"gopkg.in/yaml.v3"
)

var (
	_             agent.Engine = (*Engine)(nil)
	fileLimitOnce sync.Once
	stackVersion  = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
)

// Engine adapts the Elastic Stack runner to the shared node toolkit.
type Engine struct {
	runner        *Runner
	mu            sync.RWMutex
	spec          agentspec.Spec
	applied       bool
	configVersion int64
}

// NewEngine returns an Elastic Stack engine for agent registration.
func NewEngine() *Engine { return &Engine{runner: New()} }

// Name returns the provider identifier.
func (*Engine) Name() string { return agentspec.Engine }

// Processes lists the supported product processes.
func (e *Engine) Processes() []string { return e.runner.Processes() }

// Apply validates a provider payload, renders protected files and applies process intent.
func (e *Engine) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	if c.Engine != agentspec.Engine {
		return nil, errors.New("configuration engine does not match Elasticsearch")
	}
	var spec agentspec.Spec
	if len(c.EngineSpec) == 0 || json.Unmarshal(c.EngineSpec, &spec) != nil {
		return nil, errors.New("missing or invalid Elasticsearch engine spec")
	}
	if !stackVersion.MatchString(spec.Version) || spec.CAPEM == "" || spec.RestartToken == "" || spec.HeapMB < 0 {
		return nil, errors.New("invalid Elasticsearch engine spec")
	}
	switch spec.Component {
	case agentspec.ComponentElasticsearch:
		if spec.Elasticsearch == nil {
			return nil, errors.New("missing Elasticsearch component spec")
		}
	case agentspec.ComponentKibana:
		if spec.Kibana == nil {
			return nil, errors.New("missing Kibana component spec")
		}
	case agentspec.ComponentLogstash:
		if spec.Logstash == nil {
			return nil, errors.New("missing Logstash component spec")
		}
	case agentspec.ComponentAPM:
		if spec.APM == nil {
			return nil, errors.New("missing APM Server component spec")
		}
	default:
		return nil, errors.New("unknown Elastic component")
	}
	fileLimitOnce.Do(RaiseFileLimit)
	h := nodeHost{Node: n}
	changes, err := e.runner.Apply(ctx, h, spec, c.Cluster, c.Member)
	if err != nil {
		return changes, err
	}
	// Explicit empty intent stops children without releasing operator holds.
	states, _ := n.Snapshot()
	for name := range states {
		if name != spec.Component || h.Stopped(name) {
			if err := n.Action(ctx, name, "stop"); err != nil {
				return changes, err
			}
		}
	}
	e.mu.Lock()
	e.spec, e.applied, e.configVersion = spec, true, c.ConfigVersion
	e.mu.Unlock()
	return changes, nil
}

// Readiness dispatches the component and optional port to the local runner.
func (e *Engine) Readiness(ctx context.Context, opts agent.Options, args []string) error {
	var pipelines []agentspec.Pipeline
	if len(args) > 0 && args[0] == agentspec.ComponentLogstash {
		data, err := os.ReadFile(filepath.Join(opts.DataDir, "karakoram", "config", "pipelines.yml"))
		if err != nil {
			return errors.New("Logstash configured pipelines unavailable")
		}
		var entries []struct {
			ID string `yaml:"pipeline.id"`
		}
		if yaml.Unmarshal(data, &entries) != nil || len(entries) == 0 {
			return errors.New("Logstash configured pipelines invalid")
		}
		for _, entry := range entries {
			pipelines = append(pipelines, agentspec.Pipeline{ID: entry.ID})
		}
	}
	return e.runner.Readiness(ctx, args, pipelines...)
}

// Version reports the last successfully applied installed product version.
func (e *Engine) Version(ctx context.Context) string {
	e.mu.RLock()
	spec, applied := e.spec, e.applied
	e.mu.RUnlock()
	if !applied {
		return ""
	}
	return e.runner.Version(ctx, spec)
}

// Metrics translates local product metrics into the shared agent vocabulary.
func (e *Engine) Metrics(ctx context.Context, n *agent.Node) []agent.Metric {
	e.mu.RLock()
	spec, applied := e.spec, e.applied
	e.mu.RUnlock()
	if !applied {
		return nil
	}
	metrics := e.runner.Metrics(ctx, nodeHost{Node: n}, spec)
	result := make([]agent.Metric, len(metrics))
	for i, m := range metrics {
		result[i] = agent.Metric{Name: m.Name, Help: m.Help, Labels: m.Labels, Value: m.Value, Counter: m.Counter}
	}
	return result
}

// Routes exposes live application readiness over the authenticated agent transport.
func (e *Engine) Routes(mux *http.ServeMux, n *agent.Node) {
	mux.HandleFunc("POST /v1/elasticsearch/readiness", func(w http.ResponseWriter, r *http.Request) {
		e.mu.RLock()
		spec, applied, configVersion := e.spec, e.applied, e.configVersion
		e.mu.RUnlock()
		observation := agentspec.Readiness{Component: spec.Component, ConfigVersion: configVersion}
		states, maintenance := n.Snapshot()
		switch {
		case !applied:
			observation.Message = "Component configuration has not been applied"
		case maintenance || states[spec.Component].State != "running":
			observation.Message = "Component process is not running"
		default:
			var pipelines []agentspec.Pipeline
			if spec.Logstash != nil {
				pipelines = spec.Logstash.Pipelines
			}
			if err := e.runner.Readiness(r.Context(), []string{spec.Component}, pipelines...); err != nil {
				observation.Message = err.Error()
			} else {
				observation.Ready = true
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(observation)
	})
}

type nodeHost struct{ *agent.Node }

func (h nodeHost) Configure(name string, p Process) error {
	return h.Node.Configure(name, agent.ProcessSpec{Binary: p.Binary, Args: p.Args, Env: p.Env, Dir: p.Dir})
}
func (h nodeHost) Action(ctx context.Context, name, action string) error {
	if action == "stop" {
		states, _ := h.Snapshot()
		if _, configured := states[name]; !configured {
			return nil
		}
	}
	return h.Node.Action(ctx, name, action)
}
func (h nodeHost) Running(name string) bool {
	states, _ := h.Snapshot()
	return states[name].State == "running"
}
func (h nodeHost) Stopped(name string) bool {
	return h.Node.Stopped(name) || (h.Config().DesiredProcesses != nil && !slices.Contains(h.Config().DesiredProcesses, name))
}
