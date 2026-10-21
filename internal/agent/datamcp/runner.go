package datamcp

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
)

type endpoint struct {
	Port    int    `json:"port"`
	Token   string `json:"token"`
	Path    string `json:"path"`
	handler http.Handler
}
type state struct {
	Version       string              `json:"version"`
	ConfigVersion int64               `json:"configVersion"`
	Processes     map[string]endpoint `json:"processes"`
}

// Runner's zero value is ready for registration with the agent.
type Runner struct {
	mu      sync.RWMutex
	state   state
	digest  [32]byte
	applied bool
}

var _ agent.Engine = (*Runner)(nil)

func New() *Runner           { return &Runner{} }
func (*Runner) Name() string { return companionspec.Engine }
func (*Runner) Processes() []string {
	return []string{companionspec.ProcessRead, companionspec.ProcessReadWrite}
}
func (r *Runner) Version(context.Context) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state.Version
}
func (*Runner) Metrics(context.Context, *agent.Node) []agent.Metric { return nil }

func (r *Runner) Apply(ctx context.Context, n *agent.Node, c agentapi.MemberConfig) ([]string, error) {
	var s companionspec.Spec
	if c.Engine != companionspec.Engine || json.Unmarshal(c.EngineSpec, &s) != nil {
		return nil, errors.New("invalid data MCP payload")
	}
	if err := validate(s); err != nil {
		return nil, err
	}
	digest := sha256.Sum256(c.EngineSpec)
	r.mu.RLock()
	changed := !r.applied || r.digest != digest
	r.mu.RUnlock()
	next := state{Version: s.Version, ConfigVersion: c.ConfigVersion, Processes: map[string]endpoint{}}
	specs := map[string]agent.ProcessSpec{}
	files := map[string][]byte{}
	for _, p := range s.Processes {
		name := processName(p.Level)
		token, err := localSecret(n, name)
		if err != nil {
			return nil, err
		}
		spec, rendered, err := Render(s, p, n.DataDir(), token)
		if err != nil {
			return nil, err
		}
		specs[name] = spec
		for path, data := range rendered {
			files[path] = data
		}
		path := "/mcp"
		if s.Upstream == companionspec.UpstreamPostgres {
			path = "/mcp/v1"
		}
		e := endpoint{Port: p.Port, Token: token, Path: path}
		segment := "ro"
		if p.Level == "readWrite" {
			segment = "rw"
		}
		e.handler = proxy(e, "/v1/datamcp/"+segment)
		next.Processes[name] = e
	}
	states, _ := n.Snapshot()
	// Stop old children before replacing credentials or reusing their ports.
	if changed {
		for name := range states {
			if err := n.Action(ctx, name, "stop"); err != nil {
				return nil, err
			}
		}
	}
	for path, data := range files {
		if err := n.WriteFile(path, data, 0600); err != nil {
			return nil, err
		}
	}
	raw, _ := json.Marshal(next)
	if err := n.WriteFile("datamcp/readiness.json", raw, 0600); err != nil {
		return nil, err
	}
	var restarted []string
	for _, name := range r.Processes() {
		spec, present := specs[name]
		if !present {
			if _, configured := states[name]; !configured {
				continue
			}
			if err := n.Action(ctx, name, "stop"); err != nil {
				return nil, err
			}
			continue
		}
		if err := n.Configure(name, spec); err != nil {
			return nil, err
		}
		if err := n.Action(ctx, name, "start"); err != nil {
			return nil, err
		}
		if changed {
			restarted = append(restarted, name)
		}
	}
	r.mu.Lock()
	r.state = next
	r.digest = digest
	r.applied = true
	r.mu.Unlock()
	return restarted, nil
}

// BeforeStart also guards explicit operator starts after a level is removed.
func (*Runner) BeforeStart(_ context.Context, n *agent.Node, name string) error {
	var s companionspec.Spec
	if err := json.Unmarshal(n.Config().EngineSpec, &s); err != nil {
		return errors.New("invalid data MCP payload")
	}
	for _, p := range s.Processes {
		if processName(p.Level) == name {
			return nil
		}
	}
	return errors.New("data MCP process is absent from the configuration")
}

func localSecret(n *agent.Node, name string) (string, error) {
	// OpenRoot prevents an on-disk link from reading credentials outside the
	// protected agent directory. Atomic writes below use the same root boundary.
	root, err := os.OpenRoot(filepath.Join(n.DataDir(), "karakoram"))
	if err == nil {
		b, e := root.ReadFile(filepath.Join("datamcp", name, "local-secret"))
		root.Close()
		if e == nil {
			if len(b) < 26 {
				return "", errors.New("invalid local upstream secret")
			}
			return string(b), nil
		}
		if !os.IsNotExist(e) {
			return "", e
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}
	token := rand.Text()
	if err := n.WriteFile(filepath.Join("datamcp", name, "local-secret"), []byte(token), 0600); err != nil {
		return "", err
	}
	return token, nil
}
func (*Runner) Readiness(ctx context.Context, o agent.Options, args []string) error {
	if len(args) != 0 {
		return errors.New("data MCP readiness accepts no arguments")
	}
	root, err := os.OpenRoot(filepath.Join(o.DataDir, "karakoram"))
	if err != nil {
		return err
	}
	defer root.Close()
	raw, err := root.ReadFile("datamcp/readiness.json")
	if err != nil {
		return err
	}
	var s state
	if err = json.Unmarshal(raw, &s); err != nil {
		return err
	}
	if len(s.Processes) == 0 {
		return errors.New("no data MCP processes configured")
	}
	for name, e := range s.Processes {
		if !healthy(ctx, e) {
			return fmt.Errorf("data MCP process %s is not ready", name)
		}
	}
	return nil
}
