// Package companion manages the stateless data MCP member independently of provider state.
package companion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/datamcp/companionspec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const stateKey = "datamcp/state"
const configKey = "datamcp/config"

// State is sealed separately from Cluster.Internal: provider serializers may
// replace their entire internal document and must never erase import receipts.
type state struct {
	ID          string             `json:"id,omitempty"`
	Plan        *engine.ImportPlan `json:"plan,omitempty"`
	Pending     *infra.Op          `json:"pending,omitempty"`
	Stage       string             `json:"stage"`
	RuntimeHash string             `json:"runtimeHash,omitempty"`
	SpecHash    string             `json:"specHash,omitempty"`
	Reload      bool               `json:"reload,omitempty"`
}

type Controller struct {
	Registry      *engine.Registry
	InternalCAPEM string
}

func Member(cluster string) string { return cluster + "mcp" }

var hostname = regexp.MustCompile(`^[a-z][a-z0-9]{0,39}$`)

func Validate(c *domain.Cluster, b engine.DataMcpBackend) error {
	if !c.Spec.DataMcp.DataMcpEnabled() || b.Kind != engine.DataMcpCompanion {
		return nil
	}
	if !hostname.MatchString(Member(c.Name)) {
		return engine.InvalidField("dataMcp", "hostname", "invalid data MCP member hostname")
	}
	if b.Upstream != companionspec.UpstreamMongoDB && b.Upstream != companionspec.UpstreamPostgres {
		return engine.InvalidField("dataMcp", "upstream", "unsupported companion upstream %q", b.Upstream)
	}
	a, err := nativeartifacts.Get(b.Upstream)
	if err != nil {
		return engine.InvalidField("dataMcp", "artifact", "%v", err)
	}
	_, err = nativeartifacts.ResolveURL(a)
	if err != nil {
		return engine.InvalidField("dataMcp", "artifact", "%v", err)
	}
	return nil
}
func load(ctx context.Context, rc *engine.Context) (*state, error) {
	raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), stateKey)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var s state
	if err = json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
func save(ctx context.Context, rc *engine.Context, s *state) error {
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), stateKey, raw)
}
func (c *Controller) backend(rc *engine.Context) (engine.DataMcpProvider, engine.DataMcpBackend, error) {
	p, err := c.Registry.Get(rc.Cluster.Spec.Provider)
	if err != nil {
		return nil, engine.DataMcpBackend{}, err
	}
	d, ok := p.(engine.DataMcpProvider)
	if !ok {
		return nil, engine.DataMcpBackend{}, nil
	}
	b, err := d.DataMcp(rc.Cluster)
	return d, b, err
}
func token(ctx context.Context, rc *engine.Context) (*domain.AgentToken, error) {
	m := Member(rc.Cluster.Name)
	t, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m)
	if !errors.Is(err, store.ErrNotFound) {
		return t, err
	}
	value, err := secrets.Token()
	if err != nil {
		return nil, err
	}
	t = &domain.AgentToken{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Member: m, Token: value, Hash: secrets.Hash(value)}
	return t, rc.Secrets.SetAgentToken(ctx, t)
}
func wait() engine.Result { return engine.Result{RequeueAfter: time.Second} }
func (c *Controller) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	p, b, err := c.backend(rc)
	if err != nil {
		return engine.Result{}, err
	}
	if !rc.Cluster.Spec.DataMcp.DataMcpEnabled() || b.Kind != engine.DataMcpCompanion {
		done, err := c.Cleanup(ctx, rc)
		if !done {
			return wait(), err
		}
		return engine.Result{}, err
	}
	if err = Validate(rc.Cluster, b); err != nil {
		return engine.Result{}, err
	}
	s, err := load(ctx, rc)
	if err != nil {
		return engine.Result{}, err
	}
	t, err := token(ctx, rc)
	if err != nil {
		return engine.Result{}, err
	}
	m := Member(rc.Cluster.Name)
	if s == nil {
		existing, err := rc.Backend.GetService(ctx, m)
		if err != nil {
			return engine.Result{}, err
		}
		if existing != nil {
			return engine.Result{}, fmt.Errorf("data MCP hostname %s already exists", m)
		}
		r := resources(rc.Cluster)
		s = &state{Stage: "import", Plan: engine.NewImportPlan([]infra.ServiceSpec{engine.RuntimeServiceSpec(rc.Backend.Layout(), engine.MemberService{Cluster: rc.Cluster.Name, Member: m, Token: t.Token, Resources: r, Containers: 1})})}
		return wait(), save(ctx, rc, s)
	}
	if s.Stage == "delete" {
		done, err := c.Cleanup(ctx, rc)
		if !done {
			return wait(), err
		}
		return wait(), err
	}
	if op, err := engine.ResumeImportPlan(ctx, rc, s.Plan, m, t.Token); err != nil {
		return wait(), errors.Join(err, save(ctx, rc, s))
	} else if op != nil {
		s.Pending = op
		return wait(), save(ctx, rc, s)
	}
	if s.Pending != nil {
		status, err := rc.Backend.OpStatus(ctx, *s.Pending)
		if err != nil {
			return wait(), err
		}
		if status.Failed {
			s.Pending = nil
			return wait(), errors.Join(fmt.Errorf("data MCP %s failed: %s", s.Stage, status.Status), save(ctx, rc, s))
		}
		if !status.Done {
			return wait(), nil
		}
		s.Pending = nil
		if s.Stage == "import" {
			s.Stage = "deploy"
			s.Plan = nil
		} else if s.Stage == "deploy" {
			s.Stage = "ready"
		}
		return wait(), save(ctx, rc, s)
	}
	service, err := rc.Backend.GetService(ctx, m)
	if err != nil {
		return wait(), err
	}
	if service != nil {
		if s.ID != "" && service.ID != s.ID {
			return wait(), fmt.Errorf("data MCP service identity changed")
		}
		if !owned(service, t.Token, rc.Cluster.Name, m) {
			return wait(), fmt.Errorf("data MCP service ownership mismatch")
		}
		if s.ID == "" {
			s.ID = service.ID
			return wait(), save(ctx, rc, s)
		}
	}
	if s.Stage == "import" {
		op, err := s.Plan.Submit(ctx, rc, m, t.Token)
		if err == nil {
			s.Pending = &op
		}
		return wait(), errors.Join(err, save(ctx, rc, s))
	}
	if service == nil {
		return wait(), fmt.Errorf("data MCP service disappeared")
	}
	desiredResources := resources(rc.Cluster)
	if !resourcesMatch(service.Resources, desiredResources) {
		op, err := rc.Backend.SetResources(ctx, m, desiredResources)
		if err != nil {
			return wait(), err
		}
		s.Pending = &op
		return wait(), save(ctx, rc, s)
	}
	changed, err := publish(ctx, rc, p, b, s)
	if err != nil {
		return wait(), err
	}
	if changed {
		s.Reload = true
		if err = save(ctx, rc, s); err != nil {
			return wait(), err
		}
	}
	runtime, err := c.runtime(rc, b)
	if err != nil {
		return wait(), err
	}
	raw, _ := json.Marshal(runtime)
	hash := secrets.Hash(string(raw))
	if s.Stage == "deploy" || hash != s.RuntimeHash {
		op, err := engine.Deploy(ctx, rc, m, runtime)
		if err != nil {
			return wait(), err
		}
		s.Pending = &op
		s.Stage = "deploy"
		s.RuntimeHash = hash
		return wait(), save(ctx, rc, s)
	}
	if s.Reload {
		client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m)
		if err != nil {
			return wait(), err
		}
		if err = client.Reload(ctx); err != nil {
			return wait(), err
		}
		s.Reload = false
		if err = save(ctx, rc, s); err != nil {
			return wait(), err
		}
	}
	if err = ready(ctx, rc); err != nil {
		return engine.Result{RequeueAfter: engine.ReadinessInterval}, nil
	}
	return engine.Result{}, nil
}

// Cleanup retains sealed identity until deletion has completed, including when
// database volumes are kept. A hostname alone never authorizes deletion.
func (c *Controller) Cleanup(ctx context.Context, rc *engine.Context) (bool, error) {
	s, err := load(ctx, rc)
	if err != nil || s == nil {
		return s == nil, err
	}
	m := Member(rc.Cluster.Name)
	service, err := rc.Backend.GetService(ctx, m)
	if err != nil {
		return false, err
	}
	if service != nil {
		t, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m)
		if err != nil {
			return false, err
		}
		if s.ID != "" && s.ID != service.ID {
			return false, fmt.Errorf("refusing to delete replacement data MCP service")
		}
		if !owned(service, t.Token, rc.Cluster.Name, m) {
			return false, fmt.Errorf("refusing to delete unowned data MCP service")
		}
		if op, err := engine.ResumeImportPlan(ctx, rc, s.Plan, m, t.Token); err != nil {
			return false, errors.Join(err, save(ctx, rc, s))
		} else if op != nil {
			s.Pending = op
			return false, save(ctx, rc, s)
		}
	}
	if s.Pending != nil {
		v, err := rc.Backend.OpStatus(ctx, *s.Pending)
		if err != nil {
			return false, err
		}
		if !v.Done && !v.Failed {
			return false, nil
		}
		s.Pending = nil
		return false, save(ctx, rc, s)
	}
	if service != nil {
		if s.ID == "" {
			s.ID = service.ID
			return false, save(ctx, rc, s)
		}
		op, err := rc.Backend.DeleteServiceByID(ctx, m, s.ID)
		if err != nil {
			return false, err
		}
		s.Pending = &op
		s.Stage = "delete"
		s.Plan = nil
		return false, save(ctx, rc, s)
	}
	if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), configKey); err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	if err = rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m); err != nil && !errors.Is(err, store.ErrNotFound) {
		return false, err
	}
	err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), stateKey)
	return err == nil, err
}

func MemberConfig(ctx context.Context, rc *engine.Context, member string) (agentapi.MemberConfig, error) {
	var cfg agentapi.MemberConfig
	if member != Member(rc.Cluster.Name) {
		return cfg, store.ErrNotFound
	}
	s, err := load(ctx, rc)
	if err != nil {
		return cfg, err
	}
	if s == nil {
		return cfg, store.ErrNotFound
	}
	raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), configKey)
	if err != nil {
		return cfg, err
	}
	err = json.Unmarshal(raw, &cfg)
	return cfg, err
}

func resources(c *domain.Cluster) domain.Resources {
	if c.Spec.DataMcp.Resources != nil {
		return *c.Spec.DataMcp.Resources
	}
	return domain.Resources{CPUMode: "shared", MinCPU: 1, MaxCPU: 1, MinRAM: 0.25, MaxRAM: 0.5}
}

// An explicit mismatching public marker is a conflict, even if legacy token
// variables happen to match. Never adopt a replacement through its hostname.
func owned(s *infra.Service, token, cluster, member string) bool {
	if marker := s.Ownership[engine.OwnerVariable]; marker != "" && marker != engine.OwnerMarker(token, cluster, member, s.Hostname) {
		return false
	}
	return engine.HasOwnership(s, token, cluster, member)
}
