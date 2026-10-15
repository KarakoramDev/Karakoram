// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"gopkg.in/yaml.v3"
)

// Backend implements Zerops infrastructure operations. Persistent disk changes must
// target the volume hostname, not the runtime's ephemeral root filesystem.
type Backend struct {
	client                    *Client
	mu                        sync.Mutex
	ids                       map[string]string
	cachedAt                  time.Time
	pollInterval, waitTimeout time.Duration
}

var _ infra.Backend = (*Backend)(nil)
var _ infra.SubdomainAccess = (*Backend)(nil)
var hostnamePattern = regexp.MustCompile(`^[a-z][a-z0-9]{0,24}$`)

// New constructs a Zerops backend, using bounded context-cancelable waits for
// secrets and routing. SetEnvSecrets does not restart running database members.
func New(o Options) (*Backend, error) {
	c, err := NewClient(o)
	if err != nil {
		return nil, err
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	if o.WaitTimeout <= 0 {
		o.WaitTimeout = 5 * time.Minute
	}
	return &Backend{client: c, ids: make(map[string]string), pollInterval: o.PollInterval, waitTimeout: o.WaitTimeout}, nil
}

// Name returns the infrastructure backend identifier.
func (b *Backend) Name() string { return "zerops" }

// SetToken swaps the live client's credential without restarting services.
func (b *Backend) SetToken(token string) { b.client.SetToken(token) }

// SudoMode reports whether the token can read sensitive variables. Karakoram
// never requires it: ownership uses recorded IDs and non-sensitive markers.
func (b *Backend) SudoMode(ctx context.Context) (bool, error) { return b.client.SudoMode(ctx) }

// ValidateProject confirms both project identity and service-list permissions.
func ValidateProject(ctx context.Context, o Options) (*Backend, infra.Project, error) {
	b, err := New(o)
	if err != nil {
		return nil, infra.Project{}, err
	}
	p, err := b.ProjectInfo(ctx)
	if err != nil {
		if IsAuthenticationError(err) {
			return nil, infra.Project{}, fmt.Errorf("zerops: token cannot access the project: %w", err)
		}
		return nil, infra.Project{}, fmt.Errorf("zerops: validate project access: %w", err)
	}
	if p.ID != o.ProjectID {
		return nil, infra.Project{}, &APIError{Code: "accessDenied", Message: "Authenticated project does not match the requested project"}
	}
	if _, err = b.ListServices(ctx, ""); err != nil {
		if IsAuthenticationError(err) {
			return nil, infra.Project{}, fmt.Errorf("zerops: token cannot list project services: %w", err)
		}
		return nil, infra.Project{}, fmt.Errorf("zerops: validate project service access: %w", err)
	}
	return b, p, nil
}

// ProjectInfo returns the project identity and public addresses.
func (b *Backend) ProjectInfo(ctx context.Context) (infra.Project, error) {
	p, err := b.client.ProjectInfo(ctx)
	return infra.Project{ID: p.ID, Name: p.Name, PublicIPv4: p.PublicIPv4, PublicIPv6: p.PublicIPv6}, err
}
func (b *Backend) invalidate(name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.ids, name)
	b.cachedAt = time.Time{}
}
func (b *Backend) refresh(ctx context.Context) ([]ServiceStack, error) {
	list, err := b.client.ListServices(ctx)
	if err != nil {
		return nil, err
	}
	ids := make(map[string]string, len(list))
	for _, s := range list {
		if s.ProjectID != "" && s.ProjectID != b.client.project {
			return nil, errors.New("zerops: service belongs to another project")
		}
		ids[s.Name] = s.ID
	}
	b.mu.Lock()
	b.ids = ids
	b.cachedAt = time.Now()
	b.mu.Unlock()
	return list, nil
}
func notFound(err error) bool {
	var e *APIError
	return errors.As(err, &e) && (e.StatusCode == 404 || (e.StatusCode == 400 && e.Code == "serviceStackNotFound"))
}

// processExpired recognizes a process the platform no longer retains. Zerops
// forgets finished processes after about a day.
func processExpired(err error) bool {
	var e *APIError
	return errors.As(err, &e) && (e.StatusCode == 404 || e.StatusCode == 400) && e.Code == "processNotFound"
}

// maskedValue is how Zerops returns sensitive variables to tokens without
// sudo mode. It is never a value: it only means "not readable".
const maskedValue = "REDACTED"

// ownershipKeys are the only variables GetService exposes.
var ownershipKeys = map[string]bool{"KARAKORAM_OWNER": true, "KARAKORAM_CLUSTER": true, "KARAKORAM_MEMBER": true, "KARAKORAM_AGENT_TOKEN": true, "KARAKORAM_DATA_DIR": true, "KARAKORAM_RELAY_OWNER": true}

func (b *Backend) resolve(ctx context.Context, name string) (*ServiceStack, error) {
	if name == "core" {
		return nil, errors.New("zerops: refusing protected service")
	}
	if !hostnamePattern.MatchString(name) {
		return nil, errors.New("zerops: invalid service hostname")
	}
	for range 2 {
		b.mu.Lock()
		id := b.ids[name]
		fresh := time.Since(b.cachedAt) < time.Minute
		b.mu.Unlock()
		if id == "" || !fresh {
			if _, err := b.refresh(ctx); err != nil {
				return nil, err
			}
			b.mu.Lock()
			id = b.ids[name]
			b.mu.Unlock()
		}
		if id == "" {
			return nil, nil
		}
		s, err := b.client.GetService(ctx, id)
		if notFound(err) {
			b.invalidate(name)
			continue
		}
		if err != nil {
			return nil, err
		}
		if s.protected() {
			b.invalidate(name)
			return nil, errors.New("zerops: refusing protected service")
		}
		if s.Name != name {
			b.invalidate(name)
			continue
		}
		if s.ProjectID != "" && s.ProjectID != b.client.project {
			return nil, errors.New("zerops: service belongs to another project")
		}
		return &s, nil
	}
	return nil, nil
}
func (b *Backend) require(ctx context.Context, name string) (*ServiceStack, error) {
	s, err := b.resolve(ctx, name)
	if err == nil && s == nil {
		err = fmt.Errorf("zerops: service %s not found", name)
	}
	return s, err
}
func processOp(p *Process, kind, id string) infra.Op {
	if p == nil {
		return infra.Op{Kind: kind, ServiceID: id}
	}
	return infra.Op{ID: p.ID, Kind: kind, ServiceID: id}
}

// ImportServices builds the no-inline-pipeline import YAML and aggregates all
// returned process IDs. ZeropsYAML/Volume belong in Deployment's runtime document.
func (b *Backend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	type vertical struct {
		CPUMode  string   `yaml:"cpuMode,omitempty"`
		MinCPU   float64  `yaml:"minCpu,omitempty"`
		MaxCPU   float64  `yaml:"maxCpu,omitempty"`
		MinRAM   float64  `yaml:"minRam,omitempty"`
		MaxRAM   float64  `yaml:"maxRam,omitempty"`
		MinDisk  float64  `yaml:"minDisk,omitempty"`
		MaxDisk  float64  `yaml:"maxDisk,omitempty"`
		StartCPU *float64 `yaml:"startCpuCoreCount,omitempty"`
	}
	type service struct {
		Hostname      string            `yaml:"hostname"`
		Type          string            `yaml:"type"`
		MinContainers int               `yaml:"minContainers,omitempty"`
		MaxContainers int               `yaml:"maxContainers,omitempty"`
		Vertical      vertical          `yaml:"verticalAutoscaling"`
		Env           map[string]string `yaml:"envSecrets,omitempty"`
		Vars          map[string]string `yaml:"envVariables,omitempty"`
	}
	doc := struct {
		Services []service `yaml:"services"`
	}{Services: make([]service, 0, len(specs))}
	seen := map[string]bool{}
	for _, s := range specs {
		if !hostnamePattern.MatchString(s.Hostname) || s.Hostname == "core" || seen[s.Hostname] {
			return infra.Op{}, errors.New("zerops: invalid or duplicate import hostname")
		}
		seen[s.Hostname] = true
		if s.Type == "" || strings.HasPrefix(s.Type, "core") {
			return infra.Op{}, errors.New("zerops: invalid or protected service type")
		}
		r := s.Resources
		v := vertical{CPUMode: r.CPUMode, MinCPU: r.MinCPU, MaxCPU: r.MaxCPU, MinRAM: r.MinRAM, MaxRAM: r.MaxRAM, MinDisk: r.MinDisk, MaxDisk: r.MaxDisk, StartCPU: r.StartCPU}
		minCount, maxCount := s.MinContainers, s.MaxContainers
		if strings.HasPrefix(s.Type, "local-storage") {
			s.Type = "local-storage:single@1"
			v = vertical{MinDisk: max(r.MinDisk, 5), MaxDisk: max(r.MaxDisk, max(r.MinDisk, 5))}
			minCount, maxCount = 0, 0
		} else {
			if minCount == 0 {
				minCount = 1
			}
			if maxCount == 0 {
				maxCount = minCount
			}
			if v.CPUMode == "" {
				v.CPUMode = "SHARED"
			}
		}
		if minCount < 0 || maxCount < minCount {
			return infra.Op{}, errors.New("zerops: invalid container bounds")
		}
		for k := range s.Vars {
			if _, dup := s.Env[k]; dup {
				return infra.Op{}, errors.New("zerops: variable declared both sensitive and non-sensitive")
			}
		}
		doc.Services = append(doc.Services, service{s.Hostname, s.Type, minCount, maxCount, v, s.Env, s.Vars})
	}
	if len(specs) == 0 {
		return infra.Op{Kind: "import"}, nil
	}
	data, err := yaml.Marshal(doc)
	if err != nil {
		return infra.Op{}, errors.New("zerops: cannot encode import YAML")
	}
	result, err := b.client.ImportServices(ctx, string(data))
	if err != nil {
		return infra.Op{}, err
	}
	var ids []string
	created := make(map[string]string, len(result.ServiceStacks))
	for _, s := range result.ServiceStacks {
		for _, p := range s.Processes {
			if p.ID == "" {
				return infra.Op{}, errors.New("zerops: import returned empty process ID")
			}
			ids = append(ids, p.ID)
		}
		if s.ID != "" && seen[s.Name] {
			created[s.Name] = s.ID
		}
		b.invalidate(s.Name)
	}
	ids = slices.Compact(slices.Sorted(slices.Values(ids)))
	// JSON IDs are durable and unambiguous; no in-memory operation registry needed.
	encoded, _ := json.Marshal(ids)
	op := infra.Op{ID: "multi:" + string(encoded), Kind: "import"}
	if len(created) > 0 {
		op.Services = created
	}
	return op, nil
}
func decodeObject(raw json.RawMessage) (map[string]json.RawMessage, error) {
	m := map[string]json.RawMessage{}
	if len(raw) == 0 || string(raw) == "null" {
		return m, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, errors.New("zerops: invalid autoscaling configuration")
	}
	return m, nil
}
func number(m map[string]json.RawMessage, k string) float64 {
	var n float64
	_ = json.Unmarshal(m[k], &n)
	return n
}
func observed(s ServiceStack) infra.Service {
	v, _ := decodeObject(s.CurrentAutoscaling["verticalAutoscaling"])
	low, _ := decodeObject(v["minResource"])
	high, _ := decodeObject(v["maxResource"])
	h, _ := decodeObject(s.CurrentAutoscaling["horizontalAutoscaling"])
	var mode string
	_ = json.Unmarshal(v["cpuMode"], &mode)
	r := infra.Resources{CPUMode: mode, MinCPU: number(low, "cpuCoreCount"), MaxCPU: number(high, "cpuCoreCount"), MinRAM: number(low, "memoryGBytes"), MaxRAM: number(high, "memoryGBytes"), MinDisk: number(low, "diskGBytes"), MaxDisk: number(high, "diskGBytes")}
	if raw := v["startCpuCoreCount"]; len(raw) > 0 && string(raw) != "null" {
		n := number(v, "startCpuCoreCount")
		r.StartCPU = &n
	}
	typ := s.Base
	if typ == "" {
		typ = s.ServiceStackTypeInfo.Version
	}
	return infra.Service{ID: s.ID, Hostname: s.Name, Type: typ, Status: s.Status, Resources: r, MinContainers: int(number(h, "minContainerCount")), MaxContainers: int(number(h, "maxContainerCount"))}
}

// GetService returns nil when absent, invalidating stale platform IDs as needed.
func (b *Backend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	s, err := b.resolve(ctx, name)
	if err != nil || s == nil {
		return nil, err
	}
	v := observed(*s)
	items, err := b.client.Env(ctx, s.ID)
	if err != nil {
		return nil, err
	}
	v.Ownership = map[string]string{}
	for _, item := range items {
		if !ownershipKeys[item.Key] {
			continue
		}
		// A masked sensitive value is "unreadable", never a value to compare or keep.
		if item.Content == maskedValue {
			v.Masked = true
			continue
		}
		v.Ownership[item.Key] = item.Content
	}
	return &v, nil
}

// ListServices follows pagination and excludes system services before prefix filtering.
func (b *Backend) ListServices(ctx context.Context, prefix string) ([]infra.Service, error) {
	list, err := b.refresh(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]infra.Service, 0, len(list))
	for _, s := range list {
		if strings.HasPrefix(s.Name, prefix) {
			out = append(out, observed(s))
		}
	}
	return out, nil
}

// DeleteService schedules deletion; an already absent service is a completed no-op.
func (b *Backend) DeleteService(ctx context.Context, name string) (infra.Op, error) {
	s, err := b.resolve(ctx, name)
	if err != nil {
		return infra.Op{}, err
	}
	if s == nil {
		return infra.Op{Kind: "delete"}, nil
	}
	p, err := b.client.DeleteService(ctx, s.ID)
	b.invalidate(name)
	return processOp(&p, "delete", s.ID), err
}

// DeleteServiceByID fences deletion against hostname reuse. Resolve enforces the
// same protected-service refusals as ordinary engine operations.
func (b *Backend) DeleteServiceByID(ctx context.Context, name, expectedID string) (infra.Op, error) {
	s, err := b.resolve(ctx, name)
	if err != nil {
		return infra.Op{}, err
	}
	if s == nil {
		// Project listings intentionally hide protected services. A recorded
		// ID must not turn a hidden system service into an apparent absence.
		if expectedID != "" {
			recorded, err := b.client.GetService(ctx, expectedID)
			if err != nil && !notFound(err) {
				return infra.Op{}, err
			}
			if err == nil && recorded.protected() {
				return infra.Op{}, errors.New("zerops: refusing protected service")
			}
		}
		return infra.Op{Kind: "delete"}, nil
	}
	if expectedID == "" || s.ID != expectedID {
		return infra.Op{}, errors.New("zerops: refusing deletion of replaced service")
	}
	p, err := b.client.DeleteService(ctx, s.ID)
	b.invalidate(name)
	return processOp(&p, "delete", s.ID), err
}

// Deploy streams a gzip-tar artifact, spooling only non-seekable readers to a
// private temporary file to supply the fixed Content-Length expected by Zerops.
func (b *Backend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	s, err := b.require(ctx, name)
	if err != nil {
		return infra.Op{}, err
	}
	if d.Artifact == nil || d.ZeropsYAML == "" || d.SetupName == "" {
		return infra.Op{}, errors.New("zerops: artifact, runtime YAML and setup required")
	}
	var reader io.Reader = d.Artifact
	var size int64
	if seeker, ok := d.Artifact.(io.ReadSeeker); ok {
		start, e := seeker.Seek(0, io.SeekCurrent)
		if e != nil {
			return infra.Op{}, e
		}
		end, e := seeker.Seek(0, io.SeekEnd)
		if e != nil {
			return infra.Op{}, e
		}
		if _, e = seeker.Seek(start, io.SeekStart); e != nil {
			return infra.Op{}, e
		}
		size = end - start
	} else {
		f, e := os.CreateTemp("", "karakoram-artifact-*")
		if e != nil {
			return infra.Op{}, e
		}
		defer os.Remove(f.Name())
		defer f.Close()
		size, e = io.Copy(f, contextReader{ctx, d.Artifact})
		if e != nil {
			return infra.Op{}, e
		}
		if _, e = f.Seek(0, io.SeekStart); e != nil {
			return infra.Op{}, e
		}
		reader = f
	}
	version, err := b.client.CreateAppVersion(ctx, s.ID, d.Filename)
	if err != nil {
		return infra.Op{}, err
	}
	if version.ID == "" {
		return infra.Op{}, errors.New("zerops: empty app version ID")
	}
	if err = b.client.Upload(ctx, version.ID, reader, size); err != nil {
		return infra.Op{}, err
	}
	p, err := b.client.Deploy(ctx, version.ID, d.ZeropsYAML, d.SetupName)
	return processOp(&p, "deploy", s.ID), err
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

// SetEnvSecrets waits for user-data operations but deliberately does not restart
// members: engine providers own quorum-safe rolling restarts.
func (b *Backend) SetEnvSecrets(ctx context.Context, name string, kv map[string]string) error {
	return b.setUserData(ctx, name, kv, true)
}

// SetEnvVars writes non-sensitive variables, readable by tokens without sudo
// mode; existing sensitive entries with the same key become non-sensitive.
func (b *Backend) SetEnvVars(ctx context.Context, name string, kv map[string]string) error {
	return b.setUserData(ctx, name, kv, false)
}
func (b *Backend) setUserData(ctx context.Context, name string, kv map[string]string, sensitive bool) error {
	ctx, cancel := context.WithTimeout(ctx, b.waitTimeout)
	defer cancel()
	s, err := b.require(ctx, name)
	if err != nil {
		return err
	}
	items, err := b.client.Env(ctx, s.ID)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		var id string
		unchanged := false
		for _, item := range items {
			if item.Key == key {
				if item.Type != "USER" {
					return errors.New("zerops: cannot replace system environment entry")
				}
				// Only ownership markers may turn a sensitive entry into a
				// readable one; anything else could publish a secret.
				if item.Sensitive && !sensitive && !convertibleToPlain[key] {
					return errors.New("zerops: refusing to make a sensitive variable readable")
				}
				id = item.ID
				// A masked value never equals anything: rewrite it.
				unchanged = item.Content != maskedValue && item.Content == kv[key] && item.Sensitive == sensitive
				break
			}
		}
		if unchanged {
			continue
		}
		p, e := b.client.SetUserData(ctx, s.ID, id, key, kv[key], sensitive)
		if e != nil {
			return e
		}
		if e = b.waitOp(ctx, processOp(&p, "secret", s.ID)); e != nil {
			return e
		}
	}
	return nil
}

// convertibleToPlain lists the markers earlier versions stored as sensitive
// variables; they carry no secret and may be rewritten as readable.
var convertibleToPlain = map[string]bool{"KARAKORAM_OWNER": true, "KARAKORAM_RELAY_OWNER": true}

func (b *Backend) lifecycle(ctx context.Context, name, action string) (infra.Op, error) {
	s, err := b.require(ctx, name)
	if err != nil {
		return infra.Op{}, err
	}
	p, err := b.client.Lifecycle(ctx, s.ID, action)
	return processOp(&p, action, s.ID), err
}

// Start schedules runtime start.
func (b *Backend) Start(ctx context.Context, name string) (infra.Op, error) {
	return b.lifecycle(ctx, name, "start")
}

// Stop schedules runtime stop.
func (b *Backend) Stop(ctx context.Context, name string) (infra.Op, error) {
	return b.lifecycle(ctx, name, "stop")
}

// Restart schedules runtime restart without replaying the process-creating PUT.
func (b *Backend) Restart(ctx context.Context, name string) (infra.Op, error) {
	return b.lifecycle(ctx, name, "restart")
}

// SetSubdomainAccess schedules public HTTPS routing for a service's HTTP ports.
// Matching state is a completed no-op; returned process IDs survive backend restarts.
func (b *Backend) SetSubdomainAccess(ctx context.Context, name string, enabled bool) (infra.Op, error) {
	s, err := b.require(ctx, name)
	if err != nil {
		return infra.Op{}, err
	}
	kind := "disable-subdomain-access"
	if enabled {
		kind = "enable-subdomain-access"
	}
	if s.SubdomainAccess == enabled {
		return processOp(nil, kind, s.ID), nil
	}
	p, err := b.client.SetSubdomainAccess(ctx, s.ID, enabled)
	return processOp(&p, kind, s.ID), err
}

// SubdomainURLs derives enabled HTTPS URLs from effective ports and project identity.
// Zerops terminates TLS at its public L7 balancer; URLs have no explicit port.
func (b *Backend) SubdomainURLs(ctx context.Context, name string) ([]string, error) {
	s, err := b.require(ctx, name)
	if err != nil {
		return nil, err
	}
	if !s.SubdomainAccess {
		return nil, nil
	}
	count := 0
	for _, p := range s.Ports {
		if p.HTTPSupport || p.Scheme == "http" || p.Scheme == "https" {
			count++
		}
	}
	if count == 0 {
		return nil, nil
	}
	project, err := b.client.ProjectInfo(ctx)
	if err != nil {
		return nil, err
	}
	if project.ZeropsSubdomainHost == "" || s.NestID == "" {
		return nil, errors.New("zerops: missing subdomain routing identity")
	}
	urls := make([]string, 0, count)
	for _, p := range s.Ports {
		if p.HTTPSupport || p.Scheme == "http" || p.Scheme == "https" {
			urls = append(urls, fmt.Sprintf("https://%s-%s-%d.%s.zerops.app", s.Name, project.ZeropsSubdomainHost, p.Port, s.NestID))
		}
	}
	return urls, nil
}
func putJSON(m map[string]json.RawMessage, k string, v any) { m[k], _ = json.Marshal(v) }
func (b *Backend) resources(ctx context.Context, name string, r *infra.Resources, minCount, maxCount *int) (infra.Op, error) {
	s, err := b.require(ctx, name)
	if err != nil {
		return infra.Op{}, err
	}
	if len(s.CurrentAutoscaling) == 0 {
		return infra.Op{}, errors.New("zerops: missing current autoscaling configuration")
	}
	config := s.CurrentAutoscaling
	v, err := decodeObject(config["verticalAutoscaling"])
	if err != nil {
		return infra.Op{}, err
	}
	low, err := decodeObject(v["minResource"])
	if err != nil {
		return infra.Op{}, err
	}
	high, err := decodeObject(v["maxResource"])
	if err != nil {
		return infra.Op{}, err
	}
	// Effective free-resource defaults are not valid input: for example Zerops
	// returns 0.0625 GB but its PUT validator accepts only three decimal places.
	// Preserve the nullable custom override (including null = platform default)
	// instead of rounding or freezing a derived value.
	if s.CustomAutoscaling != nil {
		custom, e := decodeObject(s.CustomAutoscaling["verticalAutoscaling"])
		if e != nil {
			return infra.Op{}, e
		}
		if free := custom["minFreeResource"]; len(free) != 0 {
			v["minFreeResource"] = free
		} else {
			v["minFreeResource"] = json.RawMessage("null")
		}
	}
	if r != nil {
		if strings.HasPrefix(s.Base, "local-storage") || strings.HasPrefix(s.ServiceStackTypeInfo.Version, "local-storage") {
			if (r.MinDisk > 0 && r.MinDisk < 5) || (r.MaxDisk > 0 && r.MaxDisk < 5) {
				return infra.Op{}, errors.New("zerops: volume disk bounds must be at least 5 GB")
			}
		}
		for _, entry := range []struct {
			m map[string]json.RawMessage
			k string
			n float64
		}{{low, "cpuCoreCount", r.MinCPU}, {high, "cpuCoreCount", r.MaxCPU}, {low, "memoryGBytes", r.MinRAM}, {high, "memoryGBytes", r.MaxRAM}, {low, "diskGBytes", r.MinDisk}, {high, "diskGBytes", r.MaxDisk}} {
			if entry.n < 0 {
				return infra.Op{}, errors.New("zerops: negative resource bound")
			}
			if entry.n > 0 {
				putJSON(entry.m, entry.k, entry.n)
			}
		}
		if r.CPUMode != "" {
			putJSON(v, "cpuMode", r.CPUMode)
		}
		if r.StartCPU != nil {
			putJSON(v, "startCpuCoreCount", *r.StartCPU)
		}
		putJSON(v, "minResource", low)
		putJSON(v, "maxResource", high)
	}
	putJSON(config, "verticalAutoscaling", v)
	if minCount != nil {
		h, e := decodeObject(config["horizontalAutoscaling"])
		if e != nil {
			return infra.Op{}, e
		}
		putJSON(h, "minContainerCount", *minCount)
		putJSON(h, "maxContainerCount", *maxCount)
		putJSON(config, "horizontalAutoscaling", h)
	}
	p, err := b.client.Autoscaling(ctx, s.ID, config)
	return processOp(p, "autoscaling", s.ID), err
}

// SetResources fetches current effective configuration and preserves every field
// not specified by nonzero bounds, CPUMode or StartCPU. Zero bounds mean unchanged.
func (b *Backend) SetResources(ctx context.Context, name string, r infra.Resources) (infra.Op, error) {
	return b.resources(ctx, name, &r, nil, nil)
}

// SetContainerCount updates horizontal bounds while preserving complete vertical
// settings, including platform defaults, free-resource thresholds and swap.
func (b *Backend) SetContainerCount(ctx context.Context, name string, minCount, maxCount int) (infra.Op, error) {
	if minCount < 1 || maxCount < minCount {
		return infra.Op{}, errors.New("zerops: invalid container bounds")
	}
	return b.resources(ctx, name, nil, &minCount, &maxCount)
}
func routeEqual(a, b PublicPortRouting) bool {
	x, y := slices.Clone(a.FirewallIPRanges), slices.Clone(b.FirewallIPRanges)
	sort.Strings(x)
	sort.Strings(y)
	return a.InternalPort == b.InternalPort && a.PublicIPType == b.PublicIPType && a.PublicPort == b.PublicPort && a.InternalProtocol == b.InternalProtocol && a.FirewallPolicy == b.FirewallPolicy && a.FirewallAllowMyIP == b.FirewallAllowMyIP && slices.Equal(x, y)
}
func (b *Backend) syncRoutes(ctx context.Context) error {
	p, err := b.client.SyncPublicPorts(ctx)
	if err != nil {
		return err
	}
	return b.waitOp(ctx, processOp(&p, "routing", ""))
}

// ListPublicPorts observes every project's route, including services managed by
// other controllers. Staged/deleting routes stay reserved until they disappear.
func (b *Backend) ListPublicPorts(ctx context.Context) ([]infra.PublicPortRoute, error) {
	services, err := getPages[ServiceStack](ctx, b.client, b.client.projectPath("/service-stack"))
	if err != nil {
		return nil, err
	}
	var out []infra.PublicPortRoute
	for _, service := range services {
		routes, err := b.client.PublicPorts(ctx, service.ID)
		if err != nil {
			return nil, err
		}
		for _, route := range routes {
			out = append(out, infra.PublicPortRoute{Hostname: service.Name, PublicPort: infra.PublicPort{Port: route.PublicPort, TargetPort: route.InternalPort, Protocol: route.InternalProtocol, IPType: route.PublicIPType, Allowlist: route.FirewallIPRanges}})
		}
	}
	return out, nil
}

// EnsurePublicPort matches owned service routes by public or internal port,
// replaces changed rules and waits until the desired rule reports isSynced.
func (b *Backend) EnsurePublicPort(ctx context.Context, name string, p infra.PublicPort) error {
	ctx, cancel := context.WithTimeout(ctx, b.waitTimeout)
	defer cancel()
	s, err := b.require(ctx, name)
	if err != nil {
		return err
	}
	protocol := strings.ToLower(p.Protocol)
	if protocol == "" {
		protocol = "tcp"
	}
	if protocol != "tcp" {
		return errors.New("zerops: only TCP public routing supported")
	}
	ip := p.IPType
	switch ip {
	case "IPV6", "IP_V6":
		ip = "IP_V6"
	case "IPV4", "IP_V4":
		ip = "IP_V4"
	default:
		return errors.New("zerops: public IP type required")
	}
	if p.Port < 1 || p.Port > 65535 || p.TargetPort < 1 || p.TargetPort > 65535 {
		return errors.New("zerops: invalid public port")
	}
	wanted := PublicPortRouting{PublicIPType: ip, PublicPort: p.Port, InternalPort: p.TargetPort, InternalProtocol: protocol, FirewallPolicy: "WHITELIST", FirewallIPRanges: slices.Clone(p.Allowlist)}
	if wanted.FirewallIPRanges == nil {
		wanted.FirewallIPRanges = []string{}
	}
	routes, err := b.client.PublicPorts(ctx, s.ID)
	if err != nil {
		return err
	}
	found, needsSync := false, false
	for _, r := range routes {
		if r.InternalPort != p.TargetPort && r.PublicPort != p.Port {
			continue
		}
		if !r.DeleteOnSync && routeEqual(r, wanted) && !found {
			found = true
			needsSync = needsSync || !r.IsSynced
			continue
		}
		if !r.DeleteOnSync {
			if err = b.client.DeletePublicPort(ctx, r.ID); err != nil {
				return err
			}
		}
		needsSync = true
	}
	// Deletions must be activated before a replacement can reserve the same port.
	if needsSync {
		if err = b.syncRoutes(ctx); err != nil {
			return err
		}
	}
	if !found {
		if _, err = b.client.CreatePublicPort(ctx, s.ID, wanted); err != nil {
			return err
		}
		if err = b.syncRoutes(ctx); err != nil {
			return err
		}
	}
	for {
		routes, err = b.client.PublicPorts(ctx, s.ID)
		if err != nil {
			return err
		}
		for _, r := range routes {
			if r.InternalPort == p.TargetPort && !r.DeleteOnSync && r.IsSynced && routeEqual(r, wanted) {
				return nil
			}
		}
		if err = sleep(ctx, b.pollInterval); err != nil {
			return err
		}
	}
}

// DeletePublicPorts removes all service routes and verifies synchronized absence.
func (b *Backend) DeletePublicPorts(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, b.waitTimeout)
	defer cancel()
	s, err := b.resolve(ctx, name)
	if err != nil || s == nil {
		return err
	}
	routes, err := b.client.PublicPorts(ctx, s.ID)
	if err != nil {
		return err
	}
	if len(routes) == 0 {
		return nil
	}
	for _, r := range routes {
		if !r.DeleteOnSync {
			if err = b.client.DeletePublicPort(ctx, r.ID); err != nil {
				return err
			}
		}
	}
	if err = b.syncRoutes(ctx); err != nil {
		return err
	}
	for {
		routes, err = b.client.PublicPorts(ctx, s.ID)
		if err != nil {
			return err
		}
		if len(routes) == 0 {
			return nil
		}
		if err = sleep(ctx, b.pollInterval); err != nil {
			return err
		}
	}
}

// Containers returns all runtime containers; creation may yield empty hostnames.
func (b *Backend) Containers(ctx context.Context, name string) ([]infra.Container, error) {
	s, err := b.require(ctx, name)
	if err != nil {
		return nil, err
	}
	list, err := b.client.Containers(ctx, s.ID)
	if err != nil {
		return nil, err
	}
	out := make([]infra.Container, 0, len(list))
	for _, v := range list {
		c := infra.Container{ID: v.ID, Hostname: v.Hostname, Status: v.Status, IPv4: v.IPv4, IPv6: v.IPv6}
		if r := v.CurrentHardwareResource; r != nil {
			c.Resources = &domain.ObservedResources{CPUCores: r.CPUCores, MemoryGB: r.MemoryMB / 1024, DiskGB: r.DiskGB}
		}
		out = append(out, c)
	}
	return out, nil
}

// Logs returns bounded application logs filtered by timestamp and container.
func (b *Backend) Logs(ctx context.Context, name string, q infra.LogQuery) ([]infra.LogLine, error) {
	s, err := b.require(ctx, name)
	if err != nil {
		return nil, err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	list, err := b.client.Logs(ctx, s.ID, limit)
	if err != nil {
		return nil, err
	}
	out := make([]infra.LogLine, 0, len(list))
	for _, v := range list {
		if !q.Since.IsZero() && v.Timestamp.Before(q.Since) {
			continue
		}
		if q.Container != "" && q.Container != v.Hostname {
			continue
		}
		message := v.Content
		if message == "" {
			message = v.Message
		}
		out = append(out, infra.LogLine{Timestamp: v.Timestamp, Container: v.Hostname, Stream: v.FacilityLabel, Message: message})
	}
	return out, nil
}

// OpStatus aggregates durable process IDs: only all FINISHED means success;
// FAILED or CANCELED wins even when a sibling process is still running. Zerops
// forgets processes after about a day; an expired process has an unknown
// outcome. Imports then finish as Expired so callers observe the services they
// may have created; any other expired action fails as Expired so the caller's
// bounded retry re-issues it instead of polling a missing process forever.
func (b *Backend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "" {
		return infra.OpState{Done: true, Status: "FINISHED"}, nil
	}
	ids := []string{op.ID}
	if strings.HasPrefix(op.ID, "multi:") {
		if err := json.Unmarshal([]byte(strings.TrimPrefix(op.ID, "multi:")), &ids); err != nil {
			return infra.OpState{}, errors.New("zerops: invalid aggregate operation ID")
		}
	}
	state := infra.OpState{Done: true, Status: "FINISHED"}
	expired := 0
	for _, id := range ids {
		p, err := b.client.GetProcess(ctx, id)
		if processExpired(err) {
			expired++
			continue
		}
		if err != nil {
			return infra.OpState{}, err
		}
		if p.ServiceStackID != "" && !slices.Contains(state.ServiceIDs, p.ServiceStackID) {
			state.ServiceIDs = append(state.ServiceIDs, p.ServiceStackID)
		}
		switch p.Status {
		case "FINISHED":
		case "FAILED", "CANCELED":
			msg := p.Status
			if len(p.Error) > 0 && string(p.Error) != "null" {
				b.client.tokenMu.RLock()
				token := b.client.token
				b.client.tokenMu.RUnlock()
				if detail := processErrorSummary(p.Error, token); detail != "" {
					msg = detail
				}
			}
			return infra.OpState{Done: true, Failed: true, Status: p.Status, Message: msg}, nil
		default:
			state.Done = false
			state.Status = p.Status
		}
	}
	if expired > 0 {
		state.Expired = true
		if !state.Done {
			return state, nil
		}
		state.Status = "EXPIRED"
		if op.Kind != "import" {
			state.Failed = true
			state.Message = "platform process expired before its outcome was observed; retrying the action"
		}
	}
	return state, nil
}
func (b *Backend) waitOp(ctx context.Context, op infra.Op) error {
	for {
		state, err := b.OpStatus(ctx, op)
		if err != nil {
			return err
		}
		if state.Failed {
			return fmt.Errorf("zerops: process %s: %s", state.Status, state.Message)
		}
		if state.Done {
			return nil
		}
		if err = sleep(ctx, b.pollInterval); err != nil {
			return err
		}
	}
}
