// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"io"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// Resources expresses Zerops CPU, RAM, and disk autoscaling bounds.
type Resources = domain.Resources

// Project describes the backend project and available public addresses.
type Project struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	PublicIPv6 string `json:"publicIpv6,omitempty"`
	PublicIPv4 string `json:"publicIpv4,omitempty"`
}

// ServiceSpec declares a runtime or persistent volume service.
type ServiceSpec struct {
	Hostname      string    `json:"hostname"`
	Type          string    `json:"type"`
	Resources     Resources `json:"resources"`
	MinContainers int       `json:"minContainers"`
	MaxContainers int       `json:"maxContainers"`
	ZeropsYAML    string    `json:"zeropsYaml,omitempty"`
	// Env holds sensitive variables (secrets). Tokens without the platform's
	// sensitive-read capability observe them masked, so they are never
	// ownership evidence by themselves.
	Env map[string]string `json:"env,omitempty"`
	// Vars holds non-sensitive variables readable by every project token.
	Vars   map[string]string `json:"vars,omitempty"`
	Volume string            `json:"volume,omitempty"`
}

// Service describes an observed infrastructure service.
type Service struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	Type          string    `json:"type"`
	Status        string    `json:"status"`
	Resources     Resources `json:"resources"`
	MinContainers int       `json:"minContainers"`
	MaxContainers int       `json:"maxContainers"`
	// Ownership contains only Karakoram identity keys, never arbitrary environment
	// secrets. Values the token cannot read are omitted, never stored masked.
	Ownership map[string]string `json:"-"`
	// Masked reports that an ownership key exists but its sensitive value is
	// masked for this token, so legacy secret evidence cannot be compared.
	Masked bool `json:"-"`
}

// Deployment streams the executable artifact with its runtime configuration.
type Deployment struct {
	Artifact   io.Reader
	ZeropsYAML string
	SetupName  string
	Filename   string
}

// PublicPort declares public TCP routing and address restrictions.
type PublicPort struct {
	Port       int      `json:"port"`
	TargetPort int      `json:"targetPort"`
	Protocol   string   `json:"protocol"`
	IPType     string   `json:"ipType"`
	Allowlist  []string `json:"allowlist"`
}

// PublicPortRoute includes project-wide routes owned by any service.
type PublicPortRoute struct {
	Hostname string
	PublicPort
}

// Container describes an observed runtime container and placement hint.
type Container struct {
	ID             string `json:"id"`
	Hostname       string `json:"hostname"`
	Status         string `json:"status"`
	IPv4           string `json:"ipv4"`
	IPv6           string `json:"ipv6"`
	PlacementGroup string `json:"placementGroup,omitempty"`
	// Resources are the resources currently allocated to the container
	// (Zerops currentHardwareResource), nil when unknown.
	Resources *domain.ObservedResources `json:"resources,omitempty"`
}

// LogQuery bounds an infrastructure log request.
type LogQuery struct {
	Since     time.Time
	Limit     int
	Container string
}

// LogLine is a timestamped process or container log entry.
type LogLine struct {
	Timestamp time.Time `json:"timestamp"`
	Container string    `json:"container"`
	Stream    string    `json:"stream"`
	Message   string    `json:"message"`
}

// Op identifies a durable asynchronous infrastructure operation.
type Op struct {
	ID        string `json:"id"`
	Kind      string `json:"kind,omitempty"`
	ServiceID string `json:"serviceId,omitempty"`
	// Hostname and AttemptedResources retain only safe resource diagnostics for
	// failures observed after the request or a control-plane restart.
	Hostname           string `json:"hostname,omitempty"`
	AttemptedResources string `json:"attemptedResources,omitempty"`
	// Services maps hostnames to the service IDs an import created. They are
	// authoritative ownership evidence: the IDs come from Karakoram's own request.
	Services map[string]string `json:"services,omitempty"`
}

// OpState reports non-blocking infrastructure operation progress.
type OpState struct {
	Done    bool   `json:"done"`
	Failed  bool   `json:"failed"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	// Expired reports that the platform no longer knows the operation, so its
	// outcome is unknown. An expired import is Done (not Failed): callers
	// observe the services it may have created. Any other expired action is
	// also Failed, so callers re-issue it through their retry path.
	Expired bool `json:"expired,omitempty"`
	// ServiceIDs lists the services the operation's processes acted on, when known.
	ServiceIDs []string `json:"serviceIds,omitempty"`
}

// Backend defines the infrastructure operations required by engine providers.
type Backend interface {
	Name() string
	ProjectInfo(context.Context) (Project, error)
	ImportServices(context.Context, []ServiceSpec) (Op, error)
	GetService(context.Context, string) (*Service, error)
	ListServices(context.Context, string) ([]Service, error)
	DeleteService(context.Context, string) (Op, error)
	Deploy(context.Context, string, Deployment) (Op, error)
	SetEnvSecrets(context.Context, string, map[string]string) error
	// SetEnvVars writes non-sensitive variables, converting existing keys.
	SetEnvVars(context.Context, string, map[string]string) error
	Start(context.Context, string) (Op, error)
	Stop(context.Context, string) (Op, error)
	Restart(context.Context, string) (Op, error)
	SetResources(context.Context, string, Resources) (Op, error)
	// SetContainerCount sets horizontal scaling bounds (min/max containers)
	// while preserving the service's vertical configuration.
	SetContainerCount(ctx context.Context, hostname string, min, max int) (Op, error)
	EnsurePublicPort(context.Context, string, PublicPort) error
	ListPublicPorts(context.Context) ([]PublicPortRoute, error)
	DeletePublicPorts(context.Context, string) error
	Containers(context.Context, string) ([]Container, error)
	Logs(context.Context, string, LogQuery) ([]LogLine, error)
	OpStatus(context.Context, Op) (OpState, error)
}

// SubdomainAccess manages public HTTPS subdomain routing to a service's httpSupport ports.
// It is optional: infrastructure backends need not support public HTTPS routing.
type SubdomainAccess interface {
	SetSubdomainAccess(ctx context.Context, service string, enabled bool) (Op, error)
	SubdomainURLs(ctx context.Context, service string) ([]string, error)
}
