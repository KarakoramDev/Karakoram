// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package infra defines the backend-neutral infrastructure contract that engine
// providers, namespaces and the control plane use. Each infrastructure backend
// (Zerops today) implements Backend and renders the neutral service and
// deployment descriptions into its own platform objects; nothing outside a
// backend package may depend on platform types, manifests or error codes.
package infra

import (
	"context"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// Resources expresses CPU, RAM and disk bounds. CPUMode is CPUShared or
// CPUDedicated; backends without dedicated cores report
// Capabilities.DedicatedCPU=false. A backend without autoscaling treats the
// minimum (or StartCPU) as the reservation and the maximum as the limit.
type Resources = domain.Resources

// CPU modes accepted in Resources.CPUMode.
const (
	CPUShared    = "SHARED"
	CPUDedicated = "DEDICATED"
)

// Project describes the backend target (Zerops project, Dokploy environment,
// Nomad namespace) and the public addresses used for public TCP routes.
type Project struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	PublicIPv6 string `json:"publicIpv6,omitempty"`
	PublicIPv4 string `json:"publicIpv4,omitempty"`
}

// ServiceKind distinguishes runtime services from persistent volumes.
type ServiceKind string

const (
	// KindRuntime is a long-running service that runs the deployed artifact.
	KindRuntime ServiceKind = "runtime"
	// KindVolume is a persistent single-writer volume with local-disk
	// semantics. It has its own identity and lifecycle so it can outlive the
	// runtime that mounts it (keepVolumes, orphan cleanup).
	KindVolume ServiceKind = "volume"
)

// BaseUbuntu2404 is the only runtime base image engines use today.
const BaseUbuntu2404 = "ubuntu-24.04"

// Neutral service status vocabulary. Backends map platform states to these where
// they correspond. Other values are transitional states callers must treat as not
// ready.
const (
	StatusActive        = "ACTIVE"
	StatusRunning       = "RUNNING"
	StatusReadyToDeploy = "READY_TO_DEPLOY"
	StatusStopped       = "STOPPED"
	StatusCreating      = "CREATING"
	StatusDeleting      = "DELETING"
	StatusFailed        = "FAILED"
)

// Neutral container status vocabulary.
const (
	ContainerActive  = "ACTIVE"
	ContainerRunning = "RUNNING"
)

// ServiceSpec declares a runtime or persistent volume service.
type ServiceSpec struct {
	Hostname string      `json:"hostname"`
	Kind     ServiceKind `json:"kind"`
	// Base selects the runtime OS image (BaseUbuntu2404). Empty for volumes.
	Base string `json:"base,omitempty"`
	// Resources holds runtime CPU/RAM/root disk, or only MinDisk/MaxDisk for
	// a volume.
	Resources     Resources `json:"resources"`
	MinContainers int       `json:"minContainers"`
	MaxContainers int       `json:"maxContainers"`
	// Env holds sensitive variables (secrets). Tokens without the platform's
	// sensitive-read capability observe them masked, so they are never
	// ownership evidence by themselves.
	Env map[string]string `json:"env,omitempty"`
	// Vars holds non-sensitive variables readable by every target token.
	Vars map[string]string `json:"vars,omitempty"`
	// Volume names the volume service a runtime mounts at
	// Layout.VolumePath(Volume).
	Volume string `json:"volume,omitempty"`
}

// Service describes an observed infrastructure service.
type Service struct {
	ID            string      `json:"id"`
	Hostname      string      `json:"hostname"`
	Kind          ServiceKind `json:"kind"`
	Status        string      `json:"status"`
	Resources     Resources   `json:"resources"`
	MinContainers int         `json:"minContainers"`
	MaxContainers int         `json:"maxContainers"`
	// Ownership contains only Karakoram identity keys, never arbitrary environment
	// secrets. Values the token cannot read are omitted, never stored masked.
	Ownership map[string]string `json:"-"`
	// Masked reports that an ownership key exists but its sensitive value is
	// masked for this token, so legacy secret evidence cannot be compared.
	Masked bool `json:"-"`
}

// PublicPort declares public TCP routing and address restrictions.
type PublicPort struct {
	Port       int      `json:"port"`
	TargetPort int      `json:"targetPort"`
	Protocol   string   `json:"protocol"`
	IPType     string   `json:"ipType"`
	Allowlist  []string `json:"allowlist"`
}

// PublicPortRoute includes target-wide routes owned by any service.
type PublicPortRoute struct {
	Hostname string
	PublicPort
}

// Container describes an observed runtime container and placement hint.
type Container struct {
	ID       string `json:"id"`
	Hostname string `json:"hostname"`
	Status   string `json:"status"`
	IPv4     string `json:"ipv4"`
	IPv6     string `json:"ipv6"`
	// PlacementGroup is an authoritative host/node identity when the backend
	// has one (Capabilities.Placement); empty otherwise.
	PlacementGroup string `json:"placementGroup,omitempty"`
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

// Op is a durable handle for an asynchronous infrastructure operation. ID is
// opaque and backend-defined: callers persist it unchanged and pass the whole
// Op back to OpStatus. An empty ID means the operation already completed.
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
	// Expired reports that the backend no longer knows the operation, so its
	// outcome is unknown. An expired import is Done (not Failed): callers
	// observe the services it may have created. Any other expired action is
	// also Failed, so callers re-issue it through their retry path.
	Expired bool `json:"expired,omitempty"`
	// ServiceIDs lists the services the operation acted on, when known.
	ServiceIDs []string `json:"serviceIds,omitempty"`
}

// Backend defines the infrastructure operations required by engine providers.
// Lookups return (nil, nil) for an absent service; other failures use the
// neutral errors in errors.go (wrapped with safe detail).
type Backend interface {
	// Name returns the backend kind registered in the Registry, e.g. "zerops".
	Name() string
	Capabilities() Capabilities
	Layout() Layout
	ProjectInfo(context.Context) (Project, error)
	ImportServices(context.Context, []ServiceSpec) (Op, error)
	GetService(context.Context, string) (*Service, error)
	ListServices(context.Context, string) ([]Service, error)
	DeleteService(context.Context, string) (Op, error)
	// DeleteServiceByID deletes hostname only while it still has expectedID,
	// fencing deletion against hostname reuse. An absent service is a
	// completed no-op; a different ID is ErrConflict.
	DeleteServiceByID(ctx context.Context, hostname, expectedID string) (Op, error)
	Deploy(context.Context, string, Deployment) (Op, error)
	SetEnvSecrets(context.Context, string, map[string]string) error
	// SetEnvVars writes non-sensitive variables, converting existing keys.
	SetEnvVars(context.Context, string, map[string]string) error
	Start(context.Context, string) (Op, error)
	Stop(context.Context, string) (Op, error)
	Restart(context.Context, string) (Op, error)
	// SetResources changes vertical bounds. Zero values leave a bound as-is.
	// Disk bounds on a volume service grow it (Capabilities.DiskGrowth).
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

// SubdomainAccess manages public HTTPS routing to a service's HTTP ports
// (Port.HTTP). It is optional; backends that implement it report
// Capabilities.PublicHTTPS.
type SubdomainAccess interface {
	SetSubdomainAccess(ctx context.Context, service string, enabled bool) (Op, error)
	SubdomainURLs(ctx context.Context, service string) ([]string, error)
}

// CredentialRotator is implemented by backends whose live client can switch
// to a validated replacement credential without being rebuilt.
type CredentialRotator interface {
	SetToken(string)
}
