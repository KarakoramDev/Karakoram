// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package tspec defines the secret-bearing Temporal member contract shared by
// the control plane and the member agent. Payloads carry database passwords and
// private keys: never log them or copy them into status.
package tspec

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Engine is the agent engine name selected by KARAKORAM_ENGINE.
const Engine = "temporal"

// Component names. Server runs all four roles in one process (combined
// topology); the role components run one role each (split topology).
const (
	Server   = "server"
	History  = "history"
	Matching = "matching"
	Frontend = "frontend"
	Worker   = "worker"
	UI       = "ui"
)

// RollOrder is the upgrade and rolling-restart order (ADR 0056).
var RollOrder = []string{History, Matching, Frontend, Worker, Server, UI}

// Roles are the Temporal server services a component starts, in start order.
func Roles(component string) []string {
	switch component {
	case Server:
		return []string{History, Matching, Frontend, Worker}
	case History, Matching, Frontend, Worker:
		return []string{component}
	}
	return nil
}

// Port is the fixed per-role port pair; frontend also serves the HTTP API.
type Port struct{ GRPC, Membership int }

// Ports are identical on every member so peers never need a port map.
var Ports = map[string]Port{
	Frontend: {GRPC: 7233, Membership: 6933},
	History:  {GRPC: 7234, Membership: 6934},
	Matching: {GRPC: 7235, Membership: 6935},
	Worker:   {GRPC: 7239, Membership: 6939},
}

const (
	// FrontendHTTPPort is the frontend HTTP API port (TLS like gRPC).
	FrontendHTTPPort = 7243
	// UIPort is the Web UI HTTP port.
	UIPort = 8080
	// MetricsPort is the loopback-only Prometheus listener of temporal-server.
	MetricsPort = 9090
)

// Agent actions, served as POST /v1/temporal/<action> by the shared agent and
// called through agentapi.Client.Engine.
const (
	ActionStatus           = "status"            // {} → Status (GET also served)
	ActionNamespaces       = "namespaces"        // {} → []NamespaceStatus
	ActionEnsureNamespaces = "ensure-namespaces" // []Namespace → []NamespaceStatus; creates or updates
	ActionDeleteNamespace  = "delete-namespace"  // DeleteNamespace → {}; asynchronous deletion
)

// DeleteNamespace requests deletion of one namespace.
type DeleteNamespace struct {
	Name string `json:"name"`
}

// Spec is delivered only over the authenticated member configuration transport.
type Spec struct {
	// Component names exactly one of Server, History, Matching, Frontend, Worker, UI.
	Component string `json:"component"`
	// Version is the Temporal server version (UI: the ui-server version).
	Version string `json:"version"`
	// InstallDir is the extracted artifact root (temporal-server, temporal-sql-tool or ui-server).
	InstallDir string `json:"installDir"`
	// RestartToken changes whenever settings or sealed secrets change.
	RestartToken string `json:"restartToken"`
	// ClusterName is Temporal's currentClusterName; immutable.
	ClusterName string `json:"clusterName"`
	// HistoryShards is numHistoryShards; immutable after creation.
	HistoryShards int `json:"historyShards"`
	// Persistence is the bound PostgreSQL connection (server components only).
	Persistence *Database `json:"persistence,omitempty"`
	// TLS is the cluster PKI material for this component.
	TLS TLS `json:"tls"`
	// FrontendAddress is the private host:port of the frontend service (UI and clients).
	FrontendAddress string `json:"frontendAddress,omitempty"`
	// UI configures the Web UI component.
	UI *UISettings `json:"ui,omitempty"`
}

// Database is the bound persistence + visibility store on one PostgreSQL cluster.
type Database struct {
	Host               string `json:"host"`
	Port               int    `json:"port"`
	Database           string `json:"database"`
	VisibilityDatabase string `json:"visibilityDatabase"`
	Username           string `json:"username"`
	Password           string `json:"password"`
	// CAPEM verifies the PostgreSQL server certificate (verify-full).
	CAPEM string `json:"caPEM"`
	// MaxConns and VisibilityMaxConns bound each server process' pools.
	MaxConns           int `json:"maxConns"`
	VisibilityMaxConns int `json:"visibilityMaxConns"`
}

// TLS carries PEM material issued from the Temporal cluster CA.
type TLS struct {
	CAPEM string `json:"caPEM"`
	// Internode identity: server and client certificate for role-to-role mTLS,
	// also used by the agent for local frontend API calls and by the UI.
	InternodeCertPEM string `json:"internodeCertPEM"`
	InternodeKeyPEM  string `json:"internodeKeyPEM"`
	// InternodeServerName is the SAN every internode certificate carries.
	InternodeServerName string `json:"internodeServerName"`
	// Frontend serving certificate (frontend and server components only).
	FrontendCertPEM string `json:"frontendCertPEM,omitempty"`
	FrontendKeyPEM  string `json:"frontendKeyPEM,omitempty"`
	// FrontendServerName is the SAN clients verify (UI uses it too).
	FrontendServerName string `json:"frontendServerName,omitempty"`
	// RequireClientAuth makes the frontend demand client certificates.
	RequireClientAuth bool `json:"requireClientAuth"`
}

// UISettings configures ui-server.
type UISettings struct {
	// PublicURL is the external origin when published, else empty.
	PublicURL string `json:"publicUrl,omitempty"`
	OIDC      *OIDC  `json:"oidc,omitempty"`
}

// OIDC enables ui-server authentication.
type OIDC struct {
	Issuer       string `json:"issuer"`
	ClientID     string `json:"clientId"`
	ClientSecret string `json:"clientSecret"`
	CallbackURL  string `json:"callbackUrl"`
}

// Namespace is a desired Temporal namespace.
type Namespace struct {
	Name        string `json:"name"`
	Retention   string `json:"retention"` // Go duration, ≥ 24h
	Description string `json:"description,omitempty"`
}

// NamespaceStatus is an observed namespace.
type NamespaceStatus struct {
	Namespace
	State string `json:"state"` // Registered, Deprecated, Deleted
}

// Status is the agent's observation of its component.
type Status struct {
	Component     string `json:"component"`
	Version       string `json:"version"`
	RestartToken  string `json:"restartToken"`
	Ready         bool   `json:"ready"`
	Detail        string `json:"detail,omitempty"`
	SchemaVersion string `json:"schemaVersion,omitempty"`
	// VisibilitySchemaVersion is the SQL visibility schema version.
	VisibilitySchemaVersion string `json:"visibilitySchemaVersion,omitempty"`
}

var (
	safeVersion   = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?$`)
	safeName      = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	safeNamespace = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{1,254}$`)
	safeDatabase  = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// MinRetention is Temporal's minimum namespace retention.
const MinRetention = 24 * time.Hour

// Validate checks a namespace declaration.
func (n Namespace) Validate() error {
	if !safeNamespace.MatchString(n.Name) {
		return fmt.Errorf("namespace %q: invalid name", n.Name)
	}
	d, err := time.ParseDuration(n.Retention)
	if err != nil || d < MinRetention {
		return fmt.Errorf("namespace %q: retention must be a duration of at least 24h", n.Name)
	}
	if len(n.Description) > 1024 {
		return fmt.Errorf("namespace %q: description too long", n.Name)
	}
	return nil
}

// Validate checks the payload without echoing secret values.
func (s Spec) Validate() error {
	var errs []error
	isServer := Roles(s.Component) != nil
	if !isServer && s.Component != UI {
		return fmt.Errorf("unknown component %q", s.Component)
	}
	if !safeVersion.MatchString(s.Version) {
		errs = append(errs, errors.New("invalid version"))
	}
	if !filepath.IsAbs(s.InstallDir) || filepath.Clean(s.InstallDir) != s.InstallDir || !strings.HasPrefix(s.InstallDir, "/opt/karakoram/") {
		errs = append(errs, errors.New("installDir must be a clean path under /opt/karakoram"))
	}
	if s.RestartToken == "" {
		errs = append(errs, errors.New("restartToken is required"))
	}
	if !safeName.MatchString(s.ClusterName) {
		errs = append(errs, errors.New("invalid clusterName"))
	}
	if s.TLS.CAPEM == "" || s.TLS.InternodeCertPEM == "" || s.TLS.InternodeKeyPEM == "" || s.TLS.InternodeServerName == "" {
		errs = append(errs, errors.New("internode TLS material is required"))
	}
	if isServer {
		if s.HistoryShards < 1 || s.HistoryShards > 16384 {
			errs = append(errs, errors.New("historyShards must be 1-16384"))
		}
		if s.Persistence == nil {
			errs = append(errs, errors.New("persistence is required"))
		} else {
			errs = append(errs, s.Persistence.validate())
		}
		if slices.Contains(Roles(s.Component), Frontend) && (s.TLS.FrontendCertPEM == "" || s.TLS.FrontendKeyPEM == "" || s.TLS.FrontendServerName == "") {
			errs = append(errs, errors.New("frontend TLS material is required"))
		}
	} else {
		if s.Persistence != nil {
			errs = append(errs, errors.New("ui must not receive persistence credentials"))
		}
		if _, _, err := net.SplitHostPort(s.FrontendAddress); err != nil {
			errs = append(errs, errors.New("ui requires frontendAddress host:port"))
		}
		if s.TLS.FrontendServerName == "" {
			errs = append(errs, errors.New("ui requires frontendServerName"))
		}
		if s.UI != nil && s.UI.OIDC != nil {
			o := s.UI.OIDC
			if !strings.HasPrefix(o.Issuer, "https://") || o.ClientID == "" || o.ClientSecret == "" || !strings.HasPrefix(o.CallbackURL, "https://") {
				errs = append(errs, errors.New("ui oidc requires https issuer, clientId, clientSecret and https callbackUrl"))
			}
		}
	}
	return errors.Join(errs...)
}

func (d *Database) validate() error {
	var errs []error
	if d.Host == "" || strings.ContainsAny(d.Host, " /") || d.Port < 1 || d.Port > 65535 {
		errs = append(errs, errors.New("persistence endpoint is invalid"))
	}
	if !safeDatabase.MatchString(d.Database) || !safeDatabase.MatchString(d.VisibilityDatabase) || d.Database == d.VisibilityDatabase {
		errs = append(errs, errors.New("persistence databases must be two distinct valid names"))
	}
	if d.Username == "" || d.Password == "" {
		errs = append(errs, errors.New("persistence credentials are required"))
	}
	if d.CAPEM == "" {
		errs = append(errs, errors.New("persistence CA is required (verify-full)"))
	}
	if d.MaxConns < 1 || d.VisibilityMaxConns < 1 {
		errs = append(errs, errors.New("persistence connection limits must be positive"))
	}
	return errors.Join(errs...)
}
