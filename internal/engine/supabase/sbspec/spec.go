// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package sbspec defines the secret-bearing Supabase member contract using only
// the standard library. Payloads must never be included in logs or status.
package sbspec

import (
	"crypto/x509"
	"errors"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

const Engine = "supabase"

// Components is the native process vocabulary, in dependency order.
var Components = []string{"gateway", "auth", "rest", "realtime", "storage", "imgproxy", "meta", "studio", "functions", "pooler", "analytics", "vector"}

var safeVersion = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
var safeFunction = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Spec is delivered only over the authenticated member configuration transport.
type Spec struct {
	// Component names exactly one entry in Components.
	Component string `json:"component"`
	// Version is the pinned native component artifact version, not the PG version.
	Version string `json:"version"`
	// InstallDir is the immutable extracted artifact root under /opt/karakoram.
	InstallDir string `json:"installDir"`
	// RestartToken changes whenever service settings or sealed secrets change.
	RestartToken string `json:"restartToken"`
	// Topology is single (direct data peer) or ha (Patroni-discovered primary).
	Topology string `json:"topology"`
	// DataPeers contains private data-member host names, without ports.
	DataPeers []string `json:"dataPeers"`
	// CAPEM authenticates Patroni REST and database server certificates.
	CAPEM string `json:"caPEM"`
	// PatroniUsername authenticates primary probes.
	PatroniUsername string `json:"patroniUsername"`
	// PatroniPassword is the sealed Patroni REST password.
	PatroniPassword string `json:"patroniPassword"`
	// Database is the application database, normally postgres.
	Database string `json:"database"`
	// DatabaseTLS requires encrypted DB connections verified against CAPEM where supported.
	DatabaseTLS bool `json:"databaseTls"`
	// DatabaseIPv6 selects the backend's private address family for primary forwarding.
	DatabaseIPv6 bool `json:"databaseIPv6"`
	// DBRoles maps PostgreSQL role names to distinct sealed login passwords.
	DBRoles map[string]string `json:"dbRoles"`
	// SiteURL is the Auth application redirect origin.
	SiteURL string `json:"siteUrl"`
	// APIExternalURL is the public gateway origin used by Auth and Storage.
	APIExternalURL string `json:"apiExternalUrl"`
	// GatewayURL is the private HTTP gateway origin used by services.
	GatewayURL string `json:"gatewayUrl"`
	// JWTSecret is the legacy HS256 secret (at least 32 bytes).
	JWTSecret string `json:"jwtSecret"`
	// JWTKeys is the JSON private signing-JWK array used only by Auth.
	JWTKeys string `json:"jwtKeys,omitempty"`
	// JWKS is the private service verification set: its legacy symmetric JWK is secret.
	JWKS string `json:"jwks,omitempty"`
	// AnonKey is the configured legacy anonymous API JWT.
	AnonKey string `json:"anonKey"`
	// ServiceRoleKey is the configured privileged API JWT.
	ServiceRoleKey string `json:"serviceRoleKey"`
	// PublishableKey is the public opaque API key.
	PublishableKey string `json:"publishableKey,omitempty"`
	// SecretKey is the privileged opaque API key.
	SecretKey string `json:"secretKey,omitempty"`
	// AnonAsymmetricJWT is the gateway's internal JWT for PublishableKey.
	AnonAsymmetricJWT string `json:"anonAsymmetricJwt,omitempty"`
	// ServiceAsymmetricJWT is the internal JWT for SecretKey.
	ServiceAsymmetricJWT string `json:"serviceAsymmetricJwt,omitempty"`
	// Upstreams maps enabled components to private host:port discovery addresses.
	Upstreams map[string]string `json:"upstreams"`
	// GraphQL enables the gateway route only after its SQL wrapper is installed.
	GraphQL bool `json:"graphql"`
	// CORSOrigins optionally restricts browser origins; empty allows any origin.
	CORSOrigins []string `json:"corsOrigins,omitempty"`
	// Auth holds signup, redirect, email and SMTP policy.
	Auth Auth `json:"auth"`
	// REST holds exposed schema names and row limits.
	REST REST `json:"rest"`
	// Storage holds the registered S3 backend and frontend protocol credentials.
	Storage Storage `json:"storage"`
	// Studio holds display names, dashboard authentication and meta crypto key.
	Studio Studio `json:"studio"`
	// SecretKeyBase is the independent BEAM application signing secret.
	SecretKeyBase string `json:"secretKeyBase"`
	// RealtimeDBEncKey is exactly 16 characters for tenant credential encryption.
	RealtimeDBEncKey string `json:"realtimeDbEncKey"`
	// RealtimeAdminSecret authenticates only the raw Realtime administration API.
	// Tenant seeding and client authentication continue using JWTSecret and JWKS.
	RealtimeAdminSecret string `json:"realtimeAdminSecret"`
	// VaultEncKey is Supavisor's independent 32-character hexadecimal key.
	VaultEncKey string `json:"vaultEncKey"`
	// Pooler holds the tenant and connection ceilings.
	Pooler Pooler `json:"pooler"`
	// LogflarePublicToken authorizes log ingestion, not administration.
	LogflarePublicToken string `json:"logflarePublicToken"`
	// LogflarePrivateToken authorizes Studio's private analytics requests.
	LogflarePrivateToken string `json:"logflarePrivateToken"`
	// Functions maps function name to relative file-name/source pairs; main is reserved.
	Functions map[string]map[string]string `json:"functions"`
	// VerifyJWT controls JWT verification by the pinned Edge main worker.
	VerifyJWT bool `json:"verifyJwt"`
	// VectorSinkURL is an optional private HTTP source URL for member log batches.
	VectorSinkURL string `json:"vectorSinkUrl,omitempty"`
	// VectorSinkToken authenticates member log pushes with bearer authentication.
	VectorSinkToken string `json:"vectorSinkToken,omitempty"`
}

// Auth contains the supported GoTrue policy.
type Auth struct {
	// AdditionalRedirectURLs is an explicit redirect allowlist.
	AdditionalRedirectURLs []string `json:"additionalRedirectUrls"`
	// DisableSignup forbids new account registration.
	DisableSignup bool `json:"disableSignup"`
	// Autoconfirm enables email confirmation without SMTP delivery.
	Autoconfirm bool `json:"autoconfirm"`
	// JWTExpiry is the issued access-token lifetime in seconds.
	JWTExpiry int `json:"jwtExpiry"`
	// SMTP configures email delivery; password is never a public engine option.
	SMTP SMTP `json:"smtp"`
}

// SMTP holds the sealed mail transport configuration.
type SMTP struct {
	// Host is the SMTP server hostname.
	Host string `json:"host"`
	// Port is the SMTP transport port.
	Port int `json:"port"`
	// User is the SMTP login name.
	User string `json:"user"`
	// Password is the sealed SMTP login secret.
	Password string `json:"password"`
	// AdminEmail is the outgoing sender address.
	AdminEmail string `json:"adminEmail"`
	// SenderName is the outgoing sender display name.
	SenderName string `json:"senderName"`
}

// REST declares the PostgREST API surface.
type REST struct {
	// Schemas lists only schemas intended for HTTP exposure.
	Schemas []string `json:"schemas"`
	// MaxRows bounds each response; zero selects the upstream default of 1000.
	MaxRows int `json:"maxRows"`
}

// Storage configures stateless S3 object storage.
type Storage struct {
	// Endpoint is the registered backend HTTP(S) origin.
	Endpoint string `json:"endpoint"`
	// Bucket is the backend bucket name.
	Bucket string `json:"bucket"`
	// Region is the backend AWS signing region.
	Region string `json:"region"`
	// PathStyle selects path-style rather than virtual-hosted S3 requests.
	PathStyle bool `json:"pathStyle"`
	// TLS requires HTTPS for the backend endpoint.
	TLS bool `json:"tls"`
	// AccessKey is the sealed backend S3 access ID.
	AccessKey string `json:"accessKey"`
	// SecretKey is the sealed backend S3 secret.
	SecretKey string `json:"secretKey"`
	// TenantID isolates this cluster's object namespace.
	TenantID string `json:"tenantId"`
	// FileSizeLimit is the maximum upload size in bytes; zero selects 50 MiB.
	FileSizeLimit int64 `json:"fileSizeLimit"`
	// ImageTransformation enables calls to the enabled imgproxy upstream.
	ImageTransformation bool `json:"imageTransformation"`
	// ProtocolAccessKey is the independent Storage S3 frontend access ID.
	ProtocolAccessKey string `json:"protocolAccessKey"`
	// ProtocolSecretKey is the independent Storage S3 frontend signing secret.
	ProtocolSecretKey string `json:"protocolSecretKey"`
}

// Studio configures the dashboard; basic-auth is enforced by the gateway.
type Studio struct {
	// Organization is the initial organization display name.
	Organization string `json:"organization"`
	// Project is the initial project display name.
	Project string `json:"project"`
	// DashboardUsername is the gateway HTTP basic-auth username.
	DashboardUsername string `json:"dashboardUsername"`
	// DashboardPassword is the sealed gateway HTTP basic-auth password.
	DashboardPassword string `json:"dashboardPassword"`
	// MetaCryptoKey encrypts postgres-meta connection credentials.
	MetaCryptoKey string `json:"metaCryptoKey"`
}

// Pooler configures the self-hosted Supavisor tenant.
type Pooler struct {
	// TenantID selects the stable external tenant name.
	TenantID string `json:"tenantId"`
	// DefaultPoolSize bounds backend connections per user.
	DefaultPoolSize int `json:"defaultPoolSize"`
	// MaxClientConnections bounds frontend clients.
	MaxClientConnections int `json:"maxClientConnections"`
	// DBPoolSize bounds the internal metadata connection pool.
	DBPoolSize int `json:"dbPoolSize"`
}

// NeedsDatabase reports whether a component needs the local primary forwarder.
func NeedsDatabase(component string) bool {
	switch component {
	case "auth", "rest", "realtime", "storage", "meta", "studio", "functions", "pooler", "analytics":
		return true
	}
	return false
}

// Validate checks transport and filesystem safety before any process is changed.
func (s Spec) Validate() error {
	known := false
	for _, c := range Components {
		known = known || s.Component == c
	}
	if !known {
		return errors.New("unknown Supabase component")
	}
	if !safeVersion.MatchString(s.Version) || s.RestartToken == "" {
		return errors.New("component version and restart token are required")
	}
	if filepath.Dir(s.InstallDir) != "/opt/karakoram" || !strings.HasSuffix(filepath.Base(s.InstallDir), "-"+s.Version) || filepath.Clean(s.InstallDir) != s.InstallDir {
		return errors.New("invalid native artifact install directory")
	}
	if NeedsDatabase(s.Component) {
		if s.Topology != "single" && s.Topology != "ha" {
			return errors.New("invalid data topology")
		}
		if len(s.DataPeers) == 0 || (s.Topology == "single" && len(s.DataPeers) != 1) {
			return errors.New("invalid data peer count")
		}
		for _, p := range s.DataPeers {
			if p == "" || strings.ContainsAny(p, "/ :\r\n\t") && net.ParseIP(p) == nil {
				return errors.New("invalid data peer hostname")
			}
		}
		if s.Topology == "ha" && (s.CAPEM == "" || s.PatroniUsername == "" || s.PatroniPassword == "") {
			return errors.New("trust and credentials for Patroni TLS are required")
		}
		if s.Database == "" {
			return errors.New("database is required")
		}
		if (s.DatabaseTLS || s.Topology == "ha") && !x509.NewCertPool().AppendCertsFromPEM([]byte(s.CAPEM)) {
			return errors.New("valid database CA PEM is required")
		}
	}
	switch s.Component {
	case "auth", "rest", "realtime", "storage", "studio", "functions", "pooler":
		if len(s.JWTSecret) < 32 {
			return errors.New("JWT secret is required")
		}
	}
	switch s.Component {
	case "gateway", "realtime", "storage", "studio", "functions":
		if s.AnonKey == "" {
			return errors.New("anonymous API key is required")
		}
	}
	switch s.Component {
	case "gateway", "storage", "meta", "studio", "functions":
		if s.ServiceRoleKey == "" {
			return errors.New("service API key is required")
		}
	}
	if err := ValidateCORSOrigins(s.CORSOrigins); err != nil {
		return err
	}
	if s.VectorSinkURL != "" {
		u, e := url.Parse(s.VectorSinkURL)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
			return errors.New("invalid vector sink URL")
		}
	}
	if len(s.Functions) > 100 {
		return errors.New("too many functions")
	}
	for name, files := range s.Functions {
		total := 0
		if name == "main" || !safeFunction.MatchString(name) {
			return errors.New("invalid function name")
		}
		for _, source := range files {
			total += len(source)
		}
		if total > 1<<20 {
			return errors.New("function sources exceed size limit")
		}
		for f, source := range files {
			if !filepath.IsLocal(f) || len(source) > 1<<20 {
				return errors.New("invalid function file")
			}
		}
	}
	return nil
}

// GatewayUpstreams lists the components routed through Envoy. Other components
// may appear in Spec.Upstreams for service-to-service discovery only.
var GatewayUpstreams = []string{"auth", "rest", "realtime", "storage", "meta", "studio", "functions"}

// Status is the credential-free component readiness observation.
type Status struct {
	Component string `json:"component"` // Component is the supervised service name.
	Version   string `json:"version"`   // Version is the native artifact version.
	Ready     bool   `json:"ready"`     // Ready reports the component's own health endpoint.
	Message   string `json:"message"`   // Message contains no upstream response body or secrets.
	// GatewayUpstreamHosts reports resolved Envoy hosts, not application health.
	// Missing observations (including older agents) do not satisfy stack readiness.
	GatewayUpstreamHosts map[string]int `json:"gatewayUpstreamHosts,omitempty"`
}

// ValidateCORSOrigins accepts exact HTTP(S) origins, or a sole wildcard.
func ValidateCORSOrigins(origins []string) error {
	for _, origin := range origins {
		if origin == "*" && len(origins) == 1 {
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || origin != u.Scheme+"://"+u.Host {
			return errors.New("CORS origins must be HTTP(S) origins without credentials, paths, queries or fragments")
		}
	}
	return nil
}
