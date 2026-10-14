// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package namespaces manages project credentials and enrolled project relays.
package namespaces

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/KarakoramDev/Karakoram/internal/version"
)

var ErrInvalid = errors.New("invalid namespace")
var ErrConflict = store.ErrConflict
var ErrNotFound = store.ErrNotFound

type Backend interface {
	infra.Backend
	SetToken(string)
}
type EventRecorder interface {
	Record(context.Context, domain.Event) error
}
type Options struct {
	Store                                  *store.Store
	DefaultBackend                         Backend
	Zerops                                 zerops.Options
	PublicURL, AdvertiseURL, RelayHostname string
	Events                                 EventRecorder
	Logger                                 *slog.Logger
	Artifact                               engine.ArtifactSource
	ExecutableHash                         string
	// Validate returns an isolated client; it must verify identity and service permissions.
	Validate   func(context.Context, zerops.Options) (Backend, infra.Project, error)
	NewBackend func(zerops.Options) (Backend, error)
}
type sessionNotice struct {
	ns    string
	event relay.SessionEvent
}
type Registry struct {
	o        Options
	Hub      *relay.Hub
	pki      *store.RelayPKI
	mu       sync.RWMutex
	backends map[string]Backend
	locks    map[string]*sync.Mutex
	createMu sync.Mutex
	notices  chan sessionNotice
	wake     chan struct{}
	refused  map[string]time.Time
}

var relayHostnamePattern = regexp.MustCompile(`^[a-z][a-z0-9]{0,24}$`)

// RelayHostname preserves the usual name while bounding long control-plane names.
func RelayHostname(hostname string) string {
	name := hostname + "relay"
	if len(name) <= 25 {
		return name
	}
	hash := sha256.Sum256([]byte(hostname))
	return hostname[:16] + hex.EncodeToString(hash[:2]) + "relay"
}

var _ engine.Namespaces = (*Registry)(nil)

func New(ctx context.Context, o Options) (*Registry, error) {
	if o.Store == nil || o.DefaultBackend == nil {
		return nil, errors.New("namespace store and default backend are required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Artifact == nil {
		o.Artifact = engine.ExecutableArtifact{}
	}
	if o.Validate == nil {
		o.Validate = func(ctx context.Context, z zerops.Options) (Backend, infra.Project, error) {
			return zerops.ValidateProject(ctx, z)
		}
	}
	if o.NewBackend == nil {
		o.NewBackend = func(z zerops.Options) (Backend, error) { return zerops.New(z) }
	}
	if o.ExecutableHash == "" {
		path, err := os.Executable()
		if err != nil {
			return nil, err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return nil, err
		}
		o.ExecutableHash = hex.EncodeToString(h.Sum(nil))
	}
	r := &Registry{o: o, backends: map[string]Backend{domain.DefaultNamespace: o.DefaultBackend}, locks: map[string]*sync.Mutex{}, notices: make(chan sessionNotice, 128), wake: make(chan struct{}, 1), refused: map[string]time.Time{}}
	p, err := o.Store.GetRelayPKI(ctx)
	if errors.Is(err, store.ErrNotFound) {
		p = &store.RelayPKI{}
		p.CACert, p.CAKey, err = relay.GenerateCA(time.Now())
		if err == nil {
			p.HubCert, p.HubKey, err = relay.IssueHubCertificate(p.CACert, p.CAKey, time.Now())
		}
		if err == nil {
			err = o.Store.PutRelayPKI(ctx, p)
		}
	}
	if err != nil {
		return nil, err
	}
	leaf, err := relay.ParseCertificatePEM(p.HubCert)
	if err != nil {
		return nil, err
	}
	if relay.NeedsRenewal(leaf, time.Now()) {
		p.HubCert, p.HubKey, err = relay.IssueHubCertificate(p.CACert, p.CAKey, time.Now())
		if err != nil {
			return nil, err
		}
		if err = o.Store.PutRelayPKI(ctx, p); err != nil {
			return nil, err
		}
	}
	r.pki = p
	cert, err := tls.X509KeyPair(p.HubCert, p.HubKey)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(p.CACert) {
		return nil, errors.New("invalid relay CA")
	}
	r.Hub = relay.NewHub(relay.HubOptions{Certificate: cert, ClientCAs: pool, Authorize: r.authorizeStored, Logger: o.Logger, OnSession: func(ns string, e relay.SessionEvent) {
		select {
		case r.notices <- sessionNotice{ns, e}:
		default:
		}
	}})
	now := time.Now().UTC()
	n, err := o.Store.GetNamespaceForUpdate(ctx, domain.DefaultNamespace)
	createDefault := errors.Is(err, store.ErrNotFound)
	if errors.Is(err, store.ErrNotFound) {
		n = &domain.Namespace{Name: domain.DefaultNamespace, CreatedAt: now}
	} else if err != nil {
		r.Hub.Close()
		return nil, err
	}
	n.ProjectID = o.Zerops.ProjectID
	n.Default = true
	n.Token.Source = "env"
	n.Relay.Phase = "NotRequired"
	n.UpdatedAt = now
	project, validation := o.DefaultBackend.ProjectInfo(ctx)
	applyTokenValidation(n, project, validation, now)
	updatePhase(n)
	if createDefault {
		err = o.Store.CreateNamespace(ctx, n)
	} else {
		err = o.Store.UpdateNamespace(ctx, n)
	}
	if errors.Is(err, store.ErrStale) {
		o.Logger.Debug("Dropping stale namespace startup state", "namespace", n.Name)
	} else if err != nil {
		r.Hub.Close()
		return nil, err
	}
	all, err := o.Store.ListNamespacesForUpdate(ctx)
	if err != nil {
		r.Hub.Close()
		return nil, err
	}
	for _, n := range all {
		if n.Default {
			continue
		}
		n.Relay.DesiredVersion = version.Version
		if e := o.Store.UpdateNamespace(ctx, &n); e != nil {
			if errors.Is(e, store.ErrStale) {
				o.Logger.Debug("Dropping stale namespace startup state", "namespace", n.Name)
				continue
			}
			r.Hub.Close()
			return nil, e
		}
		if n.Phase != "Deleting" {
			r.enroll(&n)
		}
		token, e := o.Store.GetNamespaceTokenForUpdate(ctx, n.Name)
		z := o.Zerops
		z.Token = token
		z.ProjectID = n.ProjectID
		var b Backend
		if e == nil {
			b, e = o.NewBackend(z)
		}
		if e != nil {
			n.Token.Valid = false
			n.Token.LastError = "Stored project credentials are unavailable"
			updatePhase(&n)
			if e = o.Store.UpdateNamespace(ctx, &n); e != nil {
				if errors.Is(e, store.ErrStale) {
					o.Logger.Debug("Dropping stale namespace startup state", "namespace", n.Name)
					continue
				}
				r.Hub.Close()
				return nil, e
			}
			continue
		}
		r.backends[n.Name] = b
	}
	return r, nil
}
func (r *Registry) lock(name string) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.locks[name]
	if m == nil {
		m = &sync.Mutex{}
		r.locks[name] = m
	}
	return m
}
func (r *Registry) backend(name string) Backend {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.backends[name]
}
func (r *Registry) event(ctx context.Context, n *domain.Namespace, kind, message string) {
	if r.o.Events != nil {
		if err := r.o.Events.Record(ctx, domain.Event{Namespace: n.Name, Resource: "namespaces", Target: n.Name, Actor: "system", Type: "Normal", Reason: kind, Message: message, CreatedAt: time.Now().UTC()}); err != nil {
			r.o.Logger.Error("Namespace event persistence failed", "namespace", n.Name, "error", err)
		}
	}
}
func (r *Registry) save(ctx context.Context, n *domain.Namespace) error {
	n.UpdatedAt = time.Now().UTC()
	err := r.o.Store.UpdateNamespace(ctx, n)
	if errors.Is(err, store.ErrStale) {
		r.o.Logger.Debug("Dropping stale namespace state", "namespace", n.Name)
	}
	if err == nil && !n.Default {
		if n.Phase == "Deleting" {
			r.Hub.Revoke(n.Name)
		} else {
			r.enroll(n)
		}
	}
	return err
}
func (r *Registry) trigger() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// A failed API request is not evidence that a previously accepted token expired.
func applyTokenValidation(n *domain.Namespace, project infra.Project, validation error, now time.Time) {
	n.Token.LastValidatedAt = now
	if validation == nil && project.ID != n.ProjectID {
		validation = &zerops.APIError{Code: "accessDenied", Message: "Authenticated project does not match the configured project"}
	}
	switch {
	case validation == nil:
		n.Token.Valid = true
		n.Token.LastError = ""
		n.ProjectName = project.Name
	case zerops.IsAuthenticationError(validation):
		n.Token.Valid = false
		n.Token.LastError = "Project token access rejected: " + zerops.SafeErrorDetail(validation)
	default:
		n.Token.LastError = temporaryPlatformError(validation)
	}
}

func temporaryPlatformError(err error) string {
	return "Zerops API temporarily unavailable: " + zerops.SafeErrorDetail(err) + "; retrying"
}

func updatePhase(n *domain.Namespace) {
	if n.Phase == "Deleting" {
		return
	}
	if !n.Token.Valid {
		n.Phase = "Error"
	} else if n.Default || n.Relay.Connected {
		n.Phase = "Ready"
	} else if n.Relay.Phase == "Error" {
		n.Phase = "Error"
	} else {
		n.Phase = "Pending"
	}
}
func (r *Registry) List(ctx context.Context) ([]domain.Namespace, error) {
	return r.o.Store.ListNamespaces(ctx)
}
func (r *Registry) Get(ctx context.Context, name string) (*domain.Namespace, error) {
	return r.o.Store.GetNamespace(ctx, name)
}
func (r *Registry) Create(ctx context.Context, name, projectID, token, description string) (*domain.Namespace, error) {
	if name == domain.DefaultNamespace || domain.ValidateNamespaceName(name) != nil || projectID == "" || token == "" {
		return nil, fmt.Errorf("%w: name, project and token are required; default is reserved", ErrInvalid)
	}
	if !relayHostnamePattern.MatchString(r.o.RelayHostname) || r.o.RelayHostname == "core" {
		return nil, fmt.Errorf("%w: invalid relay hostname", ErrInvalid)
	}
	r.createMu.Lock()
	defer r.createMu.Unlock()
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	all, err := r.o.Store.ListNamespacesForUpdate(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range all {
		if n.Name == name {
			return nil, fmt.Errorf("%w: namespace already exists", ErrConflict)
		}
		if n.ProjectID == projectID {
			return nil, fmt.Errorf("%w: project is already registered", ErrInvalid)
		}
	}
	z := r.o.Zerops
	z.ProjectID = projectID
	z.Token = token
	b, p, err := r.o.Validate(ctx, z)
	if err != nil {
		if zerops.IsAuthenticationError(err) {
			return nil, fmt.Errorf("%w: %s", ErrInvalid, zerops.SafeErrorDetail(err))
		}
		return nil, errors.New(temporaryPlatformError(err))
	}
	service, err := b.GetService(ctx, r.o.RelayHostname)
	if err != nil {
		return nil, errors.New("cannot inspect relay hostname")
	}
	if service != nil {
		return nil, fmt.Errorf("%w: relay hostname is already used by a service this namespace does not own", ErrInvalid)
	}
	now := time.Now().UTC()
	marker := make([]byte, 32)
	if _, err = rand.Read(marker); err != nil {
		return nil, err
	}
	n := &domain.Namespace{Name: name, Description: description, ProjectID: projectID, ProjectName: p.Name, Phase: "Pending", CreatedAt: now, UpdatedAt: now, Token: domain.NamespaceTokenStatus{Source: "stored", Valid: true, LastValidatedAt: now}, Relay: domain.NamespaceRelayStatus{Hostname: r.o.RelayHostname, Phase: "Pending", DesiredVersion: version.Version}, OwnershipMarker: hex.EncodeToString(marker)}
	key, err := r.issue(n)
	if err != nil {
		return nil, err
	}
	if err = r.o.Store.CreateNamespaceCredentials(ctx, n, token, key); err != nil {
		return nil, err
	}
	r.enroll(n)
	r.mu.Lock()
	r.backends[name] = b
	r.mu.Unlock()
	r.event(ctx, n, "RelayEnrolled", "Relay credentials enrolled")
	r.trigger()
	return n, nil
}
func (r *Registry) Update(ctx context.Context, name, description string) (*domain.Namespace, error) {
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.Phase == "Deleting" {
		return nil, ErrConflict
	}
	n.Description = description
	err = r.save(ctx, n)
	return n, err
}
func (r *Registry) RotateToken(ctx context.Context, name, token string) (*domain.Namespace, error) {
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.Default {
		return nil, fmt.Errorf("%w: default token is environment-managed; update the service secret and restart", ErrConflict)
	}
	z := r.o.Zerops
	z.Token = token
	z.ProjectID = n.ProjectID
	replacement, p, err := r.o.Validate(ctx, z)
	if err != nil {
		if zerops.IsAuthenticationError(err) {
			return nil, fmt.Errorf("%w: %s", ErrInvalid, zerops.SafeErrorDetail(err))
		}
		return nil, errors.New(temporaryPlatformError(err))
	}
	now := time.Now().UTC()
	n.Token.Valid = true
	n.Token.LastError = ""
	n.Token.LastValidatedAt = now
	n.Token.RotatedAt = now
	n.ProjectName = p.Name
	n.UpdatedAt = now
	updatePhase(n)
	if err = r.o.Store.UpdateNamespaceCredentials(ctx, n, token, nil); err != nil {
		if errors.Is(err, store.ErrStale) {
			r.o.Logger.Debug("Dropping stale namespace credentials", "namespace", n.Name)
		}
		return nil, err
	}
	if b := r.backend(name); b != nil {
		b.SetToken(token)
	} else {
		r.mu.Lock()
		r.backends[name] = replacement
		r.mu.Unlock()
	}
	r.trigger()
	return n, nil
}
func (r *Registry) Validate(ctx context.Context, name string) (*domain.Namespace, error) {
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.Phase == "Deleting" {
		return nil, ErrConflict
	}
	z := r.o.Zerops
	z.ProjectID = n.ProjectID
	if !n.Default {
		z.Token, err = r.o.Store.GetNamespaceTokenForUpdate(ctx, name)
		if err != nil {
			return nil, err
		}
	}
	_, p, validation := r.o.Validate(ctx, z)
	applyTokenValidation(n, p, validation, time.Now().UTC())
	updatePhase(n)
	if err = r.save(ctx, n); err != nil {
		return nil, err
	}
	return n, nil
}
func (r *Registry) Delete(ctx context.Context, name string) error {
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, name)
	if err != nil {
		return err
	}
	if n.Default {
		return fmt.Errorf("%w: default namespace cannot be deleted", ErrConflict)
	}
	if err = r.o.Store.BeginNamespaceDeletion(ctx, n); err != nil {
		if errors.Is(err, store.ErrStale) {
			r.o.Logger.Debug("Dropping stale namespace deletion intent", "namespace", name)
		}
		return err
	}
	r.Hub.Revoke(name)
	if n.Phase != "Deleting" {
		n.Phase = "Deleting"
	}
	r.trigger()
	return nil
}
func (r *Registry) Backend(ns string) (infra.Backend, error) {
	if ns == "" {
		ns = domain.DefaultNamespace
	}
	n, err := r.Get(context.Background(), ns)
	if err != nil {
		return nil, fmt.Errorf("%w: namespace %s", err, ns)
	}
	if n.Phase == "Deleting" || !n.Token.Valid {
		return nil, fmt.Errorf("namespace %s is unavailable: token invalid or deletion in progress", ns)
	}
	b := r.backend(ns)
	if b == nil {
		return nil, ErrNotFound
	}
	return b, nil
}

type hubDialer struct {
	hub *relay.Hub
	ns  string
}

func (d hubDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return d.hub.DialContext(ctx, d.ns, network, addr)
}
func (r *Registry) Dialer(ns string) engine.ContextDialer {
	if ns == "" || ns == domain.DefaultNamespace {
		return nil
	}
	return hubDialer{r.Hub, ns}
}
func (r *Registry) AgentURL(ns string) string {
	if ns == "" || ns == domain.DefaultNamespace {
		return r.o.AdvertiseURL
	}
	n, err := r.Get(context.Background(), ns)
	if err != nil {
		return ""
	}
	return "https://" + n.Relay.Hostname + ":9443"
}
func (r *Registry) Authorize(ctx context.Context, ns string, cert *x509.Certificate) error {
	if err := r.authorizeStored(ctx, ns, cert); err != nil {
		return err
	}
	if !r.Hub.AcceptsSerial(ns, cert.SerialNumber.String()) {
		return errors.New("relay certificate is not enrolled")
	}
	return nil
}
func (r *Registry) authorizeStored(ctx context.Context, ns string, cert *x509.Certificate) error {
	n, err := r.Get(ctx, ns)
	if err != nil || n.Default || n.Phase == "Deleting" || cert == nil {
		return errors.New("relay namespace is not enrolled")
	}
	if !acceptsSerial(n, cert.SerialNumber.String()) {
		return errors.New("relay certificate is not enrolled")
	}
	return nil
}

func acceptsSerial(n *domain.Namespace, serial string) bool {
	return serial == n.ClientSerial || (serial == n.PreviousClientSerial && time.Now().Before(n.PreviousCertNotAfter))
}

func (r *Registry) enroll(n *domain.Namespace) {
	r.Hub.Enroll(n.Name, n.ClientSerial, n.PreviousClientSerial, n.PreviousCertNotAfter)
}

func (r *Registry) renewHubCertificate(ctx context.Context, now time.Time) error {
	leaf, err := relay.ParseCertificatePEM(r.pki.HubCert)
	if err != nil || !relay.NeedsRenewal(leaf, now) {
		return err
	}
	next := *r.pki
	next.HubCert, next.HubKey, err = relay.IssueHubCertificate(next.CACert, next.CAKey, now)
	if err != nil {
		return err
	}
	pair, err := tls.X509KeyPair(next.HubCert, next.HubKey)
	if err != nil {
		return err
	}
	if err = r.o.Store.PutRelayPKI(ctx, &next); err != nil {
		return err
	}
	r.pki.HubCert, r.pki.HubKey = next.HubCert, next.HubKey
	r.Hub.SetCertificate(pair)
	if r.o.Events != nil {
		if err = r.o.Events.Record(ctx, domain.Event{Resource: "namespaces", Target: "relay-hub", Actor: "system", Type: "Normal", Reason: "RelayHubCertificateRenewed", Message: "Relay hub certificate renewed", CreatedAt: now.UTC()}); err != nil {
			return err
		}
	}
	return nil
}
func owned(n *domain.Namespace, s *infra.Service) bool {
	return s != nil && n.OwnershipMarker != "" && subtle.ConstantTimeCompare([]byte(s.Ownership["KARAKORAM_RELAY_OWNER"]), []byte(n.OwnershipMarker)) == 1
}
func (r *Registry) issue(n *domain.Namespace) ([]byte, error) {
	cert, key, err := relay.IssueClientCertificate(r.pki.CACert, r.pki.CAKey, n.Name, time.Now())
	if err != nil {
		return nil, err
	}
	leaf, err := relay.ParseCertificatePEM(cert)
	if err != nil {
		return nil, err
	}
	n.PreviousClientSerial = n.ClientSerial
	n.PreviousCertNotAfter = n.Relay.CertNotAfter
	n.ClientCert = cert
	n.ClientSerial = leaf.SerialNumber.String()
	n.Relay.CertNotAfter = leaf.NotAfter
	n.SecretsPending = true
	return key, nil
}
func (r *Registry) Close() error { return r.Hub.Close() }
