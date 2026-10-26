// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package namespaces manages project credentials and enrolled project relays.
// Relay ownership uses a recorded service identity or a non-sensitive marker;
// enrollment certificates and keys remain sensitive variables.
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
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/relay"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/KarakoramDev/Karakoram/internal/version"
)

var ErrInvalid = errors.New("invalid namespace")
var ErrConflict = store.ErrConflict
var ErrNotFound = store.ErrNotFound

// PlatformUnavailableError exposes a safe validation diagnostic without
// misclassifying a platform failure as invalid namespace credentials.
type PlatformUnavailableError struct {
	cause       error
	displayName string
}

func (e *PlatformUnavailableError) Error() string {
	return temporaryPlatformError(e.displayName, e.cause)
}

type Backend = infra.Backend
type EventRecorder interface {
	Record(context.Context, domain.Event) error
}
type Options struct {
	Store                                  *store.Store
	DefaultBackend                         Backend
	Backends                               *infra.Registry
	DefaultTarget                          infra.Target
	PublicURL, AdvertiseURL, RelayHostname string
	Events                                 EventRecorder
	Logger                                 *slog.Logger
	Artifact                               engine.ArtifactSource
	ExecutableHash                         string
	RelayTargets                           func(*domain.Cluster) []string
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
	if o.Store == nil || o.DefaultBackend == nil || o.Backends == nil {
		return nil, errors.New("namespace store, backend registry and default backend are required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Artifact == nil {
		o.Artifact = engine.ExecutableArtifact{}
	}
	if o.DefaultTarget.Kind == "" {
		o.DefaultTarget.Kind = "zerops"
	}
	if _, err := o.Backends.Get(o.DefaultTarget.Kind); err != nil {
		return nil, err
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
	r.Hub = relay.NewHub(relay.HubOptions{Certificate: cert, ClientCAs: pool, Authorize: r.authorizeStored, AuthorizeInstance: r.authorizeInstance, Logger: o.Logger, OnSession: func(ns string, e relay.SessionEvent) {
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
	n.ProjectID = o.DefaultTarget.ID
	n.Backend = o.DefaultTarget.Kind
	n.Config = cloneConfig(o.DefaultTarget.Config)
	n.Default = true
	n.Token.Source = "env"
	n.Relay.Phase = "NotRequired"
	n.UpdatedAt = now
	project, validation := o.DefaultBackend.ProjectInfo(ctx)
	applyTokenValidation(n, project, validation, now, r.displayName(n.Backend))
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
		if r.requiresRelay(n.Backend) {
			n.Relay.DesiredVersion = version.Version
		} else {
			n.Relay.Phase = "NotRequired"
		}
		if e := o.Store.UpdateNamespace(ctx, &n); e != nil {
			if errors.Is(e, store.ErrStale) {
				o.Logger.Debug("Dropping stale namespace startup state", "namespace", n.Name)
				continue
			}
			r.Hub.Close()
			return nil, e
		}
		if n.Phase != "Deleting" && r.requiresRelay(n.Backend) {
			r.enroll(&n)
		}
		token, e := o.Store.GetNamespaceTokenForUpdate(ctx, n.Name)
		target := r.target(&n, token)
		var b Backend
		if e == nil {
			b, e = o.Backends.New(target)
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

// lockContext preserves lifecycle serialization without making cancelled API
// requests wait for a backend operation holding the namespace lock.
func lockContext(ctx context.Context, m *sync.Mutex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.TryLock() {
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			if m.TryLock() {
				return nil
			}
		}
	}
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
	if err == nil && !n.Default && r.requiresRelay(n.Backend) {
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
func applyTokenValidation(n *domain.Namespace, project infra.Project, validation error, now time.Time, displayName string) {
	n.Token.LastValidatedAt = now
	if validation == nil && project.ID != n.ProjectID {
		validation = &credentialError{message: "Authenticated project does not match the configured project"}
	}
	switch {
	case validation == nil:
		n.Token.Valid = true
		n.Token.LastError = ""
		n.ProjectName = project.Name
	case errors.Is(validation, infra.ErrUnauthorized):
		n.Token.Valid = false
		n.Token.LastError = "Project token access rejected: " + infra.SafeDetail(validation)
	case errors.Is(validation, infra.ErrConfiguration):
		n.Token.LastError = infra.SafeDetail(validation)
	default:
		n.Token.LastError = temporaryPlatformError(displayName, validation)
	}
}

func temporaryPlatformError(displayName string, err error) string {
	return displayName + " API temporarily unavailable: " + infra.SafeDetail(err) + "; retrying"
}

func updatePhase(n *domain.Namespace) {
	if n.Phase == "Deleting" {
		return
	}
	if !n.Token.Valid {
		n.Phase = "Error"
	} else if n.Default || n.Relay.Phase == "NotRequired" || n.Relay.Connected {
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
func (r *Registry) Create(ctx context.Context, name, projectID, token, description, kind string, config map[string]string) (*domain.Namespace, error) {
	if kind == "" {
		kind = "zerops"
	}
	factory, err := r.o.Backends.Get(kind)
	if err != nil {
		return nil, err
	}
	info := factory.Info()
	target := r.target(&domain.Namespace{Backend: kind, ProjectID: projectID, Config: config}, token)
	if name == "" {
		e := domain.InvalidField("name", domain.ReasonRequired, "name is required")
		e.Err = ErrInvalid
		return nil, e
	}
	if err := domain.ValidateNamespaceName(name); err != nil {
		e := domain.InvalidField("name", domain.ReasonInvalid, "%s", err)
		e.Err = ErrInvalid
		return nil, e
	}
	if name == domain.DefaultNamespace {
		return nil, domain.Conflict("namespace", name, domain.ReasonAlreadyExists, ErrConflict, "namespace %q already exists and is reserved", name)
	}
	for _, field := range []struct{ name, value string }{{"projectId", projectID}, {"token", token}} {
		if strings.TrimSpace(field.value) == "" {
			e := domain.InvalidField(field.name, domain.ReasonRequired, "%s is required", field.name)
			e.Err = ErrInvalid
			return nil, e
		}
	}
	if info.Relay && (!relayHostnamePattern.MatchString(r.o.RelayHostname) || r.o.RelayHostname == "core") {
		return nil, fmt.Errorf("%w: invalid relay hostname", ErrInvalid)
	}
	if err := lockContext(ctx, &r.createMu); err != nil {
		return nil, err
	}
	defer r.createMu.Unlock()
	m := r.lock(name)
	if err := lockContext(ctx, m); err != nil {
		return nil, err
	}
	defer m.Unlock()
	all, err := r.o.Store.ListNamespacesForUpdate(ctx)
	if err != nil {
		return nil, err
	}
	for _, n := range all {
		if n.Name == name {
			return nil, domain.Conflict("namespace", name, domain.ReasonAlreadyExists, ErrConflict, "namespace %q already exists", name)
		}
		if n.Backend == kind && n.ProjectID == projectID {
			return nil, fmt.Errorf("%w: project is already registered", ErrInvalid)
		}
	}
	b, p, err := r.o.Backends.Validate(ctx, target)
	if err != nil {
		if errors.Is(err, infra.ErrConfiguration) {
			return nil, fmt.Errorf("%w: %s", ErrInvalid, infra.SafeDetail(err))
		}
		if errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrUnsupported) {
			return nil, err
		}
		if errors.Is(err, infra.ErrUnauthorized) {
			return nil, fmt.Errorf("%w: %s", ErrInvalid, infra.SafeDetail(err))
		}
		return nil, &PlatformUnavailableError{cause: err, displayName: info.DisplayName}
	}
	if info.Relay {
		service, err := b.GetService(ctx, r.o.RelayHostname)
		if err != nil {
			if errors.Is(err, infra.ErrConfiguration) {
				return nil, fmt.Errorf("%w: %s", ErrInvalid, infra.SafeDetail(err))
			}
			if errors.Is(err, infra.ErrUnauthorized) {
				return nil, fmt.Errorf("%w: cannot inspect relay hostname; check the target token's relay service/record read permissions: %s", ErrInvalid, infra.SafeDetail(err))
			}
			if errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrUnsupported) {
				return nil, err
			}
			return nil, &PlatformUnavailableError{cause: err, displayName: info.DisplayName}
		}
		if service != nil {
			return nil, fmt.Errorf("%w: relay hostname is already used by a service this namespace does not own", ErrInvalid)
		}
	}
	now := time.Now().UTC()
	marker := make([]byte, 32)
	if _, err = rand.Read(marker); err != nil {
		return nil, err
	}
	n := &domain.Namespace{Name: name, Description: description, ProjectID: projectID, ProjectName: p.Name, Phase: "Pending", CreatedAt: now, UpdatedAt: now, Token: domain.NamespaceTokenStatus{Source: "stored", Valid: true, LastValidatedAt: now}, Relay: domain.NamespaceRelayStatus{Hostname: r.o.RelayHostname, Phase: "Pending", DesiredVersion: version.Version}, OwnershipMarker: hex.EncodeToString(marker)}
	n.Backend, n.Config = kind, target.Config
	var key []byte
	if info.Relay {
		key, err = r.issue(n)
		if err != nil {
			return nil, err
		}
	} else {
		n.Relay = domain.NamespaceRelayStatus{Phase: "NotRequired"}
		updatePhase(n)
	}
	if err = r.o.Store.CreateNamespaceCredentials(ctx, n, token, key); err != nil {
		return nil, err
	}
	if info.Relay {
		r.enroll(n)
	}
	r.mu.Lock()
	r.backends[name] = b
	r.mu.Unlock()
	if info.Relay {
		r.event(ctx, n, "RelayEnrolled", "Relay credentials enrolled")
	}
	r.trigger()
	return n, nil
}
func (r *Registry) Update(ctx context.Context, name, description string) (*domain.Namespace, error) {
	m := r.lock(name)
	if err := lockContext(ctx, m); err != nil {
		return nil, err
	}
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
	if err := lockContext(ctx, m); err != nil {
		return nil, err
	}
	defer m.Unlock()
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.Default {
		return nil, fmt.Errorf("%w: default token is environment-managed; update the service secret and restart", ErrConflict)
	}
	replacement, p, err := r.o.Backends.Validate(ctx, r.target(n, token))
	if err != nil {
		if errors.Is(err, infra.ErrConfiguration) {
			return nil, fmt.Errorf("%w: %s", ErrInvalid, infra.SafeDetail(err))
		}
		if errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrUnsupported) {
			return nil, err
		}
		if errors.Is(err, infra.ErrUnauthorized) {
			return nil, fmt.Errorf("%w: %s", ErrInvalid, infra.SafeDetail(err))
		}
		return nil, &PlatformUnavailableError{cause: err, displayName: r.displayName(n.Backend)}
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
	if b, ok := r.backend(name).(infra.CredentialRotator); ok {
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
	if err := lockContext(ctx, m); err != nil {
		return nil, err
	}
	defer m.Unlock()
	n, err := r.o.Store.GetNamespaceForUpdate(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.Phase == "Deleting" {
		return nil, ErrConflict
	}
	token := r.o.DefaultTarget.Token
	if !n.Default {
		token, err = r.o.Store.GetNamespaceTokenForUpdate(ctx, name)
		if err != nil {
			return nil, err
		}
	}
	_, p, validation := r.o.Backends.Validate(ctx, r.target(n, token))
	applyTokenValidation(n, p, validation, time.Now().UTC(), r.displayName(n.Backend))
	updatePhase(n)
	if err = r.save(ctx, n); err != nil {
		return nil, err
	}
	return n, nil
}
func (r *Registry) Delete(ctx context.Context, name string) error {
	m := r.lock(name)
	if err := lockContext(ctx, m); err != nil {
		return err
	}
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
func (d hubDialer) DialInstanceContext(ctx context.Context, network, addr, instance string) (net.Conn, error) {
	return d.hub.DialInstanceContext(ctx, d.ns, network, addr, instance)
}

// authorizeInstance binds an endpoint to a live runtime of the allowed logical
// member in this namespace's backend. Never reuse inventory across dials.
func (r *Registry) authorizeInstance(ctx context.Context, namespace, member, instance string) error {
	b, err := r.Backend(namespace)
	if err != nil {
		return err
	}
	containers, err := b.Containers(ctx, member)
	if err != nil {
		return err
	}
	for _, container := range containers {
		if container.Status != infra.ContainerActive && container.Status != infra.ContainerRunning {
			continue
		}
		if instance != "" && instance != member && (instance == container.IPv4 || instance == container.IPv6 || instance == container.Hostname) {
			return nil
		}
	}
	return &relay.ErrRefused{Reason: "target is not in namespace policy"}
}
func (r *Registry) Dialer(ns string) engine.ContextDialer {
	if ns == "" || ns == domain.DefaultNamespace {
		return nil
	}
	if n, err := r.Get(context.Background(), ns); err == nil && !r.requiresRelay(n.Backend) {
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
	if !r.requiresRelay(n.Backend) {
		return r.o.AdvertiseURL
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
	if err != nil || n.Default || !r.requiresRelay(n.Backend) || n.Phase == "Deleting" || cert == nil {
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
	if s == nil {
		return false
	}
	if n.Relay.ServiceID != "" && n.Relay.ServiceID == s.ID {
		return true
	}
	return n.OwnershipMarker != "" && subtle.ConstantTimeCompare([]byte(s.Ownership["KARAKORAM_RELAY_OWNER"]), []byte(n.OwnershipMarker)) == 1
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

func cloneConfig(config map[string]string) map[string]string {
	if config == nil {
		return nil
	}
	out := make(map[string]string, len(config))
	for key, value := range config {
		out[key] = value
	}
	return out
}

func (r *Registry) target(n *domain.Namespace, token string) infra.Target {
	return infra.Target{Kind: n.Backend, ID: n.ProjectID, Token: token, Config: cloneConfig(n.Config)}
}

func (r *Registry) requiresRelay(kind string) bool {
	f, err := r.o.Backends.Get(kind)
	return err == nil && f.Info().Relay
}

func (r *Registry) displayName(kind string) string {
	f, err := r.o.Backends.Get(kind)
	if err == nil {
		return f.Info().DisplayName
	}
	return kind
}

type credentialError struct{ message string }

func (e *credentialError) Error() string        { return e.message }
func (e *credentialError) Is(target error) bool { return target == infra.ErrUnauthorized }
func (e *credentialError) SafeDetail() string   { return e.message }
