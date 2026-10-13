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

var _ engine.Namespaces = (*Registry)(nil)

func New(ctx context.Context, o Options) (*Registry, error) {
	if o.Store == nil || o.DefaultBackend == nil {
		return nil, errors.New("namespace store and default backend are required")
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9]{0,24}$`).MatchString(o.RelayHostname) || o.RelayHostname == "core" {
		return nil, errors.New("invalid relay hostname")
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
	r.Hub = relay.NewHub(relay.HubOptions{Certificate: cert, ClientCAs: pool, Authorize: r.Authorize, Logger: o.Logger, OnSession: func(ns string, e relay.SessionEvent) {
		select {
		case r.notices <- sessionNotice{ns, e}:
		default:
		}
	}})
	now := time.Now().UTC()
	n, err := o.Store.GetNamespace(ctx, domain.DefaultNamespace)
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
	n.Token.LastValidatedAt = now
	n.Token.Valid = validation == nil && project.ID == n.ProjectID
	n.Token.LastError = ""
	if !n.Token.Valid {
		n.Token.LastError = "Environment-managed token cannot access the configured project"
	} else {
		n.ProjectName = project.Name
	}
	updatePhase(n)
	if err = o.Store.PutNamespace(ctx, n); err != nil {
		r.Hub.Close()
		return nil, err
	}
	all, err := o.Store.ListNamespaces(ctx)
	if err != nil {
		r.Hub.Close()
		return nil, err
	}
	for _, n := range all {
		if n.Default {
			continue
		}
		n.Relay.DesiredVersion = version.Version
		if e := o.Store.PutNamespace(ctx, &n); e != nil {
			r.Hub.Close()
			return nil, e
		}
		token, e := o.Store.GetNamespaceToken(ctx, n.Name)
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
			if e = o.Store.PutNamespace(ctx, &n); e != nil {
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
		if err := r.o.Events.Record(ctx, domain.Event{Namespace: n.Name, Type: "Normal", Reason: kind, Message: message, CreatedAt: time.Now().UTC()}); err != nil {
			r.o.Logger.Error("Namespace event persistence failed", "namespace", n.Name, "error", err)
		}
	}
}
func (r *Registry) save(ctx context.Context, n *domain.Namespace) error {
	n.UpdatedAt = time.Now().UTC()
	return r.o.Store.PutNamespace(ctx, n)
}
func (r *Registry) trigger() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
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
	r.createMu.Lock()
	defer r.createMu.Unlock()
	all, err := r.List(ctx)
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
		return nil, fmt.Errorf("%w: %s", ErrInvalid, err)
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
	if err = r.o.Store.PutNamespaceCredentials(ctx, n, token, key); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.backends[name] = b
	r.mu.Unlock()
	r.event(ctx, n, "NamespaceCreated", "Namespace registered")
	r.trigger()
	return n, nil
}
func (r *Registry) Update(ctx context.Context, name, description string) (*domain.Namespace, error) {
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	n, err := r.Get(ctx, name)
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
	n, err := r.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.Default {
		return nil, fmt.Errorf("%w: default token is environment-managed; update the service secret and restart", ErrConflict)
	}
	if n.Phase == "Deleting" {
		return nil, ErrConflict
	}
	z := r.o.Zerops
	z.Token = token
	z.ProjectID = n.ProjectID
	replacement, p, err := r.o.Validate(ctx, z)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalid, err)
	}
	now := time.Now().UTC()
	n.Token.Valid = true
	n.Token.LastError = ""
	n.Token.LastValidatedAt = now
	n.Token.RotatedAt = now
	n.ProjectName = p.Name
	n.UpdatedAt = now
	updatePhase(n)
	if err = r.o.Store.PutNamespaceCredentials(ctx, n, token, nil); err != nil {
		return nil, err
	}
	if b := r.backend(name); b != nil {
		b.SetToken(token)
	} else {
		r.mu.Lock()
		r.backends[name] = replacement
		r.mu.Unlock()
	}
	r.event(ctx, n, "NamespaceTokenRotated", "Project token rotated")
	return n, nil
}
func (r *Registry) Validate(ctx context.Context, name string) (*domain.Namespace, error) {
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	n, err := r.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	if n.Phase == "Deleting" {
		return nil, ErrConflict
	}
	z := r.o.Zerops
	z.ProjectID = n.ProjectID
	if !n.Default {
		z.Token, err = r.o.Store.GetNamespaceToken(ctx, name)
		if err != nil {
			return nil, err
		}
	}
	_, p, validation := r.o.Validate(ctx, z)
	was := n.Token.Valid
	first := n.Token.LastValidatedAt.IsZero()
	n.Token.Valid = validation == nil
	n.Token.LastValidatedAt = time.Now().UTC()
	n.Token.LastError = ""
	if validation != nil {
		n.Token.LastError = validation.Error()
	} else {
		n.ProjectName = p.Name
	}
	updatePhase(n)
	if err = r.save(ctx, n); err != nil {
		return nil, err
	}
	if first || was != n.Token.Valid {
		if n.Token.Valid {
			r.event(ctx, n, "NamespaceValidated", "Project token validated")
		} else {
			r.event(ctx, n, "NamespaceValidationFailed", "Project token validation failed")
		}
	}
	return n, nil
}
func (r *Registry) Delete(ctx context.Context, name string) error {
	m := r.lock(name)
	m.Lock()
	defer m.Unlock()
	n, err := r.Get(ctx, name)
	if err != nil {
		return err
	}
	if n.Default {
		return fmt.Errorf("%w: default namespace cannot be deleted", ErrConflict)
	}
	clusters, err := r.o.Store.ListClusters(ctx, name)
	if err != nil {
		return err
	}
	storages, err := r.o.Store.ListBackupStorages(ctx, name)
	if err != nil {
		return err
	}
	backups, err := r.o.Store.ListBackups(ctx, domain.ClusterRef{Namespace: name})
	if err != nil {
		return err
	}
	orphans, err := r.o.Store.ListOrphanedVolumes(ctx, name)
	if err != nil {
		return err
	}
	if len(clusters)+len(storages)+len(backups)+len(orphans) > 0 {
		return fmt.Errorf("%w: remove all clusters, backup storages, retained backups and orphaned volumes first", ErrConflict)
	}
	if n.Phase != "Deleting" {
		n.Phase = "Deleting"
		if err = r.save(ctx, n); err != nil {
			return err
		}
		r.event(ctx, n, "NamespaceDeleting", "Namespace deletion requested")
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
	return "https://" + r.o.RelayHostname + ":9443"
}
func (r *Registry) Authorize(ctx context.Context, ns string, cert *x509.Certificate) error {
	n, err := r.Get(ctx, ns)
	if err != nil || n.Default || n.Phase == "Deleting" || cert == nil {
		return errors.New("relay namespace is not enrolled")
	}
	serial := cert.SerialNumber.String()
	previousAllowed := serial == n.PreviousClientSerial && time.Now().Before(n.PreviousCertNotAfter)
	// A current-certificate connection revokes overlap immediately, even before persistence.
	if previousAllowed && r.Hub.Status(ns).CertificateSerial == n.ClientSerial {
		previousAllowed = false
	}
	if serial != n.ClientSerial && !previousAllowed {
		return errors.New("relay certificate is not enrolled")
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
