// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pgbinding

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const connectionLimit = 100

type Access interface {
	postgres.Access
	EnsureBindingRole(context.Context, *engine.Context, string, int) error
	VerifyBindingRole(context.Context, *engine.Context, string) (uint32, error)
	OwnBindingDatabase(context.Context, *engine.Context, postgres.Database) error
	BindingEndpoint(context.Context, *engine.Context) (string, []byte, error)
}

// Options supplies source-specific contexts, never the consumer's credentials.
// Trigger must enqueue immediately, not defer a single request-local trigger.
type Options struct {
	Store   *store.Store
	Source  func(context.Context, domain.ClusterRef) (*engine.Context, Access, error)
	Trigger func(domain.ClusterRef)
}

type bindingSync struct {
	held chan struct{}
	refs int // Holder and waiters; protected by Manager.mu.
}

type privilegeCache struct {
	revision string
	checked  time.Time
}

type Manager struct {
	options Options
	// Only protects registry references and cached results, never native calls.
	mu         sync.Mutex
	bindings   map[string]*bindingSync
	privileges map[string]privilegeCache
}

func New(o Options) *Manager {
	return &Manager{options: o, bindings: make(map[string]*bindingSync), privileges: make(map[string]privilegeCache)}
}

func (m *Manager) acquire(ctx context.Context, id string) (*bindingSync, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	lock := m.bindings[id]
	if lock == nil {
		lock = &bindingSync{held: make(chan struct{}, 1)}
		m.bindings[id] = lock
	}
	lock.refs++
	m.mu.Unlock()
	select {
	case lock.held <- struct{}{}:
		if err := ctx.Err(); err != nil {
			m.release(id, lock)
			return nil, err
		}
		return lock, nil
	case <-ctx.Done():
		m.unreference(id, lock)
		return nil, ctx.Err()
	}
}

func (m *Manager) release(id string, lock *bindingSync) {
	<-lock.held
	m.unreference(id, lock)
}

func (m *Manager) unreference(id string, lock *bindingSync) {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock.refs--
	if lock.refs == 0 {
		delete(m.bindings, id)
	}
}

var _ Service = (*Manager)(nil)

func (m *Manager) source(ctx context.Context, b *domain.PostgresBinding) (*engine.Context, Access, error) {
	rc, a, err := m.options.Source(ctx, b.SourceRef())
	if err != nil {
		return nil, nil, errors.Join(ErrSourceUnavailable, err)
	}
	major, _ := strconv.Atoi(strings.Split(rc.Cluster.Spec.Version, ".")[0])
	if rc.Cluster.Deleting || rc.Cluster.Spec.Provider != "postgres" || strconv.FormatInt(rc.Cluster.CreatedAt.UnixNano(), 10) != b.SourceUID || !(Request{MinMajor: b.MinMajor, MaxMajor: b.MaxMajor}).Admits(major) {
		return nil, nil, errors.Join(ErrSourceUnavailable, engine.ErrConflict)
	}
	return rc, a, nil
}
func (m *Manager) trigger(b *domain.PostgresBinding) {
	if m.options.Trigger != nil {
		m.options.Trigger(b.SourceRef())
		m.options.Trigger(b.ConsumerRef())
	}
}
func (m *Manager) bindingError(ctx context.Context, b *domain.PostgresBinding, err error) error {
	if errors.Is(err, engine.ErrConflict) {
		return errors.Join(err, m.options.Store.UpdatePostgresBindingState(ctx, b.ID, "conflict", b.CredentialGeneration))
	}
	return err
}

func (m *Manager) Ensure(ctx context.Context, r Request) (Connection, error) {
	if err := r.Validate(); err != nil {
		return Connection{}, engine.Invalid("%s", err)
	}
	id := BindingID(r)
	lock, err := m.acquire(ctx, id)
	if err != nil {
		return Connection{}, err
	}
	defer m.release(id, lock)
	b, err := m.options.Store.EnsurePostgresBinding(ctx, &domain.PostgresBinding{ID: id, SourceNamespace: r.SourceNamespace, SourceCluster: r.SourceCluster, ConsumerKind: r.ConsumerKind, ConsumerNamespace: r.ConsumerNamespace, ConsumerName: r.ConsumerName, Databases: r.Databases, Extensions: r.Extensions, MinMajor: r.MinMajor, MaxMajor: r.MaxMajor, State: "pending", CredentialGeneration: 1})
	if err != nil {
		var restoring *store.RestoreInProgressError
		if errors.As(err, &restoring) {
			return Connection{}, errors.Join(engine.ErrConflict, err)
		}
		if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
			return Connection{}, errors.Join(ErrSourceUnavailable, err)
		}
		return Connection{}, err
	}
	if strings.HasPrefix(b.State, "releasing") {
		return Connection{}, engine.ErrConflict
	}
	if b.State == "ready" {
		return m.resolve(ctx, b)
	}
	rc, a, err := m.source(ctx, b)
	if err != nil {
		return Connection{}, err
	}
	ctx = postgres.BindingContext(ctx)
	if err := a.EnsureBindingRole(ctx, rc, b.RoleName, connectionLimit); err != nil {
		return Connection{}, m.bindingError(ctx, b, err)
	}
	roleOID, err := a.VerifyBindingRole(ctx, rc, b.RoleName)
	if err != nil {
		return Connection{}, m.bindingError(ctx, b, err)
	}
	for _, name := range b.Databases {
		db := postgres.Database{Name: name, Owner: b.RoleName, OwnerOID: roleOID, Encoding: "UTF8", Locale: "C.UTF-8", AllowConnections: true, Extensions: b.Extensions}
		existing, err := a.GetDatabase(ctx, rc, name)
		if errors.Is(err, engine.ErrNotFound) {
			closed := db
			closed.AllowConnections, closed.Extensions = false, nil
			if err = a.CreateDatabase(ctx, rc, closed); err != nil {
				return Connection{}, err
			}
		} else if err != nil {
			return Connection{}, err
		} else if existing.OwnerOID == 0 || existing.OwnerOID != roleOID {
			return Connection{}, m.bindingError(ctx, b, fmt.Errorf("%w: database %q is retained or owned outside this binding incarnation; explicit operator adoption is required", engine.ErrConflict, name))
		}
		if err := a.OwnBindingDatabase(ctx, rc, db); err != nil {
			return Connection{}, m.bindingError(ctx, b, err)
		}
		if len(b.Extensions) > 0 {
			extensions, ok := a.(interface {
				EnsureDatabaseExtensions(context.Context, *engine.Context, postgres.Database) error
			})
			if !ok {
				return Connection{}, engine.ErrNotSupported
			}
			if err := extensions.EnsureDatabaseExtensions(ctx, rc, db); err != nil {
				return Connection{}, err
			}
		}
	}
	if b.State == "rotating" {
		if err := m.rotate(ctx, b, rc, a); err != nil {
			return Connection{}, err
		}
	} else {
		if err := m.options.Store.UpdatePostgresBindingState(ctx, b.ID, "ready", b.CredentialGeneration); err != nil {
			return Connection{}, err
		}
	}
	m.cachePrivileges(ctx, b, rc)
	m.trigger(b)
	return m.resolve(ctx, b)
}

func passwordKey(role string) string {
	return "access/passwords/" + base64.RawURLEncoding.EncodeToString([]byte(role))
}
func (m *Manager) Resolve(ctx context.Context, id string) (Connection, error) {
	lock, err := m.acquire(ctx, id)
	if err != nil {
		return Connection{}, err
	}
	defer m.release(id, lock)
	b, err := m.options.Store.GetPostgresBinding(ctx, id)
	if err != nil {
		return Connection{}, err
	}
	if b.State != "ready" {
		return Connection{}, engine.ErrUnavailable
	}
	return m.resolve(ctx, b)
}

func privilegeRevision(b *domain.PostgresBinding, c *domain.Cluster) (string, error) {
	source, err := postgres.BindingSourceRevision(c)
	return b.Incarnation + "/" + strconv.FormatInt(b.CredentialGeneration, 10) + "/" + source, err
}

// Callers hold the binding lock. A fresh source snapshot prevents an in-flight
// restore or replacement from publishing a repair for an obsolete revision.
func (m *Manager) cachePrivileges(ctx context.Context, b *domain.PostgresBinding, rc *engine.Context) {
	before, err := privilegeRevision(b, rc.Cluster)
	if err != nil {
		return
	}
	current, _, err := m.source(ctx, b)
	if err != nil {
		return
	}
	after, err := privilegeRevision(b, current.Cluster)
	if err == nil && before == after {
		m.mu.Lock()
		m.privileges[b.ID] = privilegeCache{revision: after, checked: time.Now()}
		m.mu.Unlock()
	}
}

func (m *Manager) reconcilePrivileges(ctx context.Context, b *domain.PostgresBinding, rc *engine.Context, a Access) error {
	revision, err := privilegeRevision(b, rc.Cluster)
	if err != nil {
		return err
	}
	m.mu.Lock()
	cached := m.privileges[b.ID]
	m.mu.Unlock()
	// Periodically revalidate untracked native changes (for example a database
	// recreated manually under the same owner) without SQL on every status poll.
	if cached.revision == revision && time.Since(cached.checked) < 5*time.Minute {
		return nil
	}
	ctx = postgres.BindingContext(ctx)
	oid, err := a.VerifyBindingRole(ctx, rc, b.RoleName)
	if err != nil {
		return m.bindingError(ctx, b, err)
	}
	for _, name := range b.Databases {
		db, err := a.GetDatabase(ctx, rc, name)
		if err != nil {
			return err
		}
		if db.OwnerOID == 0 || db.OwnerOID != oid {
			return m.bindingError(ctx, b, fmt.Errorf("%w: database %q is owned outside this binding incarnation", engine.ErrConflict, name))
		}
		if err := a.OwnBindingDatabase(ctx, rc, postgres.Database{Name: name, Owner: b.RoleName, OwnerOID: oid}); err != nil {
			return m.bindingError(ctx, b, err)
		}
	}
	m.cachePrivileges(ctx, b, rc)
	return nil
}
func (m *Manager) resolve(ctx context.Context, b *domain.PostgresBinding) (Connection, error) {
	rc, a, err := m.source(ctx, b)
	if err != nil {
		return Connection{}, err
	}
	// Existing consumers normally Resolve rather than Ensure after restart.
	// Retry transient repair failures without changing an otherwise healthy
	// ready binding's availability; only identity conflicts fail it closed.
	budget := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		budget = min(budget, time.Until(deadline)/4)
	}
	repairCtx, cancel := context.WithTimeout(ctx, budget)
	err = m.reconcilePrivileges(repairCtx, b, rc, a)
	cancel()
	if errors.Is(err, engine.ErrConflict) {
		return Connection{}, err
	}
	host, ca, err := a.BindingEndpoint(ctx, rc)
	if err != nil {
		return Connection{}, err
	}
	all, err := m.options.Store.ListPostgresBindings(ctx, b.SourceRef())
	if err != nil {
		return Connection{}, err
	}
	return Connection{BindingID: b.ID, Host: host, Port: 5432, Databases: append([]string(nil), b.Databases...), Username: b.RoleName, PasswordSecretRef: "pgbinding:" + b.ID, TLS: TLS{Mode: TLSVerifyFull, CAPEM: ca}, MaxConnections: connectionLimit, CredentialGeneration: b.CredentialGeneration, Shared: len(all) > 1, ConsumerCount: len(all)}, nil
}
func (m *Manager) Password(ctx context.Context, ref string) (string, error) {
	if !strings.HasPrefix(ref, "pgbinding:") {
		return "", engine.ErrNotFound
	}
	b, err := m.options.Store.GetPostgresBinding(ctx, strings.TrimPrefix(ref, "pgbinding:"))
	if err != nil {
		return "", err
	}
	if _, _, err := m.source(ctx, b); err != nil {
		return "", err
	}
	value, err := m.options.Store.GetClusterSecret(ctx, b.SourceRef(), passwordKey(b.RoleName))
	return string(value), err
}

func (m *Manager) Rotate(ctx context.Context, id string) (Connection, error) {
	lock, err := m.acquire(ctx, id)
	if err != nil {
		return Connection{}, err
	}
	defer m.release(id, lock)
	b, err := m.options.Store.GetPostgresBinding(ctx, id)
	if err != nil {
		return Connection{}, err
	}
	if b.State != "ready" && b.State != "rotating" {
		return Connection{}, engine.ErrConflict
	}
	rc, a, err := m.source(ctx, b)
	if err != nil {
		return Connection{}, err
	}
	if err := m.options.Store.UpdatePostgresBindingState(ctx, id, "rotating", b.CredentialGeneration); err != nil {
		return Connection{}, err
	}
	if err := m.reconcilePrivileges(ctx, b, rc, a); err != nil {
		return Connection{}, err
	}
	if err := m.rotate(postgres.BindingContext(ctx), b, rc, a); err != nil {
		return Connection{}, err
	}
	m.trigger(b)
	return m.resolve(ctx, b)
}
func (m *Manager) rotate(ctx context.Context, b *domain.PostgresBinding, rc *engine.Context, a Access) error {
	key := "bindings/" + b.ID + "/" + b.Incarnation + "/rotation/" + strconv.FormatInt(b.CredentialGeneration+1, 10)
	password, err := m.options.Store.GetClusterSecret(ctx, b.SourceRef(), key)
	if errors.Is(err, store.ErrNotFound) {
		value, e := secrets.Password()
		if e != nil {
			return e
		}
		password = []byte(value)
		if err = m.options.Store.SetClusterSecret(ctx, b.SourceRef(), key, password); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	role, err := a.GetRole(ctx, rc, b.RoleName)
	if errors.Is(err, engine.ErrNotFound) {
		return m.bindingError(ctx, b, fmt.Errorf("%w: binding role was removed", engine.ErrConflict))
	}
	if err != nil {
		return err
	}
	value := string(password)
	if err := a.UpdateRole(ctx, rc, postgres.RoleWrite{Role: *role, Password: &value}); err != nil {
		return m.bindingError(ctx, b, err)
	}
	b.CredentialGeneration++
	if err := m.options.Store.UpdatePostgresBindingState(ctx, b.ID, "ready", b.CredentialGeneration); err != nil {
		return err
	}
	b.State = "ready"
	return m.options.Store.DeleteClusterSecret(ctx, b.SourceRef(), key)
}

func (m *Manager) Release(ctx context.Context, id string, keepData bool) error {
	lock, err := m.acquire(ctx, id)
	if err != nil {
		return err
	}
	defer m.release(id, lock)
	b, err := m.options.Store.GetPostgresBinding(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	state := "releasing-drop"
	if keepData {
		state = "releasing-keep"
	}
	if strings.HasPrefix(b.State, "releasing") && b.State != state {
		return engine.ErrConflict
	}
	rc, a, err := m.source(ctx, b)
	if err != nil {
		return err
	}
	// A missing role is valid after a failed Ensure or a completed DROP whose
	// response was lost. Any existing role must still match its creation receipt.
	rolePresent := true
	roleOID, err := a.VerifyBindingRole(postgres.BindingContext(ctx), rc, b.RoleName)
	if errors.Is(err, engine.ErrNotFound) {
		rolePresent = false
	} else if err != nil {
		return m.bindingError(ctx, b, err)
	}
	ctx = postgres.BindingContext(ctx)
	if err := m.options.Store.UpdatePostgresBindingState(ctx, id, state, b.CredentialGeneration); err != nil {
		return err
	}
	if keepData && rolePresent {
		role, err := a.GetRole(ctx, rc, b.RoleName)
		if err != nil {
			return err
		}
		role.Login = false
		if err := a.UpdateRole(ctx, rc, postgres.RoleWrite{Role: *role}); err != nil {
			return m.bindingError(ctx, b, err)
		}
	} else if !keepData {
		for _, name := range b.Databases {
			db, err := a.GetDatabase(ctx, rc, name)
			if errors.Is(err, engine.ErrNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if !rolePresent || roleOID == 0 || db.OwnerOID != roleOID {
				continue // Never touch databases owned by another incarnation.
			}
			if err := a.DeleteDatabase(ctx, rc, name, true); err != nil && !errors.Is(err, engine.ErrNotFound) {
				return err
			}
		}
		if rolePresent {
			if err := a.DeleteRole(ctx, rc, b.RoleName); err != nil && !errors.Is(err, engine.ErrNotFound) {
				return m.bindingError(ctx, b, err)
			}
		}
	}
	if err := m.options.Store.DeletePostgresBinding(ctx, id); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.privileges, id)
	m.mu.Unlock()
	m.trigger(b)
	return nil
}
