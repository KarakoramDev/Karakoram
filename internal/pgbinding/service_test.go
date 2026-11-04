// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pgbinding

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type fakeAccess struct {
	postgres.Access
	st                                       *store.Store
	roles                                    map[string]postgres.Role
	roleOIDs                                 map[string]uint32
	databases                                map[string]postgres.Database
	failSchema                               bool
	failUpdate                               bool
	roleConflict                             bool
	onOwn                                    func(context.Context, postgres.Database) error
	roleCreates, schemaCalls, extensionCalls int
}

func (f *fakeAccess) EnsureBindingRole(ctx context.Context, rc *engine.Context, name string, limit int) error {
	if _, ok := f.roles[name]; ok {
		return nil
	}
	f.roles[name] = postgres.Role{Name: name, Login: true, ConnectionLimit: limit}
	f.roleCreates++
	f.roleOIDs[name] = uint32(f.roleCreates)
	return f.st.SetClusterSecret(ctx, rc.Cluster.Ref(), passwordKey(name), []byte("initial-password-value"))
}
func (f *fakeAccess) VerifyBindingRole(ctx context.Context, rc *engine.Context, name string) (uint32, error) {
	if f.roleConflict {
		return 0, engine.ErrConflict
	}
	_, err := f.GetRole(ctx, rc, name)
	return f.roleOIDs[name], err
}
func (f *fakeAccess) GetRole(_ context.Context, _ *engine.Context, name string) (*postgres.Role, error) {
	v, ok := f.roles[name]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return &v, nil
}
func (f *fakeAccess) UpdateRole(ctx context.Context, rc *engine.Context, v postgres.RoleWrite) error {
	if f.roleConflict {
		return engine.ErrConflict
	}
	f.roles[v.Name] = v.Role
	if v.Password != nil {
		if err := f.st.SetClusterSecret(ctx, rc.Cluster.Ref(), passwordKey(v.Name), []byte(*v.Password)); err != nil {
			return err
		}
		if f.failUpdate {
			f.failUpdate = false
			return engine.ErrUnavailable
		}
		return nil
	}
	return nil
}
func (f *fakeAccess) DeleteRole(_ context.Context, _ *engine.Context, name string) error {
	delete(f.roles, name)
	delete(f.roleOIDs, name)
	return nil
}
func (f *fakeAccess) GetDatabase(_ context.Context, _ *engine.Context, name string) (*postgres.Database, error) {
	v, ok := f.databases[name]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return &v, nil
}
func (f *fakeAccess) CreateDatabase(_ context.Context, _ *engine.Context, v postgres.Database) error {
	if _, ok := f.databases[v.Name]; ok {
		return engine.ErrConflict
	}
	v.OwnerOID = f.roleOIDs[v.Owner]
	f.databases[v.Name] = v
	return nil
}
func (f *fakeAccess) DeleteDatabase(_ context.Context, _ *engine.Context, name string, _ bool) error {
	delete(f.databases, name)
	return nil
}
func (f *fakeAccess) OwnBindingDatabase(ctx context.Context, _ *engine.Context, db postgres.Database) error {
	if f.onOwn != nil {
		if err := f.onOwn(ctx, db); err != nil {
			return err
		}
	}
	f.schemaCalls++
	if f.failSchema {
		f.failSchema = false
		return engine.ErrUnavailable
	}
	database := f.databases[db.Name]
	database.AllowConnections = true
	f.databases[db.Name] = database
	return nil
}
func (f *fakeAccess) EnsureDatabaseExtensions(_ context.Context, _ *engine.Context, db postgres.Database) error {
	if !f.databases[db.Name].AllowConnections {
		return errors.New("extensions attempted before binding privileges enabled connections")
	}
	f.extensionCalls++
	return nil
}
func (f *fakeAccess) BindingEndpoint(context.Context, *engine.Context) (string, []byte, error) {
	return "sourcepx", []byte("public-ca"), nil
}
func bindingFixture(t *testing.T) (*Manager, *fakeAccess, Request, *store.Store) {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	c := domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
	if err := st.CreateCluster(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	f := &fakeAccess{st: st, roles: map[string]postgres.Role{}, roleOIDs: map[string]uint32{}, databases: map[string]postgres.Database{}}
	m := New(Options{Store: st, Source: func(ctx context.Context, ref domain.ClusterRef) (*engine.Context, Access, error) {
		c, err := st.GetCluster(ctx, ref)
		return &engine.Context{Cluster: c, Store: st, Secrets: st}, f, err
	}})
	return m, f, Request{ConsumerKind: "keycloak", ConsumerName: "identity", SourceCluster: "source", Databases: []string{"identity"}, MinMajor: 16, MaxMajor: 18}, st
}
func TestEnsureRetryRotationAndRelease(t *testing.T) {
	m, f, r, st := bindingFixture(t)
	f.failSchema = true
	r.Extensions = []string{"pgcrypto"}
	if _, err := m.Ensure(t.Context(), r); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("first ensure: %v", err)
	}
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if f.roleCreates != 1 || f.schemaCalls != 2 || f.extensionCalls != 1 || !strings.HasPrefix(c.Username, "kb_") || c.CredentialGeneration != 1 || c.Host != "sourcepx" || c.Shared {
		t.Fatalf("connection=%+v access=%+v", c, f)
	}
	before, err := m.Password(t.Context(), c.PasswordSecretRef)
	if err != nil {
		t.Fatal(err)
	}
	var triggered []domain.ClusterRef
	m.options.Trigger = func(ref domain.ClusterRef) { triggered = append(triggered, ref) }
	rotated, err := m.Rotate(t.Context(), c.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	after, err := m.Password(t.Context(), rotated.PasswordSecretRef)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.CredentialGeneration != 2 || before == after || len(triggered) != 2 {
		t.Fatalf("rotation=%+v triggers=%v", rotated, triggered)
	}
	r.Databases = append(r.Databases, "visibility")
	extended, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if len(extended.Databases) != 2 || extended.BindingID != c.BindingID {
		t.Fatalf("additive ensure: %+v", extended)
	}
	if err := st.MarkClusterDeleting(t.Context(), domain.ClusterRef{Name: "source"}, domain.DeleteOptions{}); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("delete guard: %v", err)
	}
	if err := m.Release(t.Context(), c.BindingID, false); err != nil {
		t.Fatal(err)
	}
	if len(f.databases) != 0 || len(f.roles) != 0 {
		t.Fatal("release left objects")
	}
	if err := m.Release(t.Context(), c.BindingID, false); err != nil {
		t.Fatal(err)
	}
}

func TestReadyBindingReconcilesPrivilegesOncePerProcess(t *testing.T) {
	for _, state := range []string{"ready", "rotating"} {
		t.Run(state, func(t *testing.T) {
			m, f, r, st := bindingFixture(t)
			c, err := m.Ensure(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.UpdatePostgresBindingState(t.Context(), c.BindingID, state, 1); err != nil {
				t.Fatal(err)
			}
			m = New(m.options) // A control-plane upgrade/restart loses only the repair cache.
			before := f.schemaCalls
			if _, err := m.Ensure(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if f.schemaCalls != before+1 {
				t.Fatalf("existing %s binding repair calls=%d, want %d", state, f.schemaCalls, before+1)
			}
			for range 3 {
				if _, err := m.Ensure(t.Context(), r); err != nil {
					t.Fatal(err)
				}
				if _, err := m.Resolve(t.Context(), c.BindingID); err != nil {
					t.Fatal(err)
				}
			}
			if f.schemaCalls != before+1 {
				t.Fatalf("status polls repeated privilege repair: %d", f.schemaCalls)
			}
		})
	}
}

func TestReadyBindingPrivilegeRepairRetriesWithoutUnbinding(t *testing.T) {
	m, f, r, st := bindingFixture(t)
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	m = New(m.options)
	f.failSchema = true
	before := f.schemaCalls
	if _, err := m.Ensure(t.Context(), r); err != nil {
		t.Fatalf("transient privilege repair unbound healthy binding: %v", err)
	}
	b, err := st.GetPostgresBinding(t.Context(), c.BindingID)
	if err != nil || b.State != "ready" {
		t.Fatalf("healthy binding state: %+v %v", b, err)
	}
	if _, err := m.Resolve(t.Context(), c.BindingID); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Ensure(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if f.schemaCalls != before+2 {
		t.Fatalf("repair did not retry exactly once: %d", f.schemaCalls)
	}
}

func TestReadyBindingPrivilegeRepairRejectsForeignOwner(t *testing.T) {
	m, f, r, st := bindingFixture(t)
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	m = New(m.options)
	db := f.databases[r.Databases[0]]
	db.OwnerOID++
	f.databases[db.Name] = db
	before := f.schemaCalls
	if _, err := m.Ensure(t.Context(), r); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("foreign owner accepted: %v", err)
	}
	b, err := st.GetPostgresBinding(t.Context(), c.BindingID)
	if err != nil || b.State != "conflict" || f.schemaCalls != before {
		t.Fatalf("foreign database changed: binding=%+v calls=%d err=%v", b, f.schemaCalls, err)
	}
}

func TestBindingCreationStaysClosedUntilPrivilegesRepaired(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	r.Extensions = []string{"pgcrypto"}
	f.failSchema = true
	if _, err := m.Ensure(t.Context(), r); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	if f.databases[r.Databases[0]].AllowConnections || f.extensionCalls != 0 {
		t.Fatal("partial creation exposed database or attempted extensions before repair")
	}
	if _, err := m.Ensure(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if !f.databases[r.Databases[0]].AllowConnections || f.extensionCalls != 1 || f.roleCreates != 1 {
		t.Fatal("closed database did not resume through privilege repair and extensions")
	}
}

func TestBindingPrivilegesRevalidatedAfterSourceRecovery(t *testing.T) {
	m, f, r, st := bindingFixture(t)
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	public := true // Physical restore reinstates the old permissive ACLs.
	f.onOwn = func(context.Context, postgres.Database) error { public = false; return nil }
	before := f.schemaCalls
	source, err := st.GetCluster(t.Context(), domain.ClusterRef{Name: r.SourceCluster})
	if err != nil {
		t.Fatal(err)
	}
	source.Internal = []byte(`{"postgres":{"bootstrapGeneration":2},"postgresBackup":{"generation":3}}`)
	if err := st.UpdateClusterState(t.Context(), source); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Resolve(t.Context(), c.BindingID); err != nil {
		t.Fatal(err)
	}
	if public || f.schemaCalls != before+1 {
		t.Fatal("source recovery reused pre-restore privilege repair")
	}
	if _, err := m.Resolve(t.Context(), c.BindingID); err != nil || f.schemaCalls != before+1 {
		t.Fatalf("repaired recovery was not cached: %v", err)
	}
}

func TestBindingRepairDoesNotCacheChangedSourceRevision(t *testing.T) {
	m, f, r, st := bindingFixture(t)
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	m = New(m.options)
	f.onOwn = func(context.Context, postgres.Database) error {
		source, err := st.GetCluster(t.Context(), domain.ClusterRef{Name: r.SourceCluster})
		if err != nil {
			return err
		}
		source.Internal = []byte(`{"postgresBackup":{"generation":4}}`)
		return st.UpdateClusterState(t.Context(), source)
	}
	if _, err := m.Resolve(t.Context(), c.BindingID); err != nil {
		t.Fatal(err)
	}
	before := f.schemaCalls
	f.onOwn = nil
	if _, err := m.Resolve(t.Context(), c.BindingID); err != nil || f.schemaCalls != before+1 {
		t.Fatalf("in-flight source revision change cached stale repair: %v", err)
	}
}

func TestBlockedBindingRepairDoesNotBlockOtherBindings(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	first, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	r.ConsumerName, r.Databases = "other", []string{"other"}
	second, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	m = New(m.options)
	started, release := make(chan struct{}), make(chan struct{})
	f.onOwn = func(ctx context.Context, db postgres.Database) error {
		if db.Name != "identity" {
			return nil
		}
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	}
	done := make(chan error, 1)
	go func() { _, err := m.Resolve(t.Context(), first.BindingID); done <- err }()
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, secondErr := m.Resolve(ctx, second.BindingID)
	close(release)
	firstErr := <-done
	if secondErr != nil || firstErr != nil {
		t.Fatalf("unrelated binding blocked: first=%v second=%v", firstErr, secondErr)
	}
}

func TestBindingRepairDeadlineLeavesTimeForResolution(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	m = New(m.options)
	f.onOwn = func(ctx context.Context, _ postgres.Database) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := m.Resolve(ctx, c.BindingID); err != nil {
		t.Fatalf("repair consumed normal resolution deadline: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("repair exhausted parent deadline")
	}
}

func TestBindingRepairRevalidatesUntrackedDatabaseReplacement(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	public := true
	f.onOwn = func(context.Context, postgres.Database) error { public = false; return nil }
	m.mu.Lock()
	cached := m.privileges[c.BindingID]
	cached.checked = time.Now().Add(-6 * time.Minute)
	m.privileges[c.BindingID] = cached
	m.mu.Unlock()
	before := f.schemaCalls
	if _, err := m.Resolve(t.Context(), c.BindingID); err != nil || public || f.schemaCalls != before+1 {
		t.Fatalf("untracked replacement was not revalidated: public=%t err=%v", public, err)
	}
}

func TestBindingLockRegistryReclaimedAfterOperations(t *testing.T) {
	m, _, r, _ := bindingFixture(t)
	idle := func() {
		t.Helper()
		m.mu.Lock()
		defer m.mu.Unlock()
		if len(m.bindings) != 0 {
			t.Fatalf("idle binding locks retained: %d", len(m.bindings))
		}
	}
	connection, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	idle()
	if _, err := m.Resolve(t.Context(), connection.BindingID); err != nil {
		t.Fatal(err)
	}
	idle()
	if _, err := m.Rotate(t.Context(), connection.BindingID); err != nil {
		t.Fatal(err)
	}
	idle()
	_, _ = m.Backup(t.Context(), connection.BindingID, "full", "backup")
	idle()
	_, _ = m.Restore(t.Context(), connection.BindingID, "missing", "restore", false)
	idle()
	if err := m.Release(t.Context(), connection.BindingID, false); err != nil {
		t.Fatal(err)
	}
	idle()
	for i := range 100 {
		id := "missing-" + strconv.Itoa(i)
		_, _ = m.Resolve(t.Context(), id)
		_, _ = m.Rotate(t.Context(), id)
		_ = m.Release(t.Context(), id, false)
		_, _ = m.Backup(t.Context(), id, "full", "backup")
		_, _ = m.Restore(t.Context(), id, "missing", "restore", false)
		idle()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.privileges) != 0 {
		t.Fatalf("released binding retained privilege cache: %d", len(m.privileges))
	}
}

func TestBindingLockWaitersStaySerializedAndAreReclaimed(t *testing.T) {
	m := New(Options{})
	first, err := m.acquire(t.Context(), "binding")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if first != nil {
			m.release("binding", first)
		}
	}()
	const waiters = 16
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	var active atomic.Int32
	errors := make(chan error, 2*waiters)
	for range waiters {
		wg.Go(func() {
			lock, err := m.acquire(ctx, "binding")
			if err != nil {
				errors <- err
				return
			}
			if lock != first {
				errors <- fmt.Errorf("waiter acquired a replacement lock")
			}
			if active.Add(1) != 1 {
				errors <- fmt.Errorf("binding waiters overlapped")
			}
			runtime.Gosched()
			active.Add(-1)
			m.release("binding", lock)
		})
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		m.mu.Lock()
		queued := first.refs == waiters+1
		m.mu.Unlock()
		if queued {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("binding waiters did not register")
		case <-ticker.C:
		}
	}
	m.release("binding", first)
	wg.Wait()
	first = nil
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.bindings) != 0 {
		t.Fatalf("completed waiters retained lock entries: %d", len(m.bindings))
	}
}

func TestCancelledBindingLockWaiterReleasesReference(t *testing.T) {
	m := New(Options{})
	first, err := m.acquire(t.Context(), "binding")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	if lock, err := m.acquire(ctx, "binding"); !errors.Is(err, context.DeadlineExceeded) || lock != nil {
		t.Fatalf("lock wait ignored deadline: lock=%p err=%v", lock, err)
	}
	m.mu.Lock()
	remaining := first.refs
	m.mu.Unlock()
	m.release("binding", first)
	if remaining != 1 || len(m.bindings) != 0 {
		t.Fatalf("canceled waiter leaked reference: refs=%d entries=%d", remaining, len(m.bindings))
	}
}
func TestForeignDatabaseIsNotAdopted(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	f.databases["identity"] = postgres.Database{Name: "identity", Owner: "foreign"}
	if _, err := m.Ensure(t.Context(), r); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("foreign owner: %v", err)
	}
	if f.schemaCalls != 0 || f.databases["identity"].Owner != "foreign" {
		t.Fatal("foreign database mutated")
	}
}

func TestFailedEnsureCanAlwaysBeReleased(t *testing.T) {
	for _, failure := range []string{"source", "foreign-database", "schema"} {
		t.Run(failure, func(t *testing.T) {
			m, f, r, st := bindingFixture(t)
			source := m.options.Source
			switch failure {
			case "source":
				m.options.Source = func(context.Context, domain.ClusterRef) (*engine.Context, Access, error) {
					return nil, nil, engine.ErrUnavailable
				}
			case "foreign-database":
				f.databases["identity"] = postgres.Database{Name: "identity", Owner: "foreign"}
			case "schema":
				f.failSchema = true
			}
			if _, err := m.Ensure(t.Context(), r); err == nil {
				t.Fatal("expected provisioning failure")
			}
			id := BindingID(r)
			if _, err := st.GetPostgresBinding(t.Context(), id); err != nil {
				t.Fatalf("deterministic persisted ID: %v", err)
			}
			ref := domain.ClusterRef{Name: r.SourceCluster}
			if err := st.MarkClusterDeleting(t.Context(), ref, domain.DeleteOptions{}); !errors.Is(err, store.ErrConflict) {
				t.Fatalf("missing dependency guard: %v", err)
			}
			m.options.Source = source
			if err := m.Release(t.Context(), id, false); err != nil {
				t.Fatal(err)
			}
			if _, err := st.GetPostgresBinding(t.Context(), id); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("binding stranded: %v", err)
			}
			if failure == "foreign-database" && f.databases["identity"].Owner != "foreign" {
				t.Fatal("foreign database changed")
			}
			if err := st.MarkClusterDeleting(t.Context(), ref, domain.DeleteOptions{}); err != nil {
				t.Fatalf("released binding still blocks deletion: %v", err)
			}
		})
	}
}

func TestBindingIDIdentityOnly(t *testing.T) {
	r := Request{ConsumerKind: "keycloak", ConsumerName: "identity", SourceCluster: "source"}
	id := BindingID(r)
	r.ConsumerNamespace, r.SourceNamespace = "default", "default"
	r.Databases, r.Extensions, r.MinMajor = []string{"changed"}, []string{"pgcrypto"}, 17
	if BindingID(r) != id {
		t.Fatal("mutable request or default namespace changed identity")
	}
	r.SourceCluster = "other"
	if BindingID(r) == id || !strings.HasPrefix(id, "pgb-") || len(id) != 36 {
		t.Fatal("source identity or URL-safe ID invalid")
	}
}

func TestRoleConflictStopsRotationAndRelease(t *testing.T) {
	for _, action := range []string{"rotate", "keep", "drop"} {
		t.Run(action, func(t *testing.T) {
			m, f, r, st := bindingFixture(t)
			c, err := m.Ensure(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			f.roleConflict = true
			if action == "rotate" {
				_, err = m.Rotate(t.Context(), c.BindingID)
			} else {
				err = m.Release(t.Context(), c.BindingID, action == "keep")
			}
			if !errors.Is(err, engine.ErrConflict) {
				t.Fatalf("expected conflict: %v", err)
			}
			b, err := st.GetPostgresBinding(t.Context(), c.BindingID)
			if err != nil || b.State != "conflict" {
				t.Fatalf("binding conflict state: %+v %v", b, err)
			}
			if !f.roles[c.Username].Login || len(f.databases) != 1 {
				t.Fatal("foreign role or database mutated")
			}
		})
	}
}
func TestSharedSourceAndKeepData(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	first, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	r.ConsumerName = "other"
	r.Databases = []string{"other"}
	if _, err := m.Ensure(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	resolved, err := m.Resolve(t.Context(), first.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	if !resolved.Shared || resolved.ConsumerCount != 2 {
		t.Fatalf("shared=%+v", resolved)
	}
	if _, err := m.Restore(t.Context(), first.BindingID, "backup", "request", false); !errors.Is(err, ErrSharedSource) {
		t.Fatalf("shared restore=%v", err)
	}
	if err := m.Release(t.Context(), first.BindingID, true); err != nil {
		t.Fatal(err)
	}
	if f.roles[first.Username].Login || f.databases["identity"].Owner != first.Username {
		t.Fatal("keepData did not retain disabled ownership")
	}
}

func TestBindingIncarnationRecreation(t *testing.T) {
	for _, keep := range []bool{true, false} {
		t.Run(map[bool]string{true: "retained", false: "dropped"}[keep], func(t *testing.T) {
			m, f, r, st := bindingFixture(t)
			r.Databases = []string{"identity", "visibility"}
			first, err := m.Ensure(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			old, err := st.GetPostgresBinding(t.Context(), first.BindingID)
			if err != nil {
				t.Fatal(err)
			}
			oldOID := f.roleOIDs[first.Username]
			if err := m.Release(t.Context(), first.BindingID, keep); err != nil {
				t.Fatal(err)
			}
			m = New(m.options) // Only durable state survives controller recreation.
			second, err := m.Ensure(t.Context(), r)
			if keep {
				if !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), "identity") || !strings.Contains(err.Error(), "retained") {
					t.Fatalf("retained database must conflict by name: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			} else if second.BindingID != first.BindingID {
				t.Fatal("consumer cleanup identity changed")
			}
			current, err := st.GetPostgresBinding(t.Context(), BindingID(r))
			if err != nil {
				t.Fatal(err)
			}
			if current.Incarnation == "" || current.Incarnation == old.Incarnation || current.RoleName == old.RoleName {
				t.Fatal("recreated binding reused native ownership")
			}
			if keep && current.State != "conflict" {
				t.Fatalf("conflict state: %s", current.State)
			}
			if err := m.Release(t.Context(), current.ID, false); err != nil {
				t.Fatal(err)
			}
			if _, err := st.GetPostgresBinding(t.Context(), current.ID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("failed consumer binding not released: %v", err)
			}
			if keep {
				role, ok := f.roles[old.RoleName]
				if !ok || role.Login || f.roleOIDs[old.RoleName] != oldOID || len(f.roles) != 1 || len(f.databases) != 2 {
					t.Fatal("cleanup changed retained role or databases")
				}
				for _, name := range r.Databases {
					if f.databases[name].Owner != old.RoleName || f.databases[name].OwnerOID != oldOID {
						t.Fatalf("retained database %s changed", name)
					}
				}
			} else if len(f.roles) != 0 || len(f.databases) != 0 {
				t.Fatal("drop-release left native objects")
			}
		})
	}
}

func TestBindingReleaseDoesNotTrustDatabaseOwnerName(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	c, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	db := f.databases["identity"]
	db.OwnerOID++
	f.databases["identity"] = db
	if err := m.Release(t.Context(), c.BindingID, false); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.databases["identity"]; !ok {
		t.Fatal("database with unverified owner OID was dropped")
	}
}

func (f *fakeAccess) Describe() domain.ProviderInfo {
	return domain.ProviderInfo{BackupTypes: []string{"full"}}
}

func TestBackupBinderIdempotencyAndStatus(t *testing.T) {
	m, _, r, st := bindingFixture(t)
	ref := domain.ClusterRef{Name: "source"}
	c, err := st.GetCluster(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Backup.Storage = "archive"
	if err := st.UpdateClusterSpec(t.Context(), ref, c.Spec); err != nil {
		t.Fatal(err)
	}
	c, err = st.GetCluster(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	c.Status.Phase = domain.PhaseReady
	c.Status.ObservedGeneration = c.Generation
	if err := st.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateBackupStorage(t.Context(), &domain.BackupStorage{Name: "archive"}); err != nil {
		t.Fatal(err)
	}
	conn, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	first, err := m.Backup(t.Context(), conn.BindingID, "full", "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	second, err := m.Backup(t.Context(), conn.BindingID, "full", "snapshot")
	if err != nil || first != second {
		t.Fatalf("backup replay=%+v err=%v", second, err)
	}
	if _, err := m.Backup(t.Context(), conn.BindingID, "diff", "snapshot"); !errors.Is(err, engine.ErrConflict) {
		t.Fatalf("changed key content=%v", err)
	}
	pending, err := m.BackupStatus(t.Context(), conn.BindingID, first.ID)
	if err != nil || pending.Phase != PhasePending {
		t.Fatalf("status=%+v err=%v", pending, err)
	}
	backup, err := st.GetBackup(t.Context(), ref, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	backup.Phase = domain.PhaseSucceeded
	backup.Location = "snapshot"
	if err := st.UpdateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	op, err := st.GetOperation(t.Context(), backup.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	op.Phase = domain.PhaseSucceeded
	if err := st.UpdateOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	restore, err := m.Restore(t.Context(), conn.BindingID, first.ID, "restore", false)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := m.Restore(t.Context(), conn.BindingID, first.ID, "restore", false)
	if err != nil || replay != restore {
		t.Fatalf("restore replay=%+v err=%v", replay, err)
	}
	state, err := m.OperationStatus(t.Context(), conn.BindingID, restore.ID)
	if err != nil || state.Phase != PhasePending {
		t.Fatalf("operation=%+v err=%v", state, err)
	}
}

func TestRotationRecoversSameSelectedPassword(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	connection, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	f.failUpdate = true
	if _, err := m.Rotate(t.Context(), connection.BindingID); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("rotation fault=%v", err)
	}
	selected, err := m.Password(t.Context(), connection.PasswordSecretRef)
	if err != nil {
		t.Fatal(err)
	}
	// Reconstruct the controller, retaining only durable store/native state.
	m = New(m.options)
	recovered, err := m.Rotate(t.Context(), connection.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := m.Password(t.Context(), recovered.PasswordSecretRef)
	if err != nil {
		t.Fatal(err)
	}
	if selected != actual || recovered.CredentialGeneration != 2 {
		t.Fatal("recovery selected a second credential or generation")
	}
}

func TestSourceAdmissionRefusesBeforeProvisioning(t *testing.T) {
	for _, name := range []string{"absent", "outside-range", "non-postgres", "deleting"} {
		t.Run(name, func(t *testing.T) {
			m, f, r, st := bindingFixture(t)
			switch name {
			case "absent":
				r.SourceCluster = "absent"
			case "outside-range":
				r.MaxMajor = 16
			case "non-postgres":
				c, err := st.GetCluster(t.Context(), domain.ClusterRef{Name: "source"})
				if err != nil {
					t.Fatal(err)
				}
				c.Spec.Provider = "psmdb"
				if err := st.UpdateClusterSpec(t.Context(), c.Ref(), c.Spec); err != nil {
					t.Fatal(err)
				}
			case "deleting":
				if err := st.MarkClusterDeleting(t.Context(), domain.ClusterRef{Name: "source"}, domain.DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.Ensure(t.Context(), r); !errors.Is(err, ErrSourceUnavailable) {
				t.Fatalf("source refusal=%v", err)
			}
			if f.roleCreates != 0 || len(f.databases) != 0 {
				t.Fatal("unavailable source provisioned")
			}
		})
	}
}

func TestReadyEnsureDoesNotRetriggerConsumer(t *testing.T) {
	m, f, r, _ := bindingFixture(t)
	first, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	before := f.schemaCalls
	triggers := 0
	m.options.Trigger = func(domain.ClusterRef) { triggers++ }
	second, err := m.Ensure(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	if second.BindingID != first.BindingID || triggers != 0 || f.schemaCalls != before {
		t.Fatal("idempotent Ensure writes or continuously retriggers its consumer")
	}
	r.Databases = append(r.Databases, "visibility")
	if _, err := m.Ensure(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if triggers != 2 || len(f.databases) != 2 {
		t.Fatal("additive ensure did not reconcile source and consumer")
	}
}
