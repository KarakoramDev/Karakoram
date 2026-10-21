// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pgbinding

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

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
func (f *fakeAccess) OwnBindingDatabase(context.Context, *engine.Context, postgres.Database) error {
	f.schemaCalls++
	if f.failSchema {
		f.failSchema = false
		return engine.ErrUnavailable
	}
	return nil
}
func (f *fakeAccess) EnsureDatabaseExtensions(context.Context, *engine.Context, postgres.Database) error {
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
	if f.roleCreates != 1 || f.schemaCalls != 2 || f.extensionCalls != 2 || !strings.HasPrefix(c.Username, "kb_") || c.CredentialGeneration != 1 || c.Host != "sourcepx" || c.Shared {
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
