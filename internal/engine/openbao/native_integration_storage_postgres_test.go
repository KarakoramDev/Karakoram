//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/jackc/pgx/v5"
)

// nativeStorageBinding implements the shared service seam using real SQL. It is
// test-only: production inventory, source admission and sealed credential custody
// remain the responsibility of PgPlatform's binding service, not this provider.
// A mutex protects the pgx connection from concurrent agent configuration reads.
type nativeStorageBinding struct {
	mu                                     sync.Mutex
	db                                     *pgx.Conn
	endpoint                               *url.URL
	ca                                     []byte
	connection                             pgbinding.Connection
	password                               string
	ensures, resolves, passwords, releases int
	beforeRelease                          func() error
}

var _ pgbinding.Service = (*nativeStorageBinding)(nil)

func (b *nativeStorageBinding) Ensure(ctx context.Context, r pgbinding.Request) (pgbinding.Connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ensures++
	if err := r.Validate(); err != nil {
		return pgbinding.Connection{}, err
	}
	if r.ConsumerKind != "openbao" || r.SourceCluster != "nativepg" || len(r.Databases) != 1 || r.Databases[0] != "openbao" {
		return pgbinding.Connection{}, fmt.Errorf("unexpected storage binding request: %+v", r)
	}
	var version int
	if err := b.db.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer / 10000`).Scan(&version); err != nil {
		return pgbinding.Connection{}, err
	}
	if !r.Admits(version) {
		return pgbinding.Connection{}, pgbinding.ErrSourceUnavailable
	}
	id := pgbinding.BindingID(r)
	if b.connection.BindingID != "" {
		if b.connection.BindingID != id {
			return pgbinding.Connection{}, errors.New("different binding identity")
		}
		return b.connection, nil
	}
	username := "native_openbao_storage"
	b.password = "native-storage-only-password"
	role := pgx.Identifier{username}.Sanitize()
	if _, err := b.db.Exec(ctx, "CREATE ROLE "+role+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD "+nativeSQLLiteral(b.password)); err != nil {
		return pgbinding.Connection{}, err
	}
	if _, err := b.db.Exec(ctx, "CREATE DATABASE openbao OWNER "+role); err != nil {
		return pgbinding.Connection{}, err
	}
	port, err := strconv.Atoi(b.endpoint.Port())
	if err != nil {
		return pgbinding.Connection{}, err
	}
	b.connection = pgbinding.Connection{BindingID: id, Host: b.endpoint.Hostname(), Port: port, Databases: []string{"openbao"}, Username: username, PasswordSecretRef: id + "/password", TLS: pgbinding.TLS{Mode: pgbinding.TLSVerifyFull, CAPEM: b.ca}, CredentialGeneration: 1, ConsumerCount: 1}
	return b.connection, nil
}

func (b *nativeStorageBinding) Resolve(_ context.Context, id string) (pgbinding.Connection, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.resolves++
	if id == "" || id != b.connection.BindingID {
		return pgbinding.Connection{}, pgbinding.ErrSourceUnavailable
	}
	return b.connection, nil
}

func (b *nativeStorageBinding) Password(_ context.Context, ref string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.passwords++
	if ref == "" || ref != b.connection.PasswordSecretRef {
		return "", pgbinding.ErrSourceUnavailable
	}
	return b.password, nil
}

func (b *nativeStorageBinding) Release(ctx context.Context, id string, keepData bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.beforeRelease != nil {
		if err := b.beforeRelease(); err != nil {
			return err
		}
	}
	if id != b.connection.BindingID {
		return errors.New("release used wrong binding identity")
	}
	role := pgx.Identifier{b.connection.Username}.Sanitize()
	if keepData {
		if _, err := b.db.Exec(ctx, "ALTER ROLE "+role+" NOLOGIN"); err != nil {
			return err
		}
	} else {
		if _, err := b.db.Exec(ctx, "DROP DATABASE openbao"); err != nil {
			return err
		}
		if _, err := b.db.Exec(ctx, "DROP ROLE "+role); err != nil {
			return err
		}
	}
	b.releases++
	return nil
}

func nativeStorageReady(t *testing.T, h *nativeCP) {
	t.Helper()
	h.loop(t, "PostgreSQL storage Ready", func() (bool, error) {
		r, err := h.p.Sync(t.Context(), h.rc)
		if err != nil {
			return false, err
		}
		if r.ConvergenceError != nil {
			return false, r.ConvergenceError
		}
		s := state(t, h.rc)
		if s.Phase != "ready" || h.rc.Cluster.Status.ObservedGeneration != h.rc.Cluster.Generation || len(s.Members) != 3 {
			return false, nil
		}
		active := 0
		for _, m := range s.Members {
			if !m.Observed || !m.Initialized || !m.Healthy || m.Sealed || m.Voter {
				return false, nil
			}
			// Persisted readiness can briefly outlive a stopped agent. Require
			// live native health and HA observations before returning Ready.
			client, err := h.p.nativeClient(t.Context(), h.rc, m.Name, "")
			if err != nil {
				return false, err
			}
			health, err := client.Health(t.Context())
			if err != nil {
				return false, err
			}
			if !health.Initialized || health.Sealed {
				return false, nil
			}
			leader, err := client.Leader(t.Context())
			if err != nil {
				return false, err
			}
			if !leader.HAEnabled || leader.LeaderAddress == "" {
				return false, nil
			}
			if leader.IsSelf {
				active++
			}
		}
		return active == 1, nil
	})
}

func TestNativeControlPlanePostgresStorage(t *testing.T) {
	binary := os.Getenv("KARAKORAM_OPENBAO_BIN")
	if binary == "" {
		t.Skip("set KARAKORAM_OPENBAO_BIN to an installed OpenBao binary")
	}
	for _, keepData := range []bool{false, true} {
		t.Run(fmt.Sprintf("keepData=%t", keepData), func(t *testing.T) {
			ctx := t.Context()
			dsn, ca := nativePostgresServer(t)
			admin, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = admin.Close(context.Background()) })
			u, err := url.Parse(dsn)
			if err != nil {
				t.Fatal(err)
			}
			binding := &nativeStorageBinding{db: admin, endpoint: u, ca: ca}
			h := newNativeCP(t, binary)
			h.p.options.Bindings = binding
			h.rc.Cluster.Spec.EngineOptions = []byte(`{"storage":{"type":"postgresql","postgresql":{"cluster":"nativepg"}}}`)
			if err = h.p.Validate(ctx, nil, h.rc.Cluster); err != nil {
				t.Fatal("admit native binding-backed storage: ", err)
			}
			nativeStorageReady(t, h)
			binding.mu.Lock()
			exercised := binding.ensures > 0 && binding.resolves > 0 && binding.passwords > 0
			binding.mu.Unlock()
			if !exercised {
				t.Fatal("provider did not exercise Ensure, Resolve and Password")
			}
			status, err := h.p.Status(ctx, h.rc)
			if err != nil {
				t.Fatal(err)
			}
			roles := map[string]int{}
			for _, m := range status.Members {
				roles[m.Role]++
			}
			if roles["primary"] != 1 || roles["secondary"] != 2 {
				t.Fatalf("native HA health: %v", roles)
			}
			client := h.root(t)
			if err = client.Do(ctx, "POST", "sys/mounts/persisted", map[string]any{"type": "kv"}, nil); err != nil {
				t.Fatal(err)
			}
			if err = client.Do(ctx, "POST", "persisted/proof", map[string]any{"value": "postgresql-backed"}, nil); err != nil {
				t.Fatal(err)
			}
			// Inspect the real database via the bound login: the storage and HA
			// tables must be present and contain actual OpenBao state/HA locks.
			storageURL := *u
			storageURL.Path = "/openbao"
			storageURL.User = url.UserPassword(binding.connection.Username, binding.password)
			storage, err := pgx.Connect(ctx, storageURL.String())
			if err != nil {
				t.Fatal(err)
			}
			var values, locks int
			err = storage.QueryRow(ctx, `SELECT (SELECT count(*) FROM openbao_kv_store), (SELECT count(*) FROM openbao_ha_locks)`).Scan(&values, &locks)
			_ = storage.Close(ctx)
			if err != nil || values == 0 || locks == 0 {
				t.Fatalf("native PostgreSQL storage rows=%d HA locks=%d err=%v", values, locks, err)
			}
			// Stop every agent/process, discard every member's local volume,
			// then recover using only PostgreSQL state and CP-held seal material.
			for name := range h.nodes {
				if err = h.stop(name); err != nil {
					t.Fatal(err)
				}
			}
			for name, n := range h.nodes {
				if err = os.RemoveAll(n.dir); err != nil {
					t.Fatal(err)
				}
				n.dir = ""
				if err = h.start(ctx, name); err != nil {
					t.Fatal(err)
				}
			}
			nativeStorageReady(t, h)
			var persisted struct {
				Data map[string]any `json:"data"`
			}
			if err = h.root(t).Do(ctx, "GET", "persisted/proof", nil, &persisted); err != nil || persisted.Data["value"] != "postgresql-backed" {
				t.Fatalf("PostgreSQL persistence after all native volumes replaced: %+v err=%v", persisted, err)
			}
			binding.beforeRelease = func() error {
				for name, n := range h.nodes {
					if n.cancel != nil || h.services[name] != nil {
						return fmt.Errorf("binding released before runtime %s terminated", name)
					}
				}
				return nil
			}
			h.loop(t, "PostgreSQL binding cleanup", func() (bool, error) { return h.p.Cleanup(ctx, h.rc, domain.DeleteOptions{KeepVolumes: keepData}) })
			if binding.releases != 1 {
				t.Fatalf("binding release count: %d", binding.releases)
			}
			var databaseExists, roleExists, login bool
			if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname='openbao'), EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1), COALESCE((SELECT rolcanlogin FROM pg_roles WHERE rolname=$1), false)`, binding.connection.Username).Scan(&databaseExists, &roleExists, &login); err != nil {
				t.Fatal(err)
			}
			if databaseExists != keepData || roleExists != keepData || login {
				t.Fatalf("release keepData=%t: database=%t role=%t login=%t", keepData, databaseExists, roleExists, login)
			}
		})
	}
}
