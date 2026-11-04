// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

type citusAuthWire struct {
	mu                         sync.Mutex
	queries                    []string
	failUpdate, missingDefault bool
}

func (f *citusAuthWire) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

func citusAuthSQLFixture(t *testing.T, fail, missing bool) (*pgx.Conn, *citusAuthWire) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &citusAuthWire{failUpdate: fail, missingDefault: missing}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
		backend := pgproto3.NewBackend(conn, conn)
		if _, err = backend.ReceiveStartupMessage(); err != nil {
			return
		}
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if backend.Flush() != nil {
			return
		}
		state := byte('I')
		for {
			message, err := backend.Receive()
			if err != nil {
				return
			}
			if _, ok := message.(*pgproto3.Terminate); ok {
				return
			}
			query, ok := message.(*pgproto3.Query)
			if !ok {
				t.Errorf("unexpected authinfo protocol message %T", message)
				return
			}
			fixture.mu.Lock()
			fixture.queries = append(fixture.queries, query.String)
			failUpdate, missing := fixture.failUpdate, fixture.missingDefault
			fixture.mu.Unlock()
			sql := strings.ToLower(query.String)
			tag := "SET"
			switch {
			case sql == "begin":
				state = 'T'
				tag = "BEGIN"
			case sql == "commit":
				state = 'I'
				tag = "COMMIT"
			case sql == "rollback":
				state = 'I'
				tag = "ROLLBACK"
			case strings.HasPrefix(sql, "update pg_catalog.pg_dist_authinfo"):
				if failUpdate {
					state = 'E'
					backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "private authentication SQL and password"})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: state})
					if backend.Flush() != nil {
						return
					}
					continue
				}
				tag = "UPDATE 1"
				if missing {
					tag = "UPDATE 0"
				}
			case strings.HasPrefix(sql, "delete from pg_catalog.pg_dist_authinfo"):
				tag = "DELETE 1"
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: state})
			if backend.Flush() != nil {
				return
			}
		}
	}()
	config, err := pgx.ParseConfig("postgres://postgres@" + listener.Addr().String() + "/app?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(t.Context()); _ = listener.Close(); <-done })
	return conn, fixture
}

func TestCitusOutboundAuthenticationUsesTargetPasswordAndLocalCertificatePaths(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready-upgrade", true: "restore-held"}[restore], func(t *testing.T) {
			conn, fixture := citusAuthSQLFixture(t, false, false)
			spec := testSpec()
			spec.Citus = &pgspec.Citus{Database: "app"}
			spec.Superuser.Password = "target password'\\with:escapes"
			spec.CAPEM, spec.ServerCert = testTLS(t)
			spec.Postgres.RecoveryClientsHeld = restore
			p := dataPaths(t.TempDir())
			runner := New()
			if err := runner.refreshCitusAuthentication(t.Context(), conn, spec, p, 1); err != nil {
				t.Fatal(err)
			}
			queries := fixture.snapshot()
			sql := strings.Join(queries, ";\n")
			firstWrite := strings.Index(sql, "UPDATE pg_catalog.pg_dist_authinfo")
			for _, guard := range []string{"begin", "SET LOCAL citus.enable_ddl_propagation = off", "SET LOCAL citus.enable_metadata_sync = off"} {
				if index := strings.Index(sql, guard); index < 0 || index >= firstWrite {
					t.Fatal("outbound credential repair lacks local transaction guard", guard)
				}
			}
			if strings.Contains(sql, "synchronous_commit = local") != restore {
				t.Fatal("recovery commit policy escaped recovery scope")
			}
			compact := strings.ReplaceAll(sql, " ", "")
			if !strings.Contains(compact, "WHEREnodeid=0ANDrolename='postgres'") || !strings.Contains(compact, "WHERErolename='postgres'ANDnodeid<>0") {
				t.Fatal("managed default or overriding node/loopback credentials were not reconciled")
			}
			if !strings.Contains(sql, p.root) || !strings.Contains(sql, "server.pem") || !strings.Contains(sql, "server.key") || !strings.Contains(sql, "commit") {
				t.Fatal("target certificate paths or commit absent")
			}
			if err := runner.refreshCitusAuthentication(t.Context(), conn, spec, p, 1); err != nil {
				t.Fatal(err)
			}
			if len(fixture.snapshot()) != len(queries) {
				t.Fatal("unchanged primary authentication caused another write")
			}
			if err := runner.refreshCitusAuthentication(t.Context(), conn, spec, p, 2); err != nil {
				t.Fatal(err)
			}
			if len(fixture.snapshot()) == len(queries) {
				t.Fatal("promotion timeline failed to invalidate inherited credential paths")
			}
			before := len(fixture.snapshot())
			spec.Superuser.Password = "changed-target-password"
			if err := runner.refreshCitusAuthentication(t.Context(), conn, spec, p, 2); err != nil {
				t.Fatal(err)
			}
			if len(fixture.snapshot()) == before {
				t.Fatal("target credential change did not refresh outbound authentication")
			}
			before = len(fixture.snapshot())
			p.root += "/new-member"
			if err := runner.refreshCitusAuthentication(t.Context(), conn, spec, p, 2); err != nil {
				t.Fatal(err)
			}
			if len(fixture.snapshot()) == before {
				t.Fatal("local certificate directory change was not reconciled")
			}
		})
	}
}

func TestCitusOutboundAuthenticationFailureRetriesWithoutCachingOrLeaking(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "write-failure", true: "bootstrap-not-inserted"}[missing], func(t *testing.T) {
			conn, fixture := citusAuthSQLFixture(t, !missing, missing)
			spec := testSpec()
			spec.Citus = &pgspec.Citus{Database: "app"}
			runner := New()
			p := dataPaths(t.TempDir())
			err := runner.refreshCitusAuthentication(t.Context(), conn, spec, p, 1)
			if err == nil || runner.citusAuthApplied || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), spec.Superuser.Password) {
				t.Fatal("failed authentication repair cached success or leaked native diagnostics")
			}
			sql := strings.Join(fixture.snapshot(), ";\n")
			if strings.Contains(sql, "INSERT INTO") || !strings.Contains(sql, "rollback") {
				t.Fatal("failed repair raced Patroni bootstrap insertion or omitted rollback")
			}
			fixture.mu.Lock()
			fixture.failUpdate = false
			fixture.missingDefault = false
			fixture.mu.Unlock()
			if err := runner.refreshCitusAuthentication(t.Context(), conn, spec, p, 1); err != nil || !runner.citusAuthApplied {
				t.Fatal("failed repair did not retry", err)
			}
		})
	}
	if err := New().refreshCitusAuthentication(t.Context(), nil, testSpec(), paths{}, 1); err != nil {
		t.Fatal("non-Citus authentication changed", err)
	}
}

func TestCitusAuthConninfoEscapesLibpqValues(t *testing.T) {
	password := "spaces 'quotes' \\ : delimiters"
	config := citusAuthConfig{Credentials: pgspec.Credentials{Username: "postgres", Password: password}}
	info := citusAuthConninfo(config)
	parsed, err := pgx.ParseConfig("host=localhost user=postgres sslmode=disable " + info)
	if err != nil || parsed.Password != password {
		t.Fatal("target password changed during libpq quoting", err)
	}
}

func TestCitusConnectivityCannotBypassRecoveryClientFences(t *testing.T) {
	for _, bootstrap := range []bool{false, true} {
		spec := testSpec()
		spec.Citus = &pgspec.Citus{Database: "app"}
		if bootstrap {
			spec.Bootstrap = &pgspec.Bootstrap{Method: "restore"}
		} else {
			spec.Postgres.RecoveryClientsHeld = true
		}
		_, err := runCitusAction(t.Context(), spec, paths{}, pgspec.ActionCitusConnectivity, &struct{}{})
		if err == nil || err.Error() != "citus connectivity requires released recovery clients" {
			t.Fatal("remote SQL was attempted before client fence release", err)
		}
	}
}
