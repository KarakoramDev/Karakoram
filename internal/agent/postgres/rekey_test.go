// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestCitusSystemRoleBootstrapIsLocalTransaction(t *testing.T) {
	for _, restore := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "restore"}[restore], func(t *testing.T) {
			s := testSpec()
			s.Citus = &pgspec.Citus{Database: "app"}
			if restore {
				s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &pgspec.Restore{}}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			queries := make(chan []string, 1)
			go func() {
				var seen []string
				defer func() { queries <- seen }()
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				b := pgproto3.NewBackend(conn, conn)
				if _, err = b.ReceiveStartupMessage(); err != nil {
					return
				}
				b.Send(&pgproto3.AuthenticationOk{})
				b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = b.Flush()
				for {
					msg, err := b.Receive()
					if err != nil {
						return
					}
					q, ok := msg.(*pgproto3.Query)
					if !ok {
						return
					}
					seen = append(seen, q.String)
					b.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
					status := byte('T')
					if q.String == "commit" {
						status = 'I'
					}
					b.Send(&pgproto3.ReadyForQuery{TxStatus: status})
					if b.Flush() != nil {
						return
					}
				}
			}()
			c, err := pgx.Connect(t.Context(), "postgres://postgres@"+listener.Addr().String()+"/app?sslmode=disable")
			if err != nil {
				t.Fatal(err)
			}
			err = bootstrapRoles(t.Context(), c, s)
			c.Close(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			seen := <-queries
			assertLocalRoleTransaction(t, strings.Join(seen, ";\n"), restore)
			files, err := render(s, dataPaths(t.TempDir()), nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if file.name == "roles.sql" {
					assertLocalRoleTransaction(t, strings.ToLower(file.content), restore)
				}
			}
		})
	}
}

func assertLocalRoleTransaction(t *testing.T, sql string, restore bool) {
	t.Helper()
	sql = strings.ToLower(sql)
	guard := strings.Index(sql, "set local citus.enable_ddl_propagation = off")
	firstRole := strings.Index(sql, "do $kk$")
	if !strings.HasPrefix(sql, "begin") || !strings.Contains(sql, "commit") || guard < 0 || guard >= firstRole {
		t.Fatal("system identities can propagate outside their local transaction")
	}
	if strings.Contains(sql, "synchronous_commit = local") != restore {
		t.Fatal("restore commit policy escaped its recovery scope")
	}
}

func TestSystemRekeyNativeFailureClassification(t *testing.T) {
	for _, code := range []string{"0A000", "42501", "42704", "28P01", "08006", "40001", "40P01", "XX000", ""} {
		err := systemRoleFailure(&pgconn.PgError{Code: code, Message: "private SQL password"})
		status, message := rekeyErrorResponse(err)
		permanent := code == "0A000" || code == "42501" || code == "42704" || code == "28P01"
		if (status == http.StatusUnprocessableEntity) != permanent || strings.Contains(err.Error()+message, "private") {
			t.Fatalf("SQLSTATE %s: unsafe classification %d", code, status)
		}
		if !permanent && status != http.StatusServiceUnavailable {
			t.Fatalf("SQLSTATE %s: transient failure mislabeled as conflict", code)
		}
	}
	if status, _ := rekeyErrorResponse(errors.New("primary not ready")); status != http.StatusServiceUnavailable {
		t.Fatal("primary readiness must remain retryable")
	}
}
