// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Model the preloaded Timescale loader: ANY preceding query (including a
// catalog-only SELECT) loads the old version and makes UPDATE fail with 0A000.
func TestExtensionUpdateFreshTimescaleBackend(t *testing.T) {
	for _, updateError := range []string{"", "0A000", "08006"} {
		t.Run("SQLSTATE_"+updateError, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			var wg sync.WaitGroup
			var mu sync.Mutex
			var updated []string
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						defer conn.Close()
						_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
						b := pgproto3.NewBackend(conn, conn)
						startup, err := b.ReceiveStartupMessage()
						if err != nil {
							return
						}
						database := startup.(*pgproto3.StartupMessage).Parameters["database"]
						b.Send(&pgproto3.AuthenticationOk{})
						b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
						b.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
						b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
						if b.Flush() != nil {
							return
						}
						statements := 0
						for {
							msg, err := b.Receive()
							if err != nil {
								return
							}
							q, ok := msg.(*pgproto3.Query)
							if !ok {
								return
							}
							statements++
							code := ""
							switch {
							case strings.Contains(q.String, "FROM pg_catalog.pg_database"):
								b.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("datname"), DataTypeOID: 25, DataTypeSize: -1}}})
								for _, db := range []string{"app", "empty", "postgres"} {
									b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(db)}})
								}
							case strings.Contains(q.String, "FROM pg_catalog.pg_extension"):
								installed := "t"
								if database == "empty" {
									installed = "f"
								}
								b.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("exists"), DataTypeOID: 16, DataTypeSize: 1}}})
								b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(installed)}})
							case q.String == updateExtensionStatement("timescaledb"):
								code = updateError
								if statements != 1 {
									code = "0A000"
								}
								mu.Lock()
								updated = append(updated, database)
								mu.Unlock()
							case q.String == updateExtensionStatement("vector"), q.String == updateExtensionStatement("postgis"):
							default:
								code = "XX000"
							}
							if code != "" {
								b.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: "private native diagnostic"})
							} else {
								b.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
							}
							b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
							if b.Flush() != nil {
								return
							}
						}
					}()
				}
			}()
			defer wg.Wait()
			defer listener.Close()
			config, err := pgx.ParseConfig(fmt.Sprintf("postgres://test@%s/postgres?sslmode=disable", listener.Addr()))
			if err != nil {
				t.Fatal(err)
			}
			config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c, err := pgx.ConnectConfig(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(context.Background())
			names := []string{"vector", "timescaledb", "postgis"}
			results, err := updateInstalledExtensions(ctx, c, names)
			if updateError != "" {
				want := 400
				if updateError == "08006" {
					want = 503
				}
				if err == nil || accessFailure(err).(*accessError).status != want {
					t.Fatalf("error = %v, want HTTP%d", err, want)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 3 || !reflect.DeepEqual(results[0].Updated, names) || len(results[1].Updated) != 0 || !reflect.DeepEqual(results[2].Updated, names) {
				t.Fatalf("extension results: %+v", results)
			}
			mu.Lock()
			defer mu.Unlock()
			if !reflect.DeepEqual(updated, []string{"app", "postgres"}) {
				t.Fatalf("Timescale update databases: %v", updated)
			}
		})
	}
}

// Keep SQLSTATE error wrapping covered without exposing the native message.
func TestExtensionUnsupportedErrorSanitized(t *testing.T) {
	err := accessFailure(fmt.Errorf("extension update: %w", &pgconn.PgError{Code: "0A000", Message: "private native diagnostic"})).(*accessError)
	if err.status != 400 || strings.Contains(err.message, "private") {
		t.Fatalf("unsafe extension rejection: %+v", err)
	}
}
