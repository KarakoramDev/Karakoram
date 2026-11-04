// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestObservedServerVersion(t *testing.T) {
	for _, tc := range []struct {
		serverVersion, description, want string
	}{
		{"17.11", "17.11 - Percona Server for PostgreSQL 17.11.1 - Percona Distribution", "17.11.1"},
		{"16.14", "16.14 - Percona Server for PostgreSQL 16.14.1 - Percona Distribution", "16.14.1"},
		{"16.15 - Percona Distribution", "PostgreSQL 16.15 - Percona Distribution on x86_64-pc-linux-gnu", "16.15"},
		{"18.6", "PostgreSQL 18.6 on x86_64-pc-linux-gnu", "18.6"},
		{"17.11", "Percona Server for PostgreSQL ", "17.11"},
	} {
		if got := observedServerVersion(tc.serverVersion, tc.description); got != tc.want {
			t.Fatalf("observedServerVersion(%q, %q)=%q, want %q", tc.serverVersion, tc.description, got, tc.want)
		}
	}
}

func TestStatusPendingRestartAuthority(t *testing.T) {
	for _, tc := range []struct {
		name          string
		topology      string
		nativePending bool
		patroni       string
		unavailable   bool
		want          bool
	}{
		// Adoption leaves all six recovery_target* flags set even though their
		// values equal boot/reset defaults and no pg_file_settings rows remain.
		{"adopted recovery targets", pgspec.TopologyHA, true, `{}`, false, false},
		{"Patroni explicitly cleared", pgspec.TopologyHA, true, `{"pending_restart":false}`, false, false},
		{"shared buffers pending", pgspec.TopologyHA, true, `{"pending_restart":true}`, false, true},
		{"Patroni detects change before SQL", pgspec.TopologyHA, false, `{"pending_restart":true}`, false, true},
		{"Patroni unavailable does not trust residue", pgspec.TopologyHA, true, `{}`, true, false},
		{"single max connections pending", pgspec.TopologySingle, true, ``, false, true},
		{"single unchanged", pgspec.TopologySingle, false, ``, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testSpec()
			s.Topology = tc.topology
			s.Version = "17.10.1"
			p, nativeQueries := statusSQLFixture(t, tc.nativePending)
			if s.Topology == pgspec.TopologyHA {
				s.CAPEM, s.ServerCert = testTLS(t)
				s.Peers = []pgspec.Peer{{Name: s.Member, Host: "127.0.0.1"}}
				pair, err := tls.X509KeyPair([]byte(s.ServerCert.CertPEM), []byte(s.ServerCert.KeyPEM))
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/patroni":
						if tc.unavailable {
							w.WriteHeader(http.StatusServiceUnavailable)
							return
						}
						_, _ = w.Write([]byte(tc.patroni))
					case "/cluster":
						_ = json.NewEncoder(w).Encode(map[string]any{"members": []map[string]string{{"name": s.Member, "role": "primary"}}})
					case "/primary":
						w.WriteHeader(http.StatusOK)
					default:
						http.NotFound(w, r)
					}
				}))
				_ = server.Listener.Close()
				server.Listener, err = net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", pgspec.PortPatroni))
				if err != nil {
					t.Fatal(err)
				}
				server.TLS = &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}
				server.StartTLS()
				t.Cleanup(server.Close)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			st := New().status(ctx, s, p)
			if !st.Running || st.PendingRestart != tc.want {
				t.Fatalf("status running=%t pending_restart=%t, want true/%t", st.Running, st.PendingRestart, tc.want)
			}
			if st.Version != "17.11.1" {
				t.Fatalf("status version=%q, want running release, not assigned %q", st.Version, s.Version)
			}
			wantQueries := int32(0)
			if s.Topology == pgspec.TopologySingle {
				wantQueries = 1
			}
			if got := nativeQueries.Load(); got != wantQueries {
				t.Fatalf("native pending-restart queries=%d, want %d", got, wantQueries)
			}
			if !reflect.DeepEqual(st.SharedMemoryParameters, map[string]int64{"max_connections": 100, "max_prepared_transactions": 0, "max_locks_per_transaction": 64, "max_wal_senders": 10, "max_worker_processes": 8}) {
				t.Fatalf("running shared-memory confirmation lost: %v", st.SharedMemoryParameters)
			}
		})
	}
}

// Exercise Runner.status through a local PostgreSQL wire connection, including
// pgx's prepared-statement protocol, without requiring a native database install.
func statusSQLFixture(t *testing.T, pending bool) (paths, *atomic.Int32) {
	t.Helper()
	socket, err := os.MkdirTemp("", "kk-pg-status-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socket) })
	p := dataPaths(t.TempDir())
	p.socket = socket
	listener, err := net.Listen("unix", filepath.Join(socket, ".s.PGSQL.5432"))
	if err != nil {
		t.Fatal(err)
	}
	var nativeQueries atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		backend := pgproto3.NewBackend(conn, conn)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			return
		}
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if backend.Flush() != nil {
			return
		}
		row := func(query string) ([]pgproto3.FieldDescription, []any) {
			var oids []uint32
			var values []any
			switch {
			case strings.Contains(query, "pg_catalog.version()"):
				oids, values = []uint32{pgtype.TextOID, pgtype.TextOID}, []any{"17.11", "17.11 - Percona Server for PostgreSQL 17.11.1 - Percona Distribution"}
			case strings.Contains(query, "FROM pg_catalog.pg_settings"):
				nativeQueries.Add(1)
				oids, values = []uint32{pgtype.BoolOID}, []any{pending}
			case strings.Contains(query, "FROM pg_catalog.pg_database"):
				oids, values = []uint32{pgtype.TextOID}, []any{"C"}
			case strings.Contains(query, "pg_replication_slots"):
				oids, values = []uint32{pgtype.JSONBOID}, []any{[]pgspec.ReplicationSlot{{Name: "test_slot", Type: "logical", RetainedBytes: 2 << 30, WALStatus: "extended"}}}
			case strings.Contains(query, "jsonb_build_object"):
				oids = []uint32{pgtype.JSONBOID}
				values = []any{map[string]int64{"max_connections": 100, "max_prepared_transactions": 0, "max_locks_per_transaction": 64, "max_wal_senders": 10, "max_worker_processes": 8}}
			case strings.Contains(query, "pg_is_in_recovery"):
				oids, values = []uint32{pgtype.BoolOID, pgtype.TextOID}, []any{false, "123456789"}
			case strings.Contains(query, "pg_control_checkpoint"):
				oids, values = []uint32{pgtype.Int4OID}, []any{int32(1)}
			case strings.Contains(query, "pg_stat_archiver"):
				oids, values = []uint32{pgtype.TextOID, pgtype.BoolOID}, []any{"", false}
			case strings.Contains(query, "pg_current_wal_lsn()::text"):
				oids, values = []uint32{pgtype.TextOID}, []any{"0/100"}
			case strings.Contains(query, "pg_stat_replication"):
				oids, values = []uint32{pgtype.Int8OID, pgtype.TextOID}, []any{int64(0), "async"}
			case strings.Contains(query, "pg_ls_waldir"):
				oids, values = []uint32{pgtype.Int8OID, pgtype.Int8OID}, []any{int64(16 << 20), int64(2)}
			default:
				t.Errorf("unexpected status query: %s", query)
			}
			fields := make([]pgproto3.FieldDescription, len(oids))
			for i, oid := range oids {
				fields[i] = pgproto3.FieldDescription{Name: []byte(fmt.Sprintf("f%d", i)), DataTypeOID: oid, DataTypeSize: -1}
			}
			return fields, values
		}
		statements := map[string]string{}
		var fields []pgproto3.FieldDescription
		var values []any
		var formats []int16
		types := pgtype.NewMap()
		for {
			msg, err := backend.Receive()
			if err != nil {
				return
			}
			switch msg := msg.(type) {
			case *pgproto3.Parse:
				statements[msg.Name] = msg.Query
				backend.Send(&pgproto3.ParseComplete{})
			case *pgproto3.Describe:
				if msg.ObjectType == 'S' {
					fields, values = row(statements[msg.Name])
					backend.Send(&pgproto3.ParameterDescription{})
				} else {
					for i := range fields {
						fields[i].Format = 0
						if len(formats) == 1 {
							fields[i].Format = formats[0]
						} else if len(formats) > 1 {
							fields[i].Format = formats[i]
						}
					}
				}
				backend.Send(&pgproto3.RowDescription{Fields: fields})
			case *pgproto3.Bind:
				formats = msg.ResultFormatCodes
				backend.Send(&pgproto3.BindComplete{})
			case *pgproto3.Execute:
				encoded := make([][]byte, len(fields))
				for i, field := range fields {
					format := int16(0)
					if len(formats) == 1 {
						format = formats[0]
					} else if len(formats) > 1 {
						format = formats[i]
					}
					encoded[i], err = types.Encode(field.DataTypeOID, format, values[i], nil)
					if err != nil {
						t.Errorf("encoding status row: %v", err)
						return
					}
				}
				backend.Send(&pgproto3.DataRow{Values: encoded})
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case *pgproto3.Sync:
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			case *pgproto3.Close:
				backend.Send(&pgproto3.CloseComplete{})
			case *pgproto3.Terminate:
				return
			default:
				t.Errorf("unexpected status protocol message: %T", msg)
				return
			}
			if backend.Flush() != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	return p, &nativeQueries
}
