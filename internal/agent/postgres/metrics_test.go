// SPDX-License-Identifier: Apache-2.0
package postgres

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestWALLagMetricHelp(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		t.Run(fmt.Sprint(recovery), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				b := pgproto3.NewBackend(conn, conn)
				if _, err := b.ReceiveStartupMessage(); err != nil {
					return
				}
				b.Send(&pgproto3.AuthenticationOk{})
				b.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
				b.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if b.Flush() != nil {
					return
				}
				for {
					msg, err := b.Receive()
					if err != nil {
						return
					}
					q, ok := msg.(*pgproto3.Query)
					if !ok {
						return
					}
					switch {
					case strings.Contains(q.String, "pg_stat_database"):
						b.Send(&pgproto3.RowDescription{})
					case strings.Contains(q.String, "pg_is_in_recovery"):
						value := "f"
						if recovery {
							value = "t"
						}
						b.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("recovery"), DataTypeOID: 16, DataTypeSize: 1}}})
						b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(value)}})
					case strings.Contains(q.String, "pg_wal_lsn_diff"):
						b.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("lag"), DataTypeOID: 701, DataTypeSize: 8}}})
						b.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("42")}})
					default:
						b.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "not collected"})
						b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
						if b.Flush() != nil {
							return
						}
						continue
					}
					b.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
					b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if b.Flush() != nil {
						return
					}
				}
			}()
			c, err := pgx.Connect(t.Context(), "postgres://postgres@"+listener.Addr().String()+"/postgres?sslmode=disable&default_query_exec_mode=simple_protocol")
			if err != nil {
				t.Fatal(err)
			}
			metrics := databaseMetrics(t.Context(), c)
			_ = c.Close(t.Context())
			<-done
			name, help := "karakoram_postgres_replication_lag_bytes", "including transport and replay lag"
			if recovery {
				name, help = "karakoram_postgres_replay_backlog_bytes", "excludes transport lag"
			}
			for _, m := range metrics {
				if m.Name == name {
					if m.Value != 42 || m.Counter || !strings.Contains(m.Help, help) {
						t.Fatal(m)
					}
					return
				}
			}
			t.Fatal("missing WAL lag metric", metrics)
		})
	}
}
