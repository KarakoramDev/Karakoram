// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Admin memory matches the intent, but runtime and disk have independently
// lost their rows. A repeated Apply must repair both without a memory edit.
func TestProxyRepairsRuntimeAndDiskDrift(t *testing.T) {
	p := &pgspec.Proxy{Admin: pgspec.Credentials{Username: "admin", Password: "test"}, Servers: []string{"db"}, Users: []pgspec.ProxyUser{{Username: "app", Password: "test"}}}
	c := proxyDriftFixture(t, p, false)
	plan, err := proxyPlan(t.Context(), c, p, "", "")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	for _, want := range []string{"LOAD PGSQL SERVERS TO RUNTIME", "SAVE PGSQL SERVERS TO DISK", "LOAD PGSQL USERS TO RUNTIME", "SAVE PGSQL USERS TO DISK", "LOAD PGSQL VARIABLES TO RUNTIME", "SAVE PGSQL VARIABLES TO DISK", "LOAD ADMIN VARIABLES TO RUNTIME", "SAVE ADMIN VARIABLES TO DISK"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing layer repair %s", want)
		}
	}
}

func proxyDriftFixture(t *testing.T, p *pgspec.Proxy, failSave bool) *pgx.Conn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
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
			if failSave && q.String == "SAVE PGSQL SERVERS TO DISK" {
				failSave = false
				b.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "injected persistence failure"})
				b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if b.Flush() != nil {
					return
				}
				continue
			}
			if strings.HasPrefix(q.String, "SELECT ") {
				columns, table, _ := strings.Cut(strings.TrimPrefix(q.String, "SELECT "), " FROM ")
				cols := strings.Split(columns, ",")
				fields := make([]pgproto3.FieldDescription, len(cols))
				for i, col := range cols {
					fields[i] = pgproto3.FieldDescription{Name: []byte(col), DataTypeOID: 25, DataTypeSize: -1}
				}
				b.Send(&pgproto3.RowDescription{Fields: fields})
				var rows []tableRow
				switch table {
				case "pgsql_servers":
					for _, host := range p.Servers {
						rows = append(rows, tableRow{"hostname": host, "port": "5432", "hostgroup_id": "20", "max_connections": "100", "use_ssl": "0"})
					}
				case "pgsql_users":
					for _, user := range p.Users {
						rows = append(rows, tableRow{"username": user.Username, "password": user.Password, "active": "1", "default_hostgroup": "10", "transaction_persistent": "1", "use_ssl": "0"})
					}
				case "pgsql_replication_hostgroups":
					rows = []tableRow{{"writer_hostgroup": "10", "reader_hostgroup": "20", "check_type": "read_only"}}
				case "global_variables":
					settings := proxyRuntimeSettings(p, "")
					for k, v := range proxyStartupSettings(p, "") {
						settings[k] = v
					}
					settings["admin-admin_credentials"] = "admin:test"
					settings["admin-mysql_ifaces"] = "127.0.0.1:6032"
					settings["admin-pgsql_ifaces"] = "127.0.0.1:6132"
					for k, v := range settings {
						rows = append(rows, tableRow{"variable_name": k, "variable_value": v})
					}
				}
				for _, row := range rows {
					values := make([][]byte, len(cols))
					for i, col := range cols {
						values[i] = []byte(row[col])
					}
					b.Send(&pgproto3.DataRow{Values: values})
				}
			}
			b.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
			b.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if b.Flush() != nil {
				return
			}
		}
	}()
	cfg, err := pgx.ParseConfig("postgres://admin:test@" + listener.Addr().String() + "/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	c, err := pgx.ConnectConfig(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()); _ = listener.Close(); <-done })
	return c
}

func TestProxyRetriesFailedSaveWithMatchingMemory(t *testing.T) {
	p := &pgspec.Proxy{Admin: pgspec.Credentials{Username: "admin", Password: "test"}}
	c := proxyDriftFixture(t, p, true)
	if _, err := reconcileProxy(t.Context(), c, p, "", ""); err == nil {
		t.Fatal("injected SAVE failure not returned")
	}
	if _, err := reconcileProxy(t.Context(), c, p, "", ""); err != nil {
		t.Fatal("SAVE not retried from unchanged memory", err)
	}
}
