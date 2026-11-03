// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestProxyServingReadinessRequiresNativeWriter(t *testing.T) {
	for _, condition := range []string{"serving", "shunned", "reader-only", "no-backends", "missing-user", "frontend-down", "unconfigured", "data", "bootstrap"} {
		t.Run(condition, func(t *testing.T) {
			runner := New()
			probe, ok := any(runner).(interface{ ProxyServingReadiness(context.Context) error })
			if !ok {
				t.Fatal("PostgreSQL runner lacks native serving readiness hook")
			}
			runner.current = pgspec.Spec{Role: pgspec.RoleProxy, ListenHost: "0.0.0.0", Proxy: &pgspec.Proxy{Servers: []string{"db"}, Admin: pgspec.Credentials{Username: "admin", Password: "test"}, Users: []pgspec.ProxyUser{{Username: "app", Password: "test"}}}}
			runner.dir = t.TempDir()
			switch condition {
			case "unconfigured":
				runner.current = pgspec.Spec{}
			case "data":
				runner.current.Role = pgspec.RoleData
			case "bootstrap":
				runner.current.Proxy.Servers = nil
			}
			if condition != "frontend-down" {
				listener, err := net.Listen("tcp", "127.0.0.1:5432")
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
			}
			proxyServingAdminFixture(t, condition)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := probe.ProxyServingReadiness(ctx); (err == nil) != (condition == "serving") {
				t.Fatalf("serving readiness=%v condition=%s", err, condition)
			}
		})
	}
}

func proxyServingAdminFixture(t *testing.T, condition string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:6132")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
				backend := pgproto3.NewBackend(conn, conn)
				if _, err := backend.ReceiveStartupMessage(); err != nil {
					return
				}
				backend.Send(&pgproto3.AuthenticationOk{})
				backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
				backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if backend.Flush() != nil {
					return
				}
				for {
					msg, err := backend.Receive()
					if err != nil {
						return
					}
					query, ok := msg.(*pgproto3.Query)
					if !ok {
						return
					}
					columns, table, _ := strings.Cut(strings.TrimPrefix(query.String, "SELECT "), " FROM ")
					names := strings.Split(columns, ",")
					fields := make([]pgproto3.FieldDescription, len(names))
					for i, name := range names {
						fields[i] = pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: 25, DataTypeSize: -1}
					}
					backend.Send(&pgproto3.RowDescription{Fields: fields})
					var values [][]byte
					switch table {
					case "runtime_pgsql_servers":
						if condition != "no-backends" {
							group, status := "10", "ONLINE"
							if condition == "shunned" {
								status = "SHUNNED"
							}
							if condition == "reader-only" {
								group = "20"
							}
							values = [][]byte{[]byte("db"), []byte(group), []byte(status)}
						}
					case "runtime_pgsql_users":
						if condition != "missing-user" {
							values = [][]byte{[]byte("app"), []byte("1")}
						}
					}
					if values != nil {
						backend.Send(&pgproto3.DataRow{Values: values})
					}
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if backend.Flush() != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done; workers.Wait() })
}
