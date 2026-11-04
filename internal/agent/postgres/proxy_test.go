// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestProxyDNSCacheSettings(t *testing.T) {
	for _, backendTLS := range []bool{false, true} {
		p := &pgspec.Proxy{BackendTLS: backendTLS}
		settings := proxyRuntimeSettings(p, "/data/ca.pem")
		config := renderProxy(p, dataPaths(t.TempDir()), "")
		for name, want := range map[string]string{
			"monitor_local_dns_cache_ttl":              "5000",
			"monitor_local_dns_cache_refresh_interval": "1000",
			"monitor_local_dns_resolver_queue_maxsize": "128",
		} {
			if got := settings["pgsql-"+name]; got != want {
				t.Errorf("backendTLS=%t runtime %s=%q, want %q", backendTLS, name, got, want)
			}
			if !strings.Contains(config, "\n "+name+"="+want+";\n") {
				t.Errorf("backendTLS=%t bootstrap missing %s=%s", backendTLS, name, want)
			}
		}
	}
}

func TestProxyAdminCredentialRotation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:6132")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	var statements, passwords []string
	adminPassword := "old-secret"
	var connections sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				backend := pgproto3.NewBackend(conn, conn)
				if _, err := backend.ReceiveStartupMessage(); err != nil {
					return
				}
				backend.Send(&pgproto3.AuthenticationCleartextPassword{})
				if backend.Flush() != nil {
					return
				}
				msg, err := backend.Receive()
				password, ok := msg.(*pgproto3.PasswordMessage)
				if err != nil || !ok {
					return
				}
				mu.Lock()
				passwords = append(passwords, password.Password)
				valid := password.Password == adminPassword
				mu.Unlock()
				if !valid {
					backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "authentication failed"})
					_ = backend.Flush()
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
					mu.Lock()
					statements = append(statements, query.String)
					if strings.Contains(query.String, "admin-admin_credentials") {
						adminPassword = "new-secret"
					}
					mu.Unlock()
					if strings.Contains(query.String, "FROM runtime_global_variables") {
						backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("variable_name"), DataTypeOID: 25, DataTypeSize: -1}, {Name: []byte("variable_value"), DataTypeOID: 25, DataTypeSize: -1}}})
						for key, value := range proxyStartupSettings(&pgspec.Proxy{}, "") {
							backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(key), []byte(value)}})
						}
					} else if strings.HasPrefix(query.String, "SELECT ") {
						backend.Send(&pgproto3.RowDescription{})
					}
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("OK")})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if backend.Flush() != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-acceptDone
		connections.Wait()
	})
	p := &pgspec.Proxy{Admin: pgspec.Credentials{Username: "admin", Password: "new-secret"}, Monitor: pgspec.Credentials{Username: "monitor", Password: "monitor-secret"}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if restart, err := reconcileProxyWait(ctx, p, "", "", pgspec.Credentials{Username: "admin", Password: "old-secret"}); err != nil || restart {
		t.Fatalf("rotation failed: restart=%t err=%v", restart, err)
	}
	// After LOAD/SAVE the desired credentials must authenticate without the old ones.
	if restart, err := reconcileProxyWait(ctx, p, "", "", pgspec.Credentials{}); err != nil || restart {
		t.Fatalf("rotated authentication failed: restart=%t err=%v", restart, err)
	}
	p.BackendTLS = true
	if restart, err := reconcileProxyWait(ctx, p, "", "/data/ca.pem", pgspec.Credentials{}); err != nil || !restart {
		t.Fatalf("unverified proxy pools were not marked for restart: restart=%t err=%v", restart, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(passwords) != 4 || passwords[0] != "new-secret" || passwords[1] != "old-secret" || passwords[2] != "new-secret" || passwords[3] != "new-secret" {
		t.Fatal("previous credentials were not used for rotation", passwords)
	}
	plan := strings.Join(statements, "\n")
	for _, want := range []string{"UPDATE global_variables SET variable_value='admin:new-secret' WHERE variable_name='admin-admin_credentials'", "LOAD ADMIN VARIABLES TO RUNTIME", "SAVE ADMIN VARIABLES TO DISK", "UPDATE global_variables SET variable_value='/data/ca.pem' WHERE variable_name='pgsql-ssl_p2s_ca'", "UPDATE global_variables SET variable_value='5000' WHERE variable_name='pgsql-monitor_local_dns_cache_ttl'", "UPDATE global_variables SET variable_value='1000' WHERE variable_name='pgsql-monitor_local_dns_cache_refresh_interval'", "UPDATE global_variables SET variable_value='128' WHERE variable_name='pgsql-monitor_local_dns_resolver_queue_maxsize'", "LOAD PGSQL VARIABLES TO RUNTIME", "SAVE PGSQL VARIABLES TO DISK"} {
		if !strings.Contains(plan, want) {
			t.Errorf("missing admin rotation statement %q", want)
		}
	}
}

func TestProxyBackendTLSRequiresCA(t *testing.T) {
	s := testSpec()
	s.Role = pgspec.RoleProxy
	s.Proxy = &pgspec.Proxy{Admin: pgspec.Credentials{Username: "admin", Password: "secret"}, Monitor: s.Monitor, Servers: []string{"localhost"}, BackendTLS: true}
	for _, ca := range []string{"", "not a certificate"} {
		s.CAPEM = ca
		if validate(s) == nil {
			t.Fatal("backend TLS without a valid CA accepted")
		}
	}
	s.CAPEM, _ = testTLS(t)
	if err := validate(s); err != nil {
		t.Fatal(err)
	}
	p := dataPaths(t.TempDir())
	files, err := render(s, p, nil)
	if err != nil {
		t.Fatal(err)
	}
	rendered := map[string]string{}
	for _, file := range files {
		rendered[file.name] = file.content
	}
	if rendered["ca.pem"] != s.CAPEM || !strings.Contains(rendered["proxysql.cnf"], "ssl_p2s_ca=\""+p.root+"/ca.pem\";") {
		t.Fatal("backend and monitor trust source was not rendered")
	}
	if _, err := proxyPlan(context.Background(), nil, s.Proxy, "", ""); err == nil {
		t.Fatal("backend reconciliation accepted an absent trust source")
	}
}

func TestProxyAcceptsNoPublishedBackendsDuringBootstrap(t *testing.T) {
	s := testSpec()
	s.Role = pgspec.RoleProxy
	s.Peers = nil
	s.Proxy = &pgspec.Proxy{
		Admin:   pgspec.Credentials{Username: "admin", Password: "secret"},
		Monitor: s.Monitor, BackendTLS: true,
	}
	s.CAPEM, _ = testTLS(t)
	if err := validate(s); err != nil {
		t.Fatalf("proxy must start without publishing unready bootstrap identities: %v", err)
	}
}

func proxyStatusWireFixture(t *testing.T, address string, query func(string) ([]string, [][]string, bool)) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
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
					message, err := backend.Receive()
					if err != nil {
						return
					}
					request, ok := message.(*pgproto3.Query)
					if !ok {
						return
					}
					names, rows, fail := query(request.String)
					if fail {
						backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "backend unavailable"})
					} else {
						fields := make([]pgproto3.FieldDescription, len(names))
						for i, name := range names {
							fields[i] = pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: 25, DataTypeSize: -1}
						}
						backend.Send(&pgproto3.RowDescription{Fields: fields})
						for _, row := range rows {
							values := make([][]byte, len(row))
							for i, value := range row {
								values[i] = []byte(value)
							}
							backend.Send(&pgproto3.DataRow{Values: values})
						}
						backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
					}
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					if backend.Flush() != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		connections.Wait()
	})
}

func TestProxyStatusProbesIdleShunnedWriter(t *testing.T) {
	for _, tc := range []struct {
		name           string
		replica, fails bool
		want           bool
	}{
		{"primary-recovers-on-demand", false, false, true},
		{"replica-is-not-writable", true, false, false},
		{"unavailable-stays-unready", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var demanded atomic.Bool
			proxyStatusWireFixture(t, "[::1]:5432", func(query string) ([]string, [][]string, bool) {
				if query != "SHOW transaction_read_only" {
					t.Errorf("unexpected frontend health query: %s", query)
				}
				demanded.Store(true)
				value := "off"
				if tc.replica {
					value = "on"
				}
				return []string{"transaction_read_only"}, [][]string{{value}}, tc.fails
			})
			proxyStatusWireFixture(t, "127.0.0.1:6132", func(query string) ([]string, [][]string, bool) {
				if strings.Contains(query, "runtime_pgsql_users") {
					return []string{"username", "active"}, [][]string{{"postgres", "1"}}, false
				}
				if strings.Contains(query, "monitor.pgsql_server_") {
					return []string{"hostname", "time_start_us", "error"}, nil, false
				}
				if strings.Contains(query, "stats_pgsql_connection_pool") {
					return []string{"hostname", "conn_errors"}, nil, false
				}
				if query != "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers" {
					t.Errorf("unexpected admin health query: %s", query)
				}
				status := "SHUNNED"
				if demanded.Load() {
					status = "ONLINE"
				}
				return []string{"hostname", "hostgroup_id", "status"}, [][]string{{"primary", "10", status}}, false
			})
			s := testSpec()
			s.Role = pgspec.RoleProxy
			s.ListenHost = "::"
			s.Proxy = &pgspec.Proxy{
				Admin:   pgspec.Credentials{Username: "admin", Password: "admin-password"},
				Users:   []pgspec.ProxyUser{{Username: "postgres", Password: "database-password"}},
				Servers: []string{"primary"},
			}
			status := New().status(t.Context(), s, dataPaths(t.TempDir()))
			if status.Running != tc.want {
				t.Fatalf("proxy running=%t, want %t; frontend demand=%t", status.Running, tc.want, demanded.Load())
			}
			if !demanded.Load() {
				t.Fatal("TCP and runtime-table probes did not exercise lazy backend recovery")
			}
		})
	}
}

func TestProxyFrontendAddressFamilies(t *testing.T) {
	for _, tc := range []struct {
		host, writer, reader string
	}{
		{"", "0.0.0.0:5432", "0.0.0.0:5432;0.0.0.0:5433"},
		{"0.0.0.0", "0.0.0.0:5432", "0.0.0.0:5432;0.0.0.0:5433"},
		{"::", "0.0.0.0:5432;[::]:5432", "0.0.0.0:5432;[::]:5432;[::]:5433;0.0.0.0:5433"},
	} {
		for _, reader := range []bool{false, true} {
			p := &pgspec.Proxy{ReaderPort: reader}
			want := tc.writer
			if reader {
				want = tc.reader
			}
			if got := proxyStartupSettings(p, tc.host)["pgsql-interfaces"]; got != want {
				t.Errorf("host=%q reader=%t interfaces=%q, want %q", tc.host, reader, got, want)
			}
			text := renderProxy(p, dataPaths(t.TempDir()), tc.host)
			if !strings.Contains(text, " interfaces=\""+want+"\";") {
				t.Errorf("cold-start configuration lacks interfaces=%q", want)
			}
			for _, admin := range []string{`mysql_ifaces="127.0.0.1:6032"`, `pgsql_ifaces="127.0.0.1:6132"`} {
				if !strings.Contains(text, admin) {
					t.Errorf("administrative interface changed: missing %s", admin)
				}
			}
		}
	}
}
