// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"crypto/tls"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/jackc/pgx/v5/pgproto3"
)

type rolloutReplayAgents struct {
	fakeAgents
	address string
}

func (a rolloutReplayAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	base, err := a.fakeAgents.Dial(ctx, ref, name)
	if err != nil {
		return nil, err
	}
	return agentapi.NewClient(agentapi.ClientOptions{BaseURL: a.url, CAPEM: a.ca, Token: name,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if strings.HasSuffix(address, ":5432") {
				return (&net.Dialer{}).DialContext(ctx, network, a.address)
			}
			return base.DialContext(ctx, network, address)
		},
	})
}

// A native TLS PostgreSQL wire boundary, not an injected provider proof callback.
// The caller controls what the primary's SQL observation proves independently
// of the replica's generic/native status and DCS leader.
func installRolloutReplaySQL(t *testing.T, rc *engine.Context) *atomic.Bool {
	t.Helper()
	certPEM, err := secret(t.Context(), rc, "tls/servers/demop0.crt")
	if err != nil {
		t.Fatal(err)
	}
	keyPEM, err := secret(t.Context(), rc, "tls/servers/demop0.key")
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	proof := &atomic.Bool{}
	proof.Store(true)
	rc.Agents = rolloutReplayAgents{fakeAgents: rc.Agents.(fakeAgents), address: listener.Addr().String()}
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer raw.Close()
				backend := pgproto3.NewBackend(raw, raw)
				startup, err := backend.ReceiveStartupMessage()
				if err != nil {
					t.Error(err)
					return
				}
				if _, ok := startup.(*pgproto3.SSLRequest); !ok {
					t.Error("replay proof did not require native TLS")
					return
				}
				if _, err := raw.Write([]byte{'S'}); err != nil {
					t.Error(err)
					return
				}
				conn := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
				backend = pgproto3.NewBackend(conn, conn)
				startup, err = backend.ReceiveStartupMessage()
				if err != nil {
					t.Error(err)
					return
				}
				message, ok := startup.(*pgproto3.StartupMessage)
				if !ok || message.Parameters["user"] != "kk_monitor" || message.Parameters["database"] != "postgres" || message.Parameters["default_transaction_read_only"] != "on" {
					t.Error("replay proof lost monitor/read-only scope", startup)
					return
				}
				backend.Send(&pgproto3.AuthenticationOk{})
				backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.10"})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if err := backend.Flush(); err != nil {
					t.Error(err)
					return
				}
				for {
					message, err := backend.Receive()
					if err != nil {
						return
					}
					switch message := message.(type) {
					case *pgproto3.Parse:
						for _, fragment := range []string{"pg_stat_replication", "application_name = $1", "replay_lsn >= $2::pg_lsn", "pg_is_in_recovery", "pg_walfile_name", "pg_control_system", "count(*) = 1"} {
							if !strings.Contains(message.Query, fragment) {
								t.Error("incomplete primary-side replay proof", fragment)
							}
						}
						backend.Send(&pgproto3.ParseComplete{})
					case *pgproto3.Bind:
						if len(message.Parameters) != 4 || string(message.Parameters[0]) != "demop1" || string(message.Parameters[3]) != "system" {
							t.Error("replay proof not bound to waiting member")
						}
						backend.Send(&pgproto3.BindComplete{})
					case *pgproto3.Describe:
						backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("proved"), DataTypeOID: 16, DataTypeSize: 1}}})
					case *pgproto3.Execute:
						value := []byte("f")
						if proof.Load() {
							value = []byte("t")
						}
						backend.Send(&pgproto3.DataRow{Values: [][]byte{value}})
						backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
					case *pgproto3.Sync:
						backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					case *pgproto3.Terminate:
						return
					}
					if err := backend.Flush(); err != nil {
						return
					}
				}
			}()
		}
	}()
	return proof
}
