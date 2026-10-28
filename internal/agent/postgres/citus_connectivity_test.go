// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

// Citus declares use_secondary_nodes PGC_SU_BACKEND. Exercise the complete
// connectivity action against that startup-only contract, not just a SQL string.
func TestCitusConnectivityConfiguresPrimaryPolicyAtConnectionStart(t *testing.T) {
	socket, err := os.MkdirTemp("", "kk-citus-probe-")
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
	startupParams := make(chan map[string]string, 1)
	var postStartSets atomic.Int32
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
		message, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		startup, ok := message.(*pgproto3.StartupMessage)
		if !ok {
			return
		}
		startupParams <- startup.Parameters
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if backend.Flush() != nil {
			return
		}
		statements := map[string]string{}
		var fields []pgproto3.FieldDescription
		var rows [][]any
		var formats []int16
		types := pgtype.NewMap()
		describe := func(query string) {
			var oids []uint32
			switch {
			case strings.Contains(query, "pg_is_in_recovery"):
				oids = []uint32{pgtype.BoolOID}
				rows = [][]any{{false}}
			case strings.Contains(query, "run_command_on_all_nodes('SELECT 1')"):
				oids = []uint32{pgtype.Int4OID, pgtype.Int4OID, pgtype.TextOID, pgtype.Int4OID, pgtype.TextOID, pgtype.BoolOID}
				rows = [][]any{
					{int32(1), int32(0), "coordinator", int32(5432), "primary", true},
					{int32(2), int32(1), "worker1", int32(5432), "primary", true},
					{int32(3), int32(2), "worker2", int32(5432), "primary", true},
				}
			default:
				t.Errorf("unexpected connectivity query: %s", query)
			}
			fields = make([]pgproto3.FieldDescription, len(oids))
			for i, oid := range oids {
				fields[i] = pgproto3.FieldDescription{Name: []byte(fmt.Sprintf("f%d", i)), DataTypeOID: oid, DataTypeSize: -1}
			}
		}
		for {
			message, err := backend.Receive()
			if err != nil {
				return
			}
			switch message := message.(type) {
			case *pgproto3.Query:
				if strings.Contains(message.String, "SET citus.use_secondary_nodes") {
					postStartSets.Add(1)
					backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "55P02", Message: "parameter citus.use_secondary_nodes cannot be set after connection start"})
					backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				} else {
					t.Errorf("unexpected simple connectivity query: %s", message.String)
					return
				}
			case *pgproto3.Parse:
				statements[message.Name] = message.Query
				backend.Send(&pgproto3.ParseComplete{})
			case *pgproto3.Describe:
				if message.ObjectType == 'S' {
					describe(statements[message.Name])
					backend.Send(&pgproto3.ParameterDescription{})
				}
				backend.Send(&pgproto3.RowDescription{Fields: fields})
			case *pgproto3.Bind:
				formats = message.ResultFormatCodes
				backend.Send(&pgproto3.BindComplete{})
			case *pgproto3.Execute:
				for _, row := range rows {
					encoded := make([][]byte, len(fields))
					for i, field := range fields {
						format := int16(0)
						if len(formats) == 1 {
							format = formats[0]
						} else if len(formats) > 1 {
							format = formats[i]
						}
						encoded[i], err = types.Encode(field.DataTypeOID, format, row[i], nil)
						if err != nil {
							t.Error("encoding connectivity response:", err)
							return
						}
					}
					backend.Send(&pgproto3.DataRow{Values: encoded})
				}
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(rows)))})
			case *pgproto3.Sync:
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			case *pgproto3.Close:
				backend.Send(&pgproto3.CloseComplete{})
			case *pgproto3.Terminate:
				return
			default:
				t.Errorf("unexpected connectivity protocol message: %T", message)
				return
			}
			if backend.Flush() != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = listener.Close(); <-done })
	spec := testSpec()
	// A standalone wire fixture isolates SQL connection policy; Patroni authority
	// and restore-fence preflights remain covered by their existing tests.
	spec.Citus = &pgspec.Citus{Database: "app"}
	value, err := runCitusAction(t.Context(), spec, p, pgspec.ActionCitusConnectivity, &struct{}{})
	if err != nil {
		t.Fatal("healthy formation rejected by startup-only policy:", err)
	}
	status := value.(pgspec.CitusStatusResponse)
	if !status.IsCoordinator || len(status.Nodes) != 3 {
		t.Fatal("connectivity probe lost group responses", status)
	}
	for _, node := range status.Nodes {
		if !node.Active {
			t.Fatal("healthy primary reported disconnected", node)
		}
	}
	if params := <-startupParams; params["citus.use_secondary_nodes"] != "never" {
		t.Fatal("primary policy not supplied at startup")
	}
	if postStartSets.Load() != 0 {
		t.Fatal("startup-only Citus policy changed after connection start")
	}
}
