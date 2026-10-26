//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package initsql

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestNativeBootstrapBoundaries exercises only the curated product ledger, not
// upstream Auth/Storage/Realtime/Analytics release migrations or CP publication.
// The DSN MUST name an exclusively owned disposable PG17 cluster: roles are
// cluster-global. No GoTrue/PostgREST binaries or production hooks are required.
func TestNativeBootstrapBoundaries(t *testing.T) {
	dsn := os.Getenv("KARAKORAM_PG_DSN")
	if dsn == "" {
		t.Skip("set KARAKORAM_PG_DSN to an exclusively owned disposable PostgreSQL 17 primary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("cannot connect to disposable PostgreSQL")
	}
	t.Cleanup(func() { admin.Close(context.Background()) })
	var version int
	var super, recovery bool
	if err := admin.QueryRow(ctx, `SELECT current_setting('server_version_num')::int,
 (SELECT rolsuper FROM pg_roles WHERE rolname=current_user), pg_is_in_recovery()`).Scan(&version, &super, &recovery); err != nil {
		t.Fatal("cannot inspect native PostgreSQL prerequisites")
	}
	if version/10000 != 17 || !super || recovery {
		t.Fatal("configured native boundary target requires a PostgreSQL 17 primary and migration superuser")
	}
	var extensions bool
	if err := admin.QueryRow(ctx, `SELECT count(*)=2 FROM pg_available_extensions WHERE name IN ('pgcrypto','uuid-ossp')`).Scan(&extensions); err != nil {
		t.Fatal("cannot inspect native extension prerequisites")
	}
	if !extensions {
		t.Fatal("configured native boundary target requires pgcrypto and uuid-ossp extension files")
	}
	identity := boundaryIdentity(t, ctx, admin)
	plan := Plan([]string{"auth", "rest", "storage", "realtime"}, nil)
	reference := boundaryDatabase(t, ctx, admin)
	if err := Apply(ctx, reference, plan, nil); err != nil {
		t.Fatal(err)
	}
	want := boundaryCatalog(t, ctx, reference)
	boundaryReceipts(t, ctx, reference, plan)
	// Explicit security controls supplement the exact catalog comparison against
	// an uninterrupted execution of the same embedded product plan.
	var secure bool
	if err := reference.QueryRow(ctx, `SELECT
 NOT pg_has_role('authenticator','supabase_admin','MEMBER')
 AND NOT (SELECT rolcanlogin FROM pg_roles WHERE rolname='anon')
 AND (SELECT rolbypassrls FROM pg_roles WHERE rolname='service_role')
 AND NOT has_schema_privilege('anon','_karakoram','USAGE')
 AND NOT has_table_privilege('anon','_karakoram.supabase_migrations','SELECT')
 AND NOT (SELECT puballtables FROM pg_publication WHERE pubname='supabase_realtime')`).Scan(&secure); err != nil || !secure {
		t.Fatal("uninterrupted product bootstrap violated security controls")
	}

	t.Run("checksum_refusal", func(t *testing.T) {
		before := boundaryCatalog(t, ctx, reference)
		receipts := boundaryJournal(t, ctx, reference)
		step := plan[len(plan)-1]
		sum := sha256.Sum256([]byte(step.SQL))
		original := hex.EncodeToString(sum[:])
		boundaryExec(t, ctx, reference, "UPDATE _karakoram.supabase_migrations SET checksum=$1 WHERE id=$2", strings.Repeat("0", 64), step.ID)
		// Restore only the tampered checksum, including on an assertion failure.
		defer boundaryExec(t, ctx, reference, "UPDATE _karakoram.supabase_migrations SET checksum=$1 WHERE id=$2", original, step.ID)
		err := Apply(ctx, reference, plan, nil)
		if err == nil || err.Error() != "checksum differs for Supabase migration "+step.ID {
			t.Fatal("actual Apply did not refuse the named checksum-mismatched migration")
		}
		if boundaryCatalog(t, ctx, reference) != before || boundaryIdentity(t, ctx, reference) != identity {
			t.Fatal("checksum refusal changed roles, grants, schemas or system identity")
		}
		boundaryExec(t, ctx, reference, "UPDATE _karakoram.supabase_migrations SET checksum=$1 WHERE id=$2", original, step.ID)
		if boundaryJournal(t, ctx, reference) != receipts {
			t.Fatal("checksum refusal changed migration receipts beyond the deliberate tamper")
		}
		if err := Apply(ctx, reference, plan, nil); err != nil {
			t.Fatal(err)
		}
		if boundaryCatalog(t, ctx, reference) != before || boundaryJournal(t, ctx, reference) != receipts {
			t.Fatal("restored-checksum retry changed bootstrap state")
		}
		t.Log("named checksum refusal and tamper-only restoration verified on native Apply")
	})

	for _, after := range []bool{false, true} {
		name := "before_commit"
		if after {
			name = "after_commit_before_response"
		}
		t.Run(name, func(t *testing.T) {
			observer := boundaryDatabase(t, ctx, admin)
			caller, wire := boundaryConnect(t, ctx, observer.Config(), len(plan), after)
			pid := caller.PgConn().PID()
			defer caller.Close(context.Background())
			defer wire.release()
			result := make(chan error, 1)
			go func() { result <- Apply(ctx, caller, plan, nil) }()
			select {
			case <-wire.reached:
			case <-wire.done:
				t.Fatal("wire relay ended without reaching the exact COMMIT boundary")
			case <-time.After(30 * time.Second):
				t.Fatal("native Apply never reached the exact COMMIT boundary")
			}
			var state string
			var inTransaction bool
			if err := observer.QueryRow(ctx, `SELECT state, xact_start IS NOT NULL FROM pg_stat_activity WHERE pid=$1`, pid).Scan(&state, &inTransaction); err != nil {
				t.Fatal("cannot inspect the owned bootstrap backend at the boundary")
			}
			var schema bool
			if err := observer.QueryRow(ctx, "SELECT to_regnamespace('realtime') IS NOT NULL").Scan(&schema); err != nil {
				t.Fatal("cannot inspect migration visibility at the boundary")
			}
			if after {
				// The relay has consumed CommandComplete(COMMIT), but delivered
				// none of it to pgx. An independent session must see the receipt.
				if !schema {
					t.Fatal("after-COMMIT boundary lacks independently committed state")
				}
				boundaryReceipts(t, ctx, observer, plan)
			} else {
				// COMMIT was received from pgx but never forwarded to PostgreSQL.
				if !inTransaction || state != "idle in transaction" || schema {
					t.Fatal("before-COMMIT boundary lacks an open, invisible migration transaction")
				}
				boundaryReceipts(t, ctx, observer, plan[:len(plan)-1])
				var terminated bool
				if err := observer.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&terminated); err != nil || !terminated {
					t.Fatal("cannot terminate the owned native transaction")
				}
			}
			journal := boundaryJournal(t, ctx, observer)
			wire.release()
			select {
			case err := <-result:
				if err == nil || !strings.Contains(err.Error(), "committing Supabase migration "+plan[len(plan)-1].ID) {
					t.Fatal("Apply did not report loss of the final migration COMMIT response")
				}
			case <-time.After(15 * time.Second):
				t.Fatal("interrupted Apply did not return")
			}
			if boundaryJournal(t, ctx, observer) != journal {
				t.Fatal("lost response changed the independently observed journal")
			}
			// Reconstruct the caller; no reuse of its broken connection or state.
			retry, err := pgx.ConnectConfig(ctx, observer.Config().Copy())
			if err != nil {
				t.Fatal("cannot reconstruct native bootstrap caller")
			}
			defer retry.Close(context.Background())
			if err := Apply(ctx, retry, plan, nil); err != nil {
				t.Fatal(err)
			}
			boundaryReceipts(t, ctx, observer, plan)
			if boundaryCatalog(t, ctx, observer) != want || boundaryIdentity(t, ctx, observer) != identity {
				t.Fatal("recovery changed exact roles/grants/schema state or native system identity")
			}
			if after && boundaryJournal(t, ctx, observer) != journal {
				t.Fatal("after-COMMIT retry replayed or rewrote committed receipts")
			}
			if !after && boundaryJournal(t, ctx, observer, plan[len(plan)-1].ID) != journal {
				t.Fatal("before-COMMIT retry rewrote an earlier committed receipt")
			}
			stable := boundaryJournal(t, ctx, observer)
			if err := Apply(ctx, retry, plan, nil); err != nil {
				t.Fatal(err)
			}
			if boundaryJournal(t, ctx, observer) != stable || boundaryCatalog(t, ctx, observer) != want {
				t.Fatal("second recovery retry was not an exact ledger/grant no-op")
			}
			t.Log("exact wire boundary observed; caller reconstructed; native identity, unique receipts and exact grants preserved")
		})
	}
}

func boundaryDatabase(t *testing.T, ctx context.Context, admin *pgx.Conn) *pgx.Conn {
	t.Helper()
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("cannot generate disposable database name")
	}
	name := "bootstrap_boundary_" + hex.EncodeToString(nonce[:])
	boundaryExec(t, ctx, admin, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE template0")
	config := admin.Config().Copy()
	config.Database = name
	conn, err := pgx.ConnectConfig(ctx, config)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if conn != nil {
			conn.Close(cleanup)
		}
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Error("cannot remove owned disposable boundary database")
		}
	})
	if err != nil {
		t.Fatal("cannot connect to owned disposable database")
	}
	return conn
}

func boundaryExec(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatal("native boundary setup or cleanup SQL failed")
	}
}

func boundaryIdentity(t *testing.T, ctx context.Context, conn *pgx.Conn) string {
	t.Helper()
	var identity string
	if err := conn.QueryRow(ctx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&identity); err != nil {
		t.Fatal("cannot read native PostgreSQL system identity")
	}
	return identity
}

func boundaryJournal(t *testing.T, ctx context.Context, conn *pgx.Conn, excluded ...string) string {
	t.Helper()
	exclude := ""
	if len(excluded) != 0 {
		exclude = excluded[0]
	}
	var journal string
	if err := conn.QueryRow(ctx, `SELECT coalesce(jsonb_agg(jsonb_build_array(id,checksum,applied_at) ORDER BY id),'[]'::jsonb)::text FROM _karakoram.supabase_migrations WHERE id<>$1`, exclude).Scan(&journal); err != nil {
		t.Fatal("cannot inspect native migration journal")
	}
	return journal
}

func boundaryReceipts(t *testing.T, ctx context.Context, conn *pgx.Conn, plan []Step) {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT id,checksum,count(*) FROM _karakoram.supabase_migrations GROUP BY id,checksum ORDER BY id")
	if err != nil {
		t.Fatal("cannot inspect native receipts")
	}
	defer rows.Close()
	want := make(map[string]string, len(plan))
	for _, step := range plan {
		sum := sha256.Sum256([]byte(step.SQL))
		want[step.ID] = hex.EncodeToString(sum[:])
	}
	count := 0
	for rows.Next() {
		var id, checksum string
		var copies int
		if err := rows.Scan(&id, &checksum, &copies); err != nil || copies != 1 || want[id] != checksum {
			t.Fatal("native ledger does not contain exactly one genuine checksum per migration")
		}
		count++
	}
	if rows.Err() != nil || count != len(want) {
		t.Fatal("native ledger migration set differs from the genuine product plan")
	}
}

// Compare semantic catalogs, never OIDs, generated timestamps, passwords or DSNs.
// ACL arrays retain their complete grantor/grantee/privilege content. The
// reference is an uninterrupted real Apply, not handwritten replacement SQL.
func boundaryCatalog(t *testing.T, ctx context.Context, conn *pgx.Conn) string {
	t.Helper()
	queries := []string{
		`SELECT jsonb_build_array(rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit,rolconfig)::text FROM pg_roles ORDER BY rolname`,
		`SELECT jsonb_build_array(pg_get_userbyid(roleid),pg_get_userbyid(member),pg_get_userbyid(grantor),admin_option,inherit_option,set_option)::text FROM pg_auth_members ORDER BY 1`,
		`SELECT jsonb_build_array(nspname,pg_get_userbyid(nspowner),nspacl)::text FROM pg_namespace WHERE nspname !~ '^pg_' AND nspname <> 'information_schema' ORDER BY nspname`,
		`SELECT jsonb_build_array(n.nspname,c.relname,c.relkind,pg_get_userbyid(c.relowner),c.relacl,c.relrowsecurity,c.relforcerowsecurity)::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY 1`,
		`SELECT jsonb_build_array(n.nspname,p.proname,pg_get_function_identity_arguments(p.oid),pg_get_userbyid(p.proowner),p.proacl,p.prosecdef,p.proconfig)::text FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema' ORDER BY 1`,
		`SELECT jsonb_build_array(pg_get_userbyid(d.defaclrole),coalesce(n.nspname,''),d.defaclobjtype,d.defaclacl)::text FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace ORDER BY 1`,
		`SELECT jsonb_build_array(pubname,pg_get_userbyid(pubowner),puballtables,pubinsert,pubupdate,pubdelete,pubtruncate,pubviaroot)::text FROM pg_publication ORDER BY pubname`,
	}
	var snapshot strings.Builder
	for i, query := range queries {
		fmt.Fprintf(&snapshot, "catalog %d\n", i)
		rows, err := conn.Query(ctx, query)
		if err != nil {
			t.Fatal("cannot inspect native role/grant/schema catalog")
		}
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				rows.Close()
				t.Fatal("cannot scan native role/grant/schema catalog")
			}
			snapshot.WriteString(value)
			snapshot.WriteByte('\n')
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal("native role/grant/schema catalog read failed")
		}
	}
	return snapshot.String()
}
