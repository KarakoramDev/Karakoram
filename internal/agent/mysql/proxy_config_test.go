// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"modernc.org/sqlite"
)

// Use real SQLite for the admin SQL, intercepting only ProxySQL's LOAD/SAVE
// commands. Record every execution so a no-op assertion covers DELETE and LOAD
// as well as any other accidental write.
type proxyTestConnector struct{ statements *[]string }

func (p proxyTestConnector) Driver() driver.Driver { return &sqlite.Driver{} }
func (p proxyTestConnector) Connect(context.Context) (driver.Conn, error) {
	c, err := p.Driver().Open(":memory:")
	if err != nil {
		return nil, err
	}
	return &proxyTestConn{Conn: c, statements: p.statements}, nil
}

type proxyTestConn struct {
	driver.Conn
	statements *[]string
}

func (c *proxyTestConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}
func (c *proxyTestConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	*c.statements = append(*c.statements, q)
	if strings.HasPrefix(q, "ALTER INSTANCE ") || strings.HasPrefix(q, "SET GLOBAL ") {
		return driver.RowsAffected(0), nil
	}
	exec := c.Conn.(driver.ExecerContext)
	if strings.HasPrefix(q, "SAVE ") {
		return driver.RowsAffected(0), nil
	}
	if strings.HasPrefix(q, "LOAD ") {
		tables := map[string][]string{
			"LOAD MYSQL VARIABLES TO RUNTIME":   {"global_variables"},
			"LOAD MYSQL SERVERS TO RUNTIME":     {"mysql_group_replication_hostgroups", "mysql_galera_hostgroups", "mysql_servers", "mysql_hostgroup_attributes"},
			"LOAD MYSQL USERS TO RUNTIME":       {"mysql_users"},
			"LOAD MYSQL QUERY RULES TO RUNTIME": {"mysql_query_rules"},
		}
		names, ok := tables[q]
		if !ok {
			return nil, errors.New("unexpected LOAD")
		}
		for _, name := range names {
			if _, err := exec.ExecContext(ctx, "DELETE FROM runtime_"+name, nil); err != nil {
				return nil, err
			}
			if _, err := exec.ExecContext(ctx, "INSERT INTO runtime_"+name+" SELECT * FROM "+name, nil); err != nil {
				return nil, err
			}
		}
		return driver.RowsAffected(0), nil
	}
	return exec.ExecContext(ctx, q, args)
}
func proxyTestDB(t *testing.T, s myspec.Spec) (*sql.DB, *[]string) {
	t.Helper()
	var statements []string
	db := sql.OpenDB(proxyTestConnector{&statements})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	for _, table := range proxyConfigImage(s) {
		for _, prefix := range []string{"", "runtime_"} {
			columns := table.columns
			if table.name == "global_variables" {
				columns = "variable_name PRIMARY KEY,variable_value"
			}
			if _, err := db.Exec("CREATE TABLE " + prefix + table.name + "(" + columns + ")"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if !proxyCluster(s) {
		// These native admin tables exist even when the singleton desired
		// image does not own them. LOAD MYSQL SERVERS also loads their rows.
		for _, prefix := range []string{"", "runtime_"} {
			if _, err := db.Exec("CREATE TABLE " + prefix + "mysql_hostgroup_attributes(hostgroup_id PRIMARY KEY,servers_defaults)"); err != nil {
				t.Fatal(err)
			}
		}
	}
	statements = nil
	return db, &statements
}
func TestProxyReapplyIsReadOnly(t *testing.T) {
	for _, mode := range []string{"gr", "galera", "single"} {
		t.Run(mode, func(t *testing.T) {
			s := testProxySpec()
			if mode == "galera" {
				s.Proxy.Mode = "galera"
			}
			if mode == "single" {
				s.Topology = myspec.TopologySingle
				s.Proxy.ReaderPort = true
			}
			s.Proxy.ReadWriteSplit = true
			s.Proxy.Backends = append(s.Proxy.Backends, myspec.Peer{Name: "db2", Host: "db2.example", Port: 3306})
			s.Proxy.Users[0].Password = "secret,'quoted"
			db, statements := proxyTestDB(t, s)
			apply := func() {
				t.Helper()
				if err := applyProxySQL(t.Context(), db, s); err != nil {
					t.Fatal(err)
				}
			}
			apply()
			if proxyCluster(s) {
				// Monitor discovers a writer and moves the other backend offline; neither
				// role changes nor outages are desired-configuration drift.
				for _, q := range []string{
					"UPDATE runtime_mysql_servers SET hostgroup_id=10 WHERE hostname='db1.example'",
					"UPDATE runtime_mysql_servers SET hostgroup_id=30 WHERE hostname='db2.example'",
					"UPDATE runtime_mysql_servers SET status='SHUNNED' WHERE hostname='db1.example'",
					"UPDATE runtime_mysql_servers SET status='OFFLINE_HARD' WHERE hostname='db2.example'",
				} {
					if _, err := db.Exec(q); err != nil {
						t.Fatal(err)
					}
				}
			}
			*statements = nil
			apply()
			if len(*statements) != 0 {
				t.Fatalf("unchanged apply wrote: %v", *statements)
			}
			// A new connection (including after an agent restart) needs no receipt or
			// in-memory cache: runtime and memory tables are the source of truth.
			apply()
			if len(*statements) != 0 {
				t.Fatalf("repeat apply wrote: %v", *statements)
			}
			for _, drift := range []string{
				"DELETE FROM runtime_mysql_servers",
				"DELETE FROM runtime_mysql_users",
				"DELETE FROM runtime_mysql_query_rules",
				"UPDATE runtime_global_variables SET variable_value='drift'",
				"DELETE FROM mysql_servers",
				"UPDATE runtime_mysql_users SET backend=0",
				"UPDATE runtime_mysql_users SET password='drift'",
				"UPDATE runtime_mysql_servers SET use_ssl=0",
				"UPDATE runtime_mysql_query_rules SET flagIN=1 WHERE rule_id=10",
				"UPDATE mysql_query_rules SET negate_match_pattern=1 WHERE rule_id=10",
				"UPDATE runtime_mysql_query_rules SET username='wrong',re_modifiers='' WHERE rule_id=10",
			} {
				if _, err := db.Exec(drift); err != nil {
					t.Fatal(err)
				}
				*statements = nil
				apply()
				if len(*statements) == 0 {
					t.Fatalf("did not repair %s", drift)
				}
				*statements = nil
				apply()
				if len(*statements) != 0 {
					t.Fatalf("repair did not converge: %v", *statements)
				}
			}
			// A ProxySQL restart can replace the entire runtime image. Repair
			// does not depend on a process-local or persisted applied digest.
			for _, table := range proxyConfigImage(s) {
				if _, err := db.Exec("DELETE FROM runtime_" + table.name); err != nil {
					t.Fatal(err)
				}
			}
			*statements = nil
			apply()
			if len(*statements) == 0 {
				t.Fatal("empty restarted runtime was not restored")
			}
			*statements = nil
			apply()
			if len(*statements) != 0 {
				t.Fatal("restarted runtime did not converge")
			}
			// Hash rendering and split frontend/backend records are normal,
			// not configuration drift.
			a := sha1.Sum([]byte(s.Proxy.Users[0].Password))
			b := sha1.Sum(a[:])
			hash := "*" + strings.ToUpper(fmt.Sprintf("%x", b))
			if _, err := db.Exec("UPDATE runtime_mysql_users SET password=" + proxyQuote(hash) + ",backend=0"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("INSERT INTO runtime_mysql_users SELECT username,password,active,default_hostgroup,use_ssl,transaction_persistent,0,1 FROM runtime_mysql_users"); err != nil {
				t.Fatal(err)
			}
			*statements = nil
			apply()
			if len(*statements) != 0 {
				t.Fatalf("normal runtime user representation rewrote config: %v", *statements)
			}
			s.Proxy.MaxConnections++
			*statements = nil
			apply()
			if len(*statements) == 0 {
				t.Fatal("real change ignored")
			}
			*statements = nil
			apply()
			if len(*statements) != 0 {
				t.Fatal("real change did not converge")
			}
		})
	}
}

func TestMySQLTLSReloadOnlyOnChange(t *testing.T) {
	s := testProxySpec()
	s.CAPEM = "ca"
	s.ServerCert = &myspec.Certificate{CertPEM: "cert", KeyPEM: "key"}
	db, statements := proxyTestDB(t, s)
	runner := New()
	if err := runner.reload(t.Context(), db, s, s); err != nil {
		t.Fatal(err)
	}
	for _, q := range *statements {
		if q == "ALTER INSTANCE RELOAD TLS" {
			t.Fatal("unchanged TLS context reloaded")
		}
	}
	next := s
	next.ServerCert = &myspec.Certificate{CertPEM: "rotated", KeyPEM: "key"}
	*statements = nil
	if err := runner.reload(t.Context(), db, next, s); err != nil {
		t.Fatal(err)
	}
	if len(*statements) == 0 || (*statements)[0] != "ALTER INSTANCE RELOAD TLS" {
		t.Fatal("TLS rotation not reloaded")
	}
	runner.dataTLSReloadPending = true
	*statements = nil
	if err := runner.reload(t.Context(), db, next, next); err != nil {
		t.Fatal(err)
	}
	if len(*statements) == 0 || (*statements)[0] != "ALTER INSTANCE RELOAD TLS" || runner.dataTLSReloadPending {
		t.Fatal("pending TLS reload was not completed")
	}
}

func TestProxyClusterSeedsReadersAndRetainsWriter(t *testing.T) {
	for _, mode := range []string{"gr", "galera"} {
		t.Run(mode, func(t *testing.T) {
			s := testProxySpec()
			if mode == "galera" {
				s.Proxy.Mode = mode
			}
			db, _ := proxyTestDB(t, s)
			if err := applyProxySQL(t.Context(), db, s); err != nil {
				t.Fatal(err)
			}
			var group int
			if err := db.QueryRow("SELECT hostgroup_id FROM runtime_mysql_servers").Scan(&group); err != nil {
				t.Fatal(err)
			}
			if group != 20 {
				t.Fatalf("unverified backend seeded in %d", group)
			}
			if _, err := db.Exec("UPDATE runtime_mysql_servers SET hostgroup_id=10,status='SHUNNED'"); err != nil {
				t.Fatal(err)
			}
			s.Proxy.Backends = append(s.Proxy.Backends, myspec.Peer{Name: "new", Host: "new.example", Port: 3306})
			if err := applyProxySQL(t.Context(), db, s); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("SELECT hostgroup_id FROM runtime_mysql_servers WHERE hostname='db1.example'").Scan(&group); err != nil {
				t.Fatal(err)
			}
			if group != 10 {
				t.Fatalf("existing writer moved to %d", group)
			}
			var status string
			if err := db.QueryRow("SELECT status FROM runtime_mysql_servers WHERE hostname='db1.example'").Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status != "SHUNNED" {
				t.Fatal("monitor-shunned writer was reactivated")
			}
			if err := db.QueryRow("SELECT hostgroup_id FROM runtime_mysql_servers WHERE hostname='new.example'").Scan(&group); err != nil {
				t.Fatal(err)
			}
			if group != 20 {
				t.Fatalf("new backend seeded in %d", group)
			}
			text, err := RenderProxyConfig(s, "/config", "/data")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(text, "hostgroup=10,use_ssl") {
				t.Fatal("cold start seeds unverified writers")
			}
		})
	}
}

func TestUnchangedMySQLConfigFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "my.cnf")
	if unchangedConfigFile(path, []byte("config")) {
		t.Fatal("missing file matched")
	}
	if err := os.WriteFile(path, []byte("config"), 0600); err != nil {
		t.Fatal(err)
	}
	if !unchangedConfigFile(path, []byte("config")) {
		t.Fatal("equal file did not match")
	}
	if unchangedConfigFile(path, []byte("changed")) {
		t.Fatal("changed file matched")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if unchangedConfigFile(path, []byte("config")) {
		t.Fatal("insecure permissions matched")
	}
}

func TestTLSRotationSurvivesFailureBeforeReload(t *testing.T) {
	old := coreTestSpec()
	runner := New()
	runner.current = old // Previous successful apply and active TLS context.
	next := old
	next.ServerCert = &myspec.Certificate{CertPEM: "rotated", KeyPEM: "key"}
	runner.setApplyState(next, t.TempDir())
	if !runner.dataTLSReloadPending {
		t.Fatal("rotation was not recorded before fallible apply work")
	}
	// Abort here, as a certificate write or waitLocal error does. The retry
	// records identical desired state before it can reach reload.
	previous := runner.setApplyState(next, t.TempDir())
	if dataTLSChanged(previous, next) || !runner.dataTLSReloadPending {
		t.Fatal("early apply failure lost pending TLS activation")
	}
	db, statements := proxyTestDB(t, testProxySpec())
	if err := runner.reload(t.Context(), db, next, previous); err != nil {
		t.Fatal(err)
	}
	if len(*statements) == 0 || (*statements)[0] != "ALTER INSTANCE RELOAD TLS" || runner.dataTLSReloadPending {
		t.Fatal("retry did not activate the pending TLS context")
	}
	*statements = nil
	previous = runner.setApplyState(next, t.TempDir())
	if err := runner.reload(t.Context(), db, next, previous); err != nil {
		t.Fatal(err)
	}
	for _, q := range *statements {
		if q == "ALTER INSTANCE RELOAD TLS" {
			t.Fatal("successful activation did not clear pending TLS work")
		}
	}
}

func TestSingletonAdministrativeStatusDrift(t *testing.T) {
	s := testProxySpec()
	s.Topology = myspec.TopologySingle
	db, statements := proxyTestDB(t, s)
	apply := func() {
		t.Helper()
		if err := applyProxySQL(t.Context(), db, s); err != nil {
			t.Fatal(err)
		}
	}
	apply()
	for _, prefix := range []string{"", "runtime_"} {
		if _, err := db.Exec("UPDATE " + prefix + "mysql_servers SET status='OFFLINE_HARD'"); err != nil {
			t.Fatal(err)
		}
		*statements = nil
		apply()
		if len(*statements) == 0 {
			t.Fatalf("%sOFFLINE_HARD was not repaired", prefix)
		}
		var status string
		if err := db.QueryRow("SELECT status FROM runtime_mysql_servers").Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "ONLINE" {
			t.Fatalf("singleton remains %s", status)
		}
		*statements = nil
		apply()
		if len(*statements) != 0 {
			t.Fatal("singleton status repair did not converge")
		}
	}
	if _, err := db.Exec("UPDATE runtime_mysql_servers SET status='SHUNNED'"); err != nil {
		t.Fatal(err)
	}
	*statements = nil
	apply()
	if len(*statements) != 0 {
		t.Fatal("transient singleton shunning triggered a reload")
	}
}

func assertProxyHostgroupDefaults(t *testing.T, db *sql.DB, prefix string, maxConnections int) {
	t.Helper()
	rows, err := db.Query("SELECT hostgroup_id,servers_defaults FROM " + prefix + "mysql_hostgroup_attributes ORDER BY hostgroup_id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := map[int]bool{}
	for rows.Next() {
		var group int
		var defaults string
		if err := rows.Scan(&group, &defaults); err != nil {
			t.Fatal(err)
		}
		if found[group] {
			t.Fatal("duplicate managed hostgroup defaults", prefix, group)
		}
		found[group] = true
		assertProxyBackendDefaultJSON(t, defaults, maxConnections)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(found) != 4 || !found[10] || !found[11] || !found[20] || !found[30] {
		t.Fatal("memory/runtime defaults do not cover every managed hostgroup", prefix, found)
	}
}

func assertProxyDefaultsRepairCommittedBeforeRuntimeLoad(t *testing.T, statements []string) {
	t.Helper()
	begin, commit, load, attributeWrites := -1, -1, -1, 0
	for i, statement := range statements {
		switch statement {
		case "BEGIN":
			begin = i
		case "COMMIT":
			commit = i
		case "LOAD MYSQL SERVERS TO RUNTIME":
			load = i
		}
		if strings.HasPrefix(statement, "DELETE FROM mysql_hostgroup_attributes") || strings.HasPrefix(statement, "INSERT INTO mysql_hostgroup_attributes") {
			attributeWrites++
			if begin < 0 || commit >= 0 {
				t.Fatal("backend defaults were changed outside the atomic memory-image transaction", statements)
			}
		}
		if strings.HasPrefix(statement, "LOAD ") && commit < 0 {
			t.Fatal("runtime was loaded before the repaired image committed", statements)
		}
	}
	if begin < 0 || commit <= begin || load <= commit || attributeWrites != 5 {
		t.Fatal("defaults repair did not atomically replace four rows and load MYSQL SERVERS afterward", statements)
	}
}

func TestProxyHostgroupDefaultsRepairMemoryAndRuntimeDrift(t *testing.T) {
	for _, mode := range []string{"gr", "galera"} {
		t.Run(mode, func(t *testing.T) {
			s := testProxySpec()
			s.Proxy.MaxConnections = 731
			if mode == "galera" {
				s.Proxy.Mode = mode
			}
			image := proxyConfigImage(s)
			names := []string{"mysql_group_replication_hostgroups", "mysql_galera_hostgroups", "mysql_servers", "mysql_users", "mysql_query_rules", "global_variables", "mysql_hostgroup_attributes"}
			if len(image) != len(names) {
				t.Fatal("cluster image did not append exactly one defaults table", len(image))
			}
			for i, name := range names {
				if image[i].name != name {
					t.Fatal("adding hostgroup defaults changed an existing image index", i, image[i].name, name)
				}
			}
			db, statements := proxyTestDB(t, s)
			apply := func() {
				t.Helper()
				if err := applyProxySQL(t.Context(), db, s); err != nil {
					t.Fatal(err)
				}
			}
			apply()
			assertProxyDefaultsRepairCommittedBeforeRuntimeLoad(t, *statements)
			for _, prefix := range []string{"", "runtime_"} {
				assertProxyHostgroupDefaults(t, db, prefix, s.Proxy.MaxConnections)
			}
			// D087's monitor-owned writer/status movement remains read-only.
			if _, err := db.Exec("UPDATE runtime_mysql_servers SET hostgroup_id=10,status='SHUNNED'"); err != nil {
				t.Fatal(err)
			}
			*statements = nil
			apply()
			if len(*statements) != 0 {
				t.Fatal("unchanged defaults or normal monitor placement caused SQL writes", *statements)
			}
			for _, prefix := range []string{"", "runtime_"} {
				for _, drift := range []string{
					"UPDATE " + prefix + "mysql_hostgroup_attributes SET servers_defaults=" + proxyQuote(`{"weight":1,"max_connections":731,"use_ssl":0}`) + " WHERE hostgroup_id=10",
					"DELETE FROM " + prefix + "mysql_hostgroup_attributes WHERE hostgroup_id=11",
					"UPDATE " + prefix + "mysql_hostgroup_attributes SET servers_defaults=" + proxyQuote(`{"weight":1,"max_connections":17,"use_ssl":1}`) + " WHERE hostgroup_id=20",
					"UPDATE " + prefix + "mysql_hostgroup_attributes SET servers_defaults=" + proxyQuote(`{"weight":9,"max_connections":731,"use_ssl":1}`) + " WHERE hostgroup_id=30",
				} {
					if _, err := db.Exec(drift); err != nil {
						t.Fatal(err)
					}
					*statements = nil
					apply()
					assertProxyDefaultsRepairCommittedBeforeRuntimeLoad(t, *statements)
					for _, tablePrefix := range []string{"", "runtime_"} {
						assertProxyHostgroupDefaults(t, db, tablePrefix, s.Proxy.MaxConnections)
					}
					var group int
					var status string
					if err := db.QueryRow("SELECT hostgroup_id,status FROM runtime_mysql_servers").Scan(&group, &status); err != nil || group != 10 || status != "SHUNNED" {
						t.Fatal("repair changed the monitor's writer placement or outage status", group, status, err)
					}
					*statements = nil
					apply()
					if len(*statements) != 0 {
						t.Fatal("repaired defaults did not converge to a zero-write reload", drift, *statements)
					}
				}
			}
			s.Proxy.MaxConnections = 977
			*statements = nil
			apply()
			assertProxyDefaultsRepairCommittedBeforeRuntimeLoad(t, *statements)
			assertProxyHostgroupDefaults(t, db, "", 977)
			assertProxyHostgroupDefaults(t, db, "runtime_", 977)
			*statements = nil
			apply()
			if len(*statements) != 0 {
				t.Fatal("changed nondefault connection limit failed to converge", *statements)
			}
		})
	}
}

func TestProxyHostgroupDefaultsRepairRollsBackAtomically(t *testing.T) {
	for _, mode := range []string{"gr", "galera"} {
		t.Run(mode, func(t *testing.T) {
			s := testProxySpec()
			s.Proxy.MaxConnections = 731
			if mode == "galera" {
				s.Proxy.Mode = mode
			}
			db, statements := proxyTestDB(t, s)
			if err := applyProxySQL(t.Context(), db, s); err != nil {
				t.Fatal(err)
			}
			drift := `{"weight":1,"max_connections":17,"use_ssl":0}`
			for _, q := range []string{
				"UPDATE mysql_servers SET max_connections=17",
				"UPDATE mysql_hostgroup_attributes SET servers_defaults=" + proxyQuote(drift) + " WHERE hostgroup_id=20",
				"CREATE TRIGGER fail_hostgroup_defaults BEFORE INSERT ON mysql_hostgroup_attributes WHEN NEW.hostgroup_id=20 BEGIN SELECT RAISE(ABORT,'injected defaults write failure'); END",
			} {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
			*statements = nil
			if err := applyProxySQL(t.Context(), db, s); err == nil || !strings.Contains(err.Error(), "injected defaults write failure") {
				t.Fatal("atomic repair did not surface the native SQLite write failure", err)
			}
			for _, statement := range *statements {
				if statement == "COMMIT" || strings.HasPrefix(statement, "LOAD ") {
					t.Fatal("failed attribute write committed or loaded a partial server image", *statements)
				}
			}
			var maxConnections int
			if err := db.QueryRow("SELECT max_connections FROM mysql_servers").Scan(&maxConnections); err != nil || maxConnections != 17 {
				t.Fatal("failed late-table repair did not roll back earlier mysql_servers writes", maxConnections, err)
			}
			var defaults string
			if err := db.QueryRow("SELECT servers_defaults FROM mysql_hostgroup_attributes WHERE hostgroup_id=20").Scan(&defaults); err != nil || defaults != drift {
				t.Fatal("failed repair did not restore the original memory defaults row", defaults, err)
			}
			assertProxyHostgroupDefaults(t, db, "runtime_", 731)
			if _, err := db.Exec("DROP TRIGGER fail_hostgroup_defaults"); err != nil {
				t.Fatal(err)
			}
			*statements = nil
			if err := applyProxySQL(t.Context(), db, s); err != nil {
				t.Fatal(err)
			}
			assertProxyDefaultsRepairCommittedBeforeRuntimeLoad(t, *statements)
			assertProxyHostgroupDefaults(t, db, "", 731)
			assertProxyHostgroupDefaults(t, db, "runtime_", 731)
			*statements = nil
			if err := applyProxySQL(t.Context(), db, s); err != nil || len(*statements) != 0 {
				t.Fatal("successful retry did not restore read-only convergence", err, *statements)
			}
		})
	}
}

func TestSingletonProxyLeavesHostgroupAttributesUnmanaged(t *testing.T) {
	s := testProxySpec()
	s.Topology = myspec.TopologySingle
	image := proxyConfigImage(s)
	if len(image) != 6 {
		t.Fatal("singleton desired table image changed", len(image))
	}
	for _, table := range image {
		if table.name == "mysql_hostgroup_attributes" {
			t.Fatal("singleton claimed unrelated hostgroup defaults")
		}
	}
	db, statements := proxyTestDB(t, s)
	if err := applyProxySQL(t.Context(), db, s); err != nil {
		t.Fatal(err)
	}
	unmanaged := `{"weight":3,"max_connections":17,"use_ssl":0}`
	for _, prefix := range []string{"", "runtime_"} {
		if _, err := db.Exec("INSERT INTO " + prefix + "mysql_hostgroup_attributes(hostgroup_id,servers_defaults) VALUES(999," + proxyQuote(unmanaged) + ")"); err != nil {
			t.Fatal(err)
		}
	}
	*statements = nil
	if err := applyProxySQL(t.Context(), db, s); err != nil || len(*statements) != 0 {
		t.Fatal("unmanaged singleton attributes triggered writes", err, *statements)
	}
	s.Proxy.MaxConnections++
	if err := applyProxySQL(t.Context(), db, s); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "runtime_"} {
		var defaults string
		if err := db.QueryRow("SELECT servers_defaults FROM " + prefix + "mysql_hostgroup_attributes WHERE hostgroup_id=999").Scan(&defaults); err != nil || defaults != unmanaged {
			t.Fatal("singleton repair deleted or rewrote unrelated defaults", prefix, defaults, err)
		}
	}
}
