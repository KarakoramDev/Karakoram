//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type nativeAccessDeps struct{ db *sql.DB }

func (nativeAccessDeps) GaleraAccess() bool { return false }

func (d nativeAccessDeps) LocalAccessDB(context.Context) (*sql.DB, error) { return d.db, nil }

func TestNativeMySQLAccess(t *testing.T) {
	binDir := os.Getenv("KARAKORAM_MYSQL_BIN_DIR")
	if binDir == "" {
		t.Skip("set KARAKORAM_MYSQL_BIN_DIR to a native Percona Server bin directory")
	}
	binDir, err := filepath.Abs(binDir)
	if err != nil {
		t.Fatal(err)
	}
	dir := nativeTestDir(t)
	data := filepath.Join(dir, "data")
	if err = os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "mysql.sock")
	port := nativeTestPort(t)
	basedir := filepath.Dir(binDir)
	initCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	init := exec.CommandContext(initCtx, filepath.Join(binDir, "mysqld"), "--no-defaults", "--basedir="+basedir, "--datadir="+data, "--innodb-buffer-pool-size=128M", "--initialize-insecure")
	if output, err := init.CombinedOutput(); err != nil {
		t.Fatalf("initialize: %v\n%s", err, output)
	}
	errorLog := filepath.Join(dir, "error.log")
	cmd := exec.Command(filepath.Join(binDir, "mysqld"), "--no-defaults", "--basedir="+basedir, "--datadir="+data, "--socket="+socket, "--pid-file="+filepath.Join(dir, "mysql.pid"), "--log-error="+errorLog, "--bind-address=127.0.0.1", fmt.Sprintf("--port=%d", port), "--mysqlx=OFF", "--innodb-buffer-pool-size=128M", "--innodb-redo-log-capacity=64M", "--max-connections=30", "--table-open-cache=128", "--require-secure-transport=ON", "--skip-log-bin")
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	rootConfig := mysqldriver.NewConfig()
	rootConfig.User = "root"
	rootConfig.Net = "unix"
	rootConfig.Addr = socket
	rootConfig.Timeout = time.Second
	root, err := sql.Open("mysql", rootConfig.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	deadline := time.Now().Add(40 * time.Second)
	for {
		if err = root.Ping(); err == nil {
			break
		}
		select {
		case <-done:
			log, _ := os.ReadFile(errorLog)
			t.Fatalf("mysqld exited: %v\n%s", waitErr, log)
		default:
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(errorLog)
			t.Fatalf("mysqld startup: %v\n%s", err, log)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var version string
	if err = root.QueryRow("SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	t.Logf("native server %s", version)
	agentPassword := "native-admin-password-123"
	c, err := root.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	err = executeUserStatement(context.Background(), c, accessStatement{prefix: "CREATE USER 'kk_agent'@'localhost' IDENTIFIED BY ", password: &agentPassword})
	_ = c.Close()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = root.Exec("GRANT ALL PRIVILEGES ON *.* TO 'kk_agent'@'localhost' WITH GRANT OPTION"); err != nil {
		t.Fatal(err)
	}
	agentConfig := mysqldriver.NewConfig()
	agentConfig.User = myspec.UserAgent
	agentConfig.Passwd = agentPassword
	agentConfig.Net = "unix"
	agentConfig.Addr = socket
	agentConfig.Collation = "utf8mb4_0900_ai_ci"
	agentConfig.Timeout = 3 * time.Second
	agentDB, err := sql.Open("mysql", agentConfig.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = agentDB.Close() })
	mux := http.NewServeMux()
	RegisterAccessRoutes(mux, nativeAccessDeps{agentDB})
	call := func(action string, req myspec.AccessRequest, status int, output any) {
		t.Helper()
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("POST", "/v1/mysql/"+action, strings.NewReader(string(body))))
		if w.Code != status {
			t.Fatalf("%s/%s: status=%d want=%d response=%s", action, req.Operation, w.Code, status, w.Body.String())
		}
		if output != nil {
			if err = json.Unmarshal(w.Body.Bytes(), output); err != nil {
				t.Fatalf("%s: %v", w.Body.String(), err)
			}
		}
	}
	applied := func(req myspec.AccessRequest) {
		t.Helper()
		var result myspec.AccessResult
		call(myspec.ActionAccessUsers, req, 200, &result)
		if result.Outcome != "applied" {
			t.Fatalf("%s: %+v", req.Operation, result)
		}
	}
	checkPassword := func(password string, wantSuccess bool) {
		t.Helper()
		cfg := mysqldriver.NewConfig()
		cfg.User = "app"
		cfg.Passwd = password
		cfg.Net = "tcp"
		cfg.Addr = fmt.Sprintf("127.0.0.1:%d", port)
		cfg.TLSConfig = "skip-verify"
		cfg.Timeout = 3 * time.Second
		db, err := sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		err = db.Ping()
		if (err == nil) != wantSuccess {
			t.Fatalf("password acceptance success=%v want=%v", err == nil, wantSuccess)
		}
	}
	oldPassword := "first-'\\你好-password-123"
	create := myspec.AccessRequest{Operation: "create", RequestID: "create_one", User: &myspec.UserWrite{Name: "app", MaxUserConnections: 9, Password: &oldPassword}}
	applied(create)
	checkPassword(oldPassword, true)
	// Replaying an applied request repairs an interrupted receipt publication
	// without re-running CREATE or changing any native credential.
	if _, err = root.Exec("UPDATE _karakoram.access_receipts SET outcome='pending' WHERE operation_id='create_one'"); err != nil {
		t.Fatal(err)
	}
	applied(create)
	var conflict myspec.AccessResult
	different := create
	different.User = &myspec.UserWrite{Name: "app", MaxUserConnections: 10, Password: &oldPassword}
	call(myspec.ActionAccessUsers, different, 200, &conflict)
	if conflict.Code != "receipt_conflict" {
		t.Fatalf("receipt identity reuse: %+v", conflict)
	}
	var users []myspec.User
	call(myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "list"}, 200, &users)
	if len(users) != 1 || users[0].Name != "app" || users[0].Host != "%" || users[0].MaxUserConnections != 9 || users[0].RoutableThroughProxy {
		t.Fatalf("users: %+v", users)
	}
	newPassword := "second-'\\你好-password-123"
	rotate := myspec.AccessRequest{Operation: "password", RequestID: "rotate_one", Name: "app", Password: &newPassword, RetainCurrent: true}
	// Simulate a crash after preparation and receipt persistence but before DDL.
	digest, err := accessRequestDigest(rotate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = root.Exec("ALTER USER 'app'@'%'" + markerAttribute(rotate.RequestID, digest, "prepared")); err != nil {
		t.Fatal(err)
	}
	if _, err = root.Exec("INSERT INTO _karakoram.access_receipts(operation_id,request_hash,outcome) VALUES (?,?,'pending')", rotate.RequestID, digest); err != nil {
		t.Fatal(err)
	}
	applied(rotate)
	checkPassword(oldPassword, true)
	checkPassword(newPassword, true)
	// A successful replay must not replace the retained old password with new.
	applied(rotate)
	checkPassword(oldPassword, true)
	var user myspec.User
	call(myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "get", Name: "app"}, 200, &user)
	if !user.PasswordRetained {
		t.Fatal("secondary password not reported")
	}
	applied(myspec.AccessRequest{Operation: "discard-old", RequestID: "discard_one", Name: "app"})
	checkPassword(newPassword, true)
	checkPassword(oldPassword, false)
	call(myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "get", Name: "app"}, 200, &user)
	if user.PasswordRetained {
		t.Fatal("secondary password survived discard")
	}
	locked := true
	applied(myspec.AccessRequest{Operation: "patch", RequestID: "lock_one", Name: "app", Patch: &myspec.UserPatch{Locked: &locked}})
	checkPassword(newPassword, false)
	locked = false
	applied(myspec.AccessRequest{Operation: "patch", RequestID: "unlock_one", Name: "app", Patch: &myspec.UserPatch{Locked: &locked}})
	checkPassword(newPassword, true)
	call(myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "create", Database: &myspec.Database{Name: "app_db"}}, 200, nil)
	var databases []myspec.Database
	call(myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "list"}, 200, &databases)
	if len(databases) != 1 || databases[0].Name != "app_db" || databases[0].CharacterSet != "utf8mb4" || databases[0].Collation != "utf8mb4_0900_ai_ci" {
		t.Fatalf("databases: %+v", databases)
	}
	grant := myspec.Grant{User: "app", Database: "app_db", Privileges: []string{"SELECT", "INSERT", "CREATE_VIEW"}}
	call(myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "grant", Grant: &grant}, 200, nil)
	var grants []myspec.Grant
	call(myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "list", UserFilter: "app"}, 200, &grants)
	if len(grants) != 1 || grants[0].Database != "app_db" || len(grants[0].Privileges) != 3 {
		t.Fatalf("grants: %+v", grants)
	}
	var nativeGrantDB string
	if err = root.QueryRow("SELECT Db FROM mysql.db WHERE User='app' AND Host='%' ").Scan(&nativeGrantDB); err != nil {
		t.Fatal(err)
	}
	if nativeGrantDB != `app\_db` {
		t.Fatalf("database grant is not exact: %q", nativeGrantDB)
	}
	call(myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "list"}, 400, nil)
	call(myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "revoke", Grant: &grant}, 200, nil)
	call(myspec.ActionAccessGrants, myspec.AccessRequest{Operation: "list", DatabaseFilter: "app_db"}, 200, &grants)
	if len(grants) != 0 {
		t.Fatalf("grants after revoke: %+v", grants)
	}
	if _, err = root.Exec("CREATE TABLE app_db.example (id INT)"); err != nil {
		t.Fatal(err)
	}
	call(myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "delete", Name: "app_db"}, 409, nil)
	call(myspec.ActionAccessDatabases, myspec.AccessRequest{Operation: "delete", Name: "app_db", Force: true}, 200, nil)
	deleteRequest := myspec.AccessRequest{Operation: "delete", RequestID: "delete_one", Name: "app"}
	applied(deleteRequest)
	call(myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "get", Name: "app"}, 404, nil)
	recreateNative := func() {
		t.Helper()
		c, err := root.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err = executeUserStatement(context.Background(), c, accessStatement{prefix: "CREATE USER 'app'@'%' IDENTIFIED BY ", password: &newPassword, suffix: " REQUIRE SSL"}); err != nil {
			t.Fatal(err)
		}
	}
	prepareDelete := func(req myspec.AccessRequest) {
		t.Helper()
		digest, err := accessRequestDigest(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = root.Exec("ALTER USER 'app'@'%'" + markerAttribute(req.RequestID, digest, "delete-prepared")); err != nil {
			t.Fatal(err)
		}
		if _, err = root.Exec("INSERT INTO _karakoram.access_receipts(operation_id,request_hash,outcome,identity_marker) VALUES (?,?,'pending',?)", req.RequestID, digest, encodeDeletionMarker(req.RequestID, digest, "delete-prepared")); err != nil {
			t.Fatal(err)
		}
	}
	// Applied tombstones remain successful without touching foreign recreation.
	recreateNative()
	applied(deleteRequest)
	checkPassword(newPassword, true)
	// Recover a crash after preparation but before DROP.
	deleteRequest.RequestID = "delete_two"
	prepareDelete(deleteRequest)
	applied(deleteRequest)
	call(myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "get", Name: "app"}, 404, nil)
	// Recover a crash after DROP committed but before its receipt update.
	recreateNative()
	deleteRequest.RequestID = "delete_three"
	prepareDelete(deleteRequest)
	if _, err = root.Exec("DROP USER 'app'@'%'"); err != nil {
		t.Fatal(err)
	}
	applied(deleteRequest)
	// A prepared receipt cannot authorize dropping a replacement account.
	recreateNative()
	deleteRequest.RequestID = "delete_four"
	prepareDelete(deleteRequest)
	if _, err = root.Exec("DROP USER 'app'@'%'"); err != nil {
		t.Fatal(err)
	}
	recreateNative()
	var deletionConflict myspec.AccessResult
	call(myspec.ActionAccessUsers, deleteRequest, 200, &deletionConflict)
	if deletionConflict.Outcome != "rejected" || deletionConflict.Code != "account_identity_changed" || deletionConflict.Status != 409 {
		t.Fatalf("foreign deletion: %+v", deletionConflict)
	}
	checkPassword(newPassword, true)
	deleteRequest.RequestID = "delete_five"
	applied(deleteRequest)
	applied(deleteRequest)
	changedDelete := deleteRequest
	changedDelete.Name = "other_user"
	call(myspec.ActionAccessUsers, changedDelete, 200, &deletionConflict)
	if deletionConflict.Code != "receipt_conflict" {
		t.Fatalf("delete identity reuse: %+v", deletionConflict)
	}
	call(myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "delete", RequestID: "protected_delete", Name: "kk_agent"}, 200, &deletionConflict)
	if deletionConflict.Outcome != "rejected" || deletionConflict.Status != 400 {
		t.Fatalf("protected deletion: %+v", deletionConflict)
	}
	if _, err = root.Exec("SET GLOBAL super_read_only=ON"); err != nil {
		t.Fatal(err)
	}
	defer root.Exec("SET GLOBAL super_read_only=OFF")
	call(myspec.ActionAccessUsers, myspec.AccessRequest{Operation: "list"}, 503, nil)
}
