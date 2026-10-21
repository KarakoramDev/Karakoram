// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func testGroupSpec() myspec.Spec {
	return myspec.Spec{Member: "db1", Version: "8.4.11-11", Peers: []myspec.Peer{{Name: "db1", Host: "127.0.0.1", Port: 24850}}, GroupReplication: &myspec.GroupReplication{GroupName: "11111111-2222-3333-4444-555555555555", FormationEpoch: 1, Seeds: []myspec.Peer{{Name: "db1", Host: "127.0.0.1", Port: 24850}}, Consistency: myspec.ConsistencyBeforeOnPrimaryFailover, UnreachableMajorityTimeoutSeconds: 8, AutorejoinTries: 3, MemberWeight: 50}}
}

func testCloneSpec() myspec.Spec {
	s := testGroupSpec()
	donor := myspec.Peer{Name: "db2", Host: "127.0.0.2", Port: 24850}
	s.Peers = append(s.Peers, donor)
	s.GroupReplication.CloneDonor = &donor
	return s
}
func TestRenderGroupOptions(t *testing.T) {
	s := testGroupSpec()
	m := RenderGroupOptions(s)
	for k, v := range map[string]string{"loose-group_replication_local_address": "127.0.0.1:24850", "loose-group_replication_group_seeds": "127.0.0.1:24850", "loose-group_replication_communication_stack": "MYSQL", "loose-group_replication_start_on_boot": "OFF", "loose-group_replication_ssl_mode": "REQUIRED", "loose-group_replication_recovery_use_ssl": "ON", "loose-group_replication_exit_state_action": "READ_ONLY", "plugin-load-add": "group_replication.so;mysql_clone.so"} {
		if m[k] != v {
			t.Errorf("%s=%q want %q", k, m[k], v)
		}
	}
	if RenderGroupOptions(myspec.Spec{}) != nil {
		t.Fatal("singleton GR options")
	}
}
func TestGroupJoinReceipt(t *testing.T) {
	s := testGroupSpec()
	dir := t.TempDir()
	if GroupJoined(dir, s) {
		t.Fatal("unjoined")
	}
	path := groupReceiptPath(dir, s)
	if err := durableGroupFile(path, []byte("joined\n")); err != nil {
		t.Fatal(err)
	}
	if !GroupJoined(dir, s) {
		t.Fatal("receipt missing")
	}
	info, _ := os.Stat(path)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
	s.GroupReplication.FormationEpoch++
	if GroupJoined(dir, s) {
		t.Fatal("stale epoch accepted")
	}
}

type rejectedGroupDeps struct {
	s        myspec.Spec
	identity myspec.DataIdentity
	dir      string
}

func (d *rejectedGroupDeps) LocalAccessDB(context.Context) (*sql.DB, error) {
	panic("rejected action reached database")
}
func (d *rejectedGroupDeps) GroupSpec() myspec.Spec                      { return d.s }
func (d *rejectedGroupDeps) GroupReceiptsDir() string                    { return d.dir }
func (d *rejectedGroupDeps) GroupIdentity() (myspec.DataIdentity, error) { return d.identity, nil }
func (d *rejectedGroupDeps) SaveGroupIdentity(i myspec.DataIdentity) error {
	d.identity = i
	return nil
}
func (d *rejectedGroupDeps) RestartAfterClone(context.Context) error {
	panic("rejected action restarted")
}
func (d *rejectedGroupDeps) CloneRecipientStopped() bool { return false }
func TestBootstrapReplayFailsClosed(t *testing.T) {
	s := testGroupSpec()
	s.GroupReplication.Bootstrap = true
	d := &rejectedGroupDeps{s: s, identity: myspec.DataIdentity{Initialized: true, FormationEpoch: 1}}
	for _, r := range []myspec.GroupRequest{{GroupName: s.GroupReplication.GroupName, FormationEpoch: 1, Bootstrap: true}, {GroupName: s.GroupReplication.GroupName, FormationEpoch: 1}} {
		if BootstrapGroup(context.Background(), d, r) == nil {
			t.Fatal("accepted consumed/unauthorized bootstrap")
		}
	}
}
func TestCloneVersionMismatch(t *testing.T) {
	s := testCloneSpec()
	d := &rejectedGroupDeps{s: s}
	if CloneGroup(context.Background(), d, myspec.CloneRequest{Receipt: "clone-one", Version: "8.0.46-37", Donor: *s.GroupReplication.CloneDonor}) == nil {
		t.Fatal("accepted mismatched clone")
	}
}
func TestCompletedCloneReplay(t *testing.T) {
	s := testCloneSpec()
	d := &rejectedGroupDeps{s: s, dir: t.TempDir()}
	req := myspec.CloneRequest{Receipt: "operation-one", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
	b, err := json.Marshal(cloneReceipt{Request: req, Completed: true})
	if err != nil {
		t.Fatal(err)
	}
	if err = durableGroupFile(filepath.Join(d.dir, "clone-"+req.Receipt), b); err != nil {
		t.Fatal(err)
	}
	if err = CloneGroup(context.Background(), d, req); err != nil {
		t.Fatal(err)
	}
	req.Donor.Port++
	if err = CloneGroup(context.Background(), d, req); err == nil {
		t.Fatal("receipt accepted different donor")
	}
}

func TestBootstrapRequiresCurrentSeedAuthorization(t *testing.T) {
	s := testGroupSpec()
	d := &rejectedGroupDeps{s: s, identity: myspec.DataIdentity{Initialized: true}}
	r := myspec.GroupRequest{GroupName: s.GroupReplication.GroupName, FormationEpoch: 1, Bootstrap: true}
	if err := BootstrapGroup(context.Background(), d, r); err == nil {
		t.Fatal("request bootstrap bit bypassed current selected seed")
	}
	if d.identity.FormationEpoch != 0 {
		t.Fatal("unauthorized bootstrap consumed epoch")
	}
}

func TestCloneRequiresCurrentTopologyAuthorization(t *testing.T) {
	for _, mode := range []string{"singleton", "no donor", "wrong donor", "self donor", "nonmember donor", "nonmember recipient", "wrong version", "bootstrap seed", "unapproved recovery receipt"} {
		t.Run(mode, func(t *testing.T) {
			s := testCloneSpec()
			req := myspec.CloneRequest{Receipt: "operation-one", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
			switch mode {
			case "singleton":
				s.GroupReplication = nil
			case "no donor":
				s.GroupReplication.CloneDonor = nil
			case "wrong donor":
				req.Donor.Port++
			case "self donor":
				req.Donor = s.Peers[0]
				s.GroupReplication.CloneDonor = &req.Donor
			case "nonmember donor":
				s.Peers = append(s.Peers[:1], myspec.Peer{Name: "db3", Host: "127.0.0.3", Port: 24850})
			case "nonmember recipient":
				s.Member = "db4"
			case "wrong version":
				req.Version = "8.0.46-37"
			case "bootstrap seed":
				s.GroupReplication.Bootstrap = true
			case "unapproved recovery receipt":
				s.GroupReplication.Reclone = true
				s.GroupReplication.RecloneReceipt = "different-operation"
			}
			d := &rejectedGroupDeps{s: s, dir: t.TempDir()}
			if err := CloneGroup(context.Background(), d, req); err == nil {
				t.Fatal("unauthorized clone accepted")
			}
		})
	}
}

type groupQuery struct {
	contains string
	values   []driver.Value
}

type groupTestConnector struct {
	t       *testing.T
	queries []groupQuery
	pingErr error
}

func (c *groupTestConnector) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *groupTestConnector) Driver() driver.Driver                        { return nil }
func (c *groupTestConnector) Prepare(query string) (driver.Stmt, error) {
	c.t.Fatalf("unexpected prepared mutation: %s", query)
	return nil, errors.New("unexpected prepare")
}
func (c *groupTestConnector) Close() error              { return nil }
func (c *groupTestConnector) Begin() (driver.Tx, error) { return nil, errors.New("unexpected begin") }
func (c *groupTestConnector) Ping(context.Context) error {
	err := c.pingErr
	c.pingErr = nil
	return err
}
func (c *groupTestConnector) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if len(c.queries) == 0 {
		c.t.Fatalf("unexpected SQL: %s", query)
	}
	next := c.queries[0]
	c.queries = c.queries[1:]
	if !strings.Contains(query, next.contains) {
		c.t.Fatalf("SQL %q does not contain %q", query, next.contains)
	}
	return &groupTestRows{values: next.values}, nil
}

type groupTestRows struct{ values []driver.Value }

func (r *groupTestRows) Columns() []string { return make([]string, len(r.values)) }
func (r *groupTestRows) Close() error      { return nil }
func (r *groupTestRows) Next(dst []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}
	copy(dst, r.values)
	r.values = nil
	return nil
}

type cloneTestDeps struct {
	rejectedGroupDeps
	db       *sql.DB
	restarts int
	localErr error
	stopped  bool
}

func (d *cloneTestDeps) LocalAccessDB(context.Context) (*sql.DB, error) {
	if d.localErr != nil {
		err := d.localErr
		d.localErr = nil
		return nil, err
	}
	return d.db, nil
}
func (d *cloneTestDeps) RestartAfterClone(context.Context) error { d.restarts++; return nil }
func (d *cloneTestDeps) CloneRecipientStopped() bool             { return d.stopped }

func TestPendingCloneRequiresNewMatchingNativeOperation(t *testing.T) {
	for _, mode := range []string{"old completion", "legacy intent", "wrong donor", "unfinished", "failed", "restart ready", "matching completion", "recovery completion", "unavailable recipient", "lost ping reply", "stopped recipient"} {
		t.Run(mode, func(t *testing.T) {
			s := testCloneSpec()
			req := myspec.CloneRequest{Receipt: "operation-one", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
			if mode == "recovery completion" {
				s.GroupReplication.Reclone = true
				s.GroupReplication.RecloneReceipt = req.Receipt
			}
			old := cloneNativeStatus{ID: 1, BeginTime: "2026-10-20 00:00:00.000", Source: "127.0.0.2:24850", State: "Completed"}
			current := old
			current.BeginTime = "2026-10-21 00:00:00.000"
			record := cloneReceipt{Request: req, Baseline: &old}
			switch mode {
			case "old completion":
				current = old
			case "legacy intent":
				record.Baseline = nil
			case "wrong donor":
				current.Source = "127.0.0.3:24850"
			case "unfinished", "restart ready":
				current.State = "In Progress"
			case "failed":
				current.State = "Failed"
			}
			nativeRow := groupQuery{contains: "FROM performance_schema.clone_status", values: []driver.Value{int64(current.ID), current.BeginTime, current.Source, current.State}}
			queries := []groupQuery{nativeRow}
			valid := mode == "matching completion" || mode == "recovery completion" || mode == "restart ready" || mode == "stopped recipient"
			if current.State != "Completed" {
				queries = append(queries, groupQuery{contains: "FROM performance_schema.clone_progress", values: []driver.Value{mode == "restart ready"}})
			}
			if valid {
				completedRow := groupQuery{contains: "FROM performance_schema.clone_status", values: []driver.Value{int64(current.ID), current.BeginTime, current.Source, "Completed"}}
				queries = append(queries, groupQuery{contains: "SELECT @@server_uuid", values: []driver.Value{"new-server-uuid"}}, completedRow)
			}
			connector := &groupTestConnector{t: t, queries: queries}
			db := sql.OpenDB(connector)
			defer db.Close()
			d := &cloneTestDeps{rejectedGroupDeps: rejectedGroupDeps{s: s, dir: t.TempDir(), identity: myspec.DataIdentity{Initialized: true}}, db: db}
			wantRestarts := 1
			if mode == "unavailable recipient" || mode == "stopped recipient" {
				d.localErr = errors.New("recipient unreachable")
			}
			if mode == "stopped recipient" {
				d.stopped = true
				wantRestarts++
			}
			if mode == "lost ping reply" {
				connector.pingErr = mysqldriver.ErrInvalidConn
			}
			b, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(d.dir, "clone-"+req.Receipt)
			if err = durableGroupFile(path, b); err != nil {
				t.Fatal(err)
			}
			err = CloneGroup(context.Background(), d, req)
			if !valid {
				if err == nil || d.restarts != 0 || d.identity.FormationEpoch != 0 {
					t.Fatalf("unmatched clone completed: err=%v restarts=%d identity=%+v", err, d.restarts, d.identity)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if d.restarts != wantRestarts || d.identity.ServerUUID != "new-server-uuid" || d.identity.FormationEpoch != s.GroupReplication.FormationEpoch {
				t.Fatalf("clone identity was not durably finalized: %+v", d)
			}
			if err = CloneGroup(context.Background(), d, req); err != nil || d.restarts != wantRestarts {
				t.Fatalf("completed replay reran native action: %v", err)
			}
			if len(connector.queries) != 0 {
				t.Fatal("expected native reads missing")
			}
		})
	}
}

func TestCloneRejectsExistingMemberOrSingletonData(t *testing.T) {
	for _, mode := range []string{"consumed epoch", "joined member", "singleton transactions", "unlogged user schema"} {
		t.Run(mode, func(t *testing.T) {
			s := testCloneSpec()
			req := myspec.CloneRequest{Receipt: "operation-one", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
			queries := []groupQuery{{contains: "SELECT VERSION()", values: []driver.Value{s.Version}}}
			identity := myspec.DataIdentity{Initialized: true}
			if mode == "consumed epoch" {
				identity.FormationEpoch = 1
			}
			if mode == "singleton transactions" {
				queries = append(queries, groupQuery{contains: "@@GLOBAL.gtid_executed", values: []driver.Value{"uuid:1-9", int64(0)}})
			}
			if mode == "unlogged user schema" {
				queries = append(queries, groupQuery{contains: "@@GLOBAL.gtid_executed", values: []driver.Value{"", int64(1)}})
			}
			connector := &groupTestConnector{t: t, queries: queries}
			db := sql.OpenDB(connector)
			defer db.Close()
			d := &cloneTestDeps{rejectedGroupDeps: rejectedGroupDeps{s: s, dir: t.TempDir(), identity: identity}, db: db}
			if mode == "joined member" {
				if err := durableGroupFile(groupReceiptPath(d.dir, s), []byte("joined\n")); err != nil {
					t.Fatal(err)
				}
			}
			if err := CloneGroup(context.Background(), d, req); err == nil {
				t.Fatal("existing data clone accepted")
			}
			if d.restarts != 0 || len(connector.queries) != 0 {
				t.Fatal("unexpected native behavior")
			}
			if _, err := os.Stat(filepath.Join(d.dir, "clone-"+req.Receipt)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("rejected overwrite wrote intent: %v", err)
			}
		})
	}
}

func TestCloneNativeBaselineRequiresNewerStart(t *testing.T) {
	s := testCloneSpec()
	req := myspec.CloneRequest{Receipt: "fresh-clone", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
	baseline := cloneNativeStatus{ID: 5, BeginTime: "2026-10-20 01:00:00.000", Source: "127.0.0.2:24850", State: "Completed"}
	record := cloneReceipt{Request: req, Baseline: &baseline}
	current := baseline
	current.ID++
	if err := verifyCloneCompletion(record, current); err == nil {
		t.Fatal("different ID with indeterminate identical start time accepted")
	}
	current.BeginTime = "2026-10-19 01:00:00.000"
	if err := verifyCloneCompletion(record, current); err == nil {
		t.Fatal("older operation with different ID accepted")
	}
	empty := cloneNativeStatus{}
	record.Baseline = &empty
	if err := verifyCloneCompletion(record, current); err != nil {
		t.Fatalf("first native clone cannot complete from empty baseline: %v", err)
	}
}

func TestCloneLostReplyRequiresObservedRestartReadiness(t *testing.T) {
	for _, mode := range []string{"active copy", "restart ready", "completed", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			s := testCloneSpec()
			req := myspec.CloneRequest{Receipt: "lost-reply", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
			baseline := cloneNativeStatus{}
			record := cloneReceipt{Request: req, Baseline: &baseline}
			state := "In Progress"
			if mode == "completed" {
				state = "Completed"
			}
			row := groupQuery{contains: "FROM performance_schema.clone_status", values: []driver.Value{int64(1), "2026-10-21 00:00:00.000", "127.0.0.2:24850", state}}
			queries := []groupQuery{row}
			if state != "Completed" {
				queries = append(queries, groupQuery{contains: "STAGE='FILE SYNC' AND STATE='Completed'", values: []driver.Value{mode == "restart ready"}})
			}
			valid := mode == "restart ready" || mode == "completed"
			if valid {
				queries = append(queries,
					groupQuery{contains: "SELECT @@server_uuid", values: []driver.Value{"new-server-uuid"}},
					groupQuery{contains: "FROM performance_schema.clone_status", values: []driver.Value{int64(1), "2026-10-21 00:00:00.000", "127.0.0.2:24850", "Completed"}},
				)
			}
			connector := &groupTestConnector{t: t, queries: queries}
			db := sql.OpenDB(connector)
			defer db.Close()
			d := &cloneTestDeps{rejectedGroupDeps: rejectedGroupDeps{s: s, dir: t.TempDir(), identity: myspec.DataIdentity{Initialized: true}}, db: db}
			if mode == "unavailable" {
				d.localErr = mysqldriver.ErrInvalidConn
			}
			path := filepath.Join(d.dir, "clone-"+req.Receipt)
			b, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if err = durableGroupFile(path, b); err != nil {
				t.Fatal(err)
			}
			// This is the production ErrInvalidConn branch, not a CLONE retry.
			err = recoverCloneReply(context.Background(), d, path, record)
			if !valid {
				if err == nil || d.restarts != 0 || d.identity.FormationEpoch != 0 {
					t.Fatalf("lost reply interrupted unproven clone: err=%v restarts=%d identity=%+v", err, d.restarts, d.identity)
				}
			} else if err != nil || d.restarts != 1 || d.identity.FormationEpoch != s.GroupReplication.FormationEpoch {
				t.Fatalf("observed clone did not finalize: err=%v restarts=%d identity=%+v", err, d.restarts, d.identity)
			}
			b, err = os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = json.Unmarshal(b, &record); err != nil {
				t.Fatal(err)
			}
			if record.Completed != valid {
				t.Fatalf("unexpected completion receipt after lost reply: %+v", record)
			}
			if mode == "active copy" {
				// An actual receipt replay after the ambiguous reply must
				// fail closed while copying, then recover once restart-ready.
				connector.queries = []groupQuery{
					row,
					{contains: "STAGE='FILE SYNC' AND STATE='Completed'", values: []driver.Value{false}},
				}
				if err = CloneGroup(context.Background(), d, req); err == nil || d.restarts != 0 {
					t.Fatalf("pending replay interrupted active copy: err=%v restarts=%d", err, d.restarts)
				}
				connector.queries = []groupQuery{
					row,
					{contains: "STAGE='FILE SYNC' AND STATE='Completed'", values: []driver.Value{true}},
					{contains: "SELECT @@server_uuid", values: []driver.Value{"new-server-uuid"}},
					{contains: "FROM performance_schema.clone_status", values: []driver.Value{int64(1), "2026-10-21 00:00:00.000", "127.0.0.2:24850", "Completed"}},
				}
				if err = CloneGroup(context.Background(), d, req); err != nil || d.restarts != 1 {
					t.Fatalf("pending replay failed after file sync: err=%v restarts=%d", err, d.restarts)
				}
			}
		})
	}
}
