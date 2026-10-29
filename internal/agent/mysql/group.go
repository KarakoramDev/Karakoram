// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// GroupDeps binds bounded group actions to the agent-owned identity and supervisor.
// Identity writes must be durable and receipts must live outside the cloned datadir.
type GroupDeps interface {
	LocalAccessDB(context.Context) (*sql.DB, error)
	GroupSpec() myspec.Spec
	GroupReceiptsDir() string
	GroupIdentity() (myspec.DataIdentity, error)
	SaveGroupIdentity(myspec.DataIdentity) error
	RestartAfterClone(context.Context) error
	// CloneRecipientStopped requires process-exit evidence, not SQL reachability.
	CloneRecipientStopped() bool
}

var groupActionMu sync.Mutex

// RenderGroupOptions uses loose plugin variables so --initialize remains safe.
// start_on_boot stays OFF: the supervisor uses the durable join receipt to
// restore memory-only recovery credentials after each process start.
func RenderGroupOptions(s myspec.Spec, root string) map[string]string {
	if s.GroupReplication == nil {
		return nil
	}
	g := s.GroupReplication
	host := s.Member
	port := myspec.PortMySQL
	for _, p := range s.Peers {
		if p.Name == s.Member {
			host = p.Host
			port = p.Port
		}
	}
	seeds := make([]string, 0, len(g.Seeds))
	for _, p := range g.Seeds {
		seeds = append(seeds, net.JoinHostPort(p.Host, strconv.Itoa(p.Port)))
	}
	// BufferPoolBytes is the control plane's half-MinRAM baseline, before user
	// parameter overrides. Reserve one eighth of that member RAM for messages;
	// never inherit the upstream 1 GiB cache on a small member. Cache metadata,
	// connections, Performance Schema and clone still need separate headroom.
	messageCache := max(int64(128<<20), s.BufferPoolBytes/4)
	return map[string]string{
		"plugin-load-add":                                         "group_replication.so;mysql_clone.so",
		"loose-group_replication_group_name":                      g.GroupName,
		"loose-group_replication_local_address":                   net.JoinHostPort(host, strconv.Itoa(port)),
		"loose-group_replication_group_seeds":                     strings.Join(seeds, ","),
		"loose-group_replication_communication_stack":             "MYSQL",
		"loose-group_replication_message_cache_size":              strconv.FormatInt(messageCache, 10),
		"loose-group_replication_single_primary_mode":             "ON",
		"loose-group_replication_ssl_mode":                        "VERIFY_IDENTITY",
		"loose-group_replication_recovery_use_ssl":                "ON",
		"loose-group_replication_recovery_ssl_ca":                 filepath.Join(root, "ca.pem"),
		"loose-group_replication_recovery_ssl_cert":               filepath.Join(root, "server-cert.pem"),
		"loose-group_replication_recovery_ssl_key":                filepath.Join(root, "server-key.pem"),
		"loose-group_replication_recovery_ssl_verify_server_cert": "ON",
		"loose-group_replication_consistency":                     g.Consistency,
		"loose-group_replication_exit_state_action":               "READ_ONLY",
		"loose-group_replication_unreachable_majority_timeout":    strconv.Itoa(g.UnreachableMajorityTimeoutSeconds),
		"loose-group_replication_autorejoin_tries":                strconv.Itoa(g.AutorejoinTries),
		"loose-group_replication_member_weight":                   strconv.Itoa(g.MemberWeight),
		"loose-group_replication_start_on_boot":                   "OFF",
	}
}
func groupReceiptPath(dir string, s myspec.Spec) string {
	return filepath.Join(dir, "group-joined-"+s.GroupReplication.GroupName+"-"+strconv.FormatUint(s.GroupReplication.FormationEpoch, 10))
}
func GroupJoined(dir string, s myspec.Spec) bool {
	if s.GroupReplication == nil {
		return false
	}
	b, e := os.ReadFile(groupReceiptPath(dir, s))
	return e == nil && string(b) == "joined\n"
}
func durableGroupFile(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".group-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// PersistStartOnBoot records a successful ONLINE join. It never writes server
// persisted variables: cloned datadirs must not carry topology or credentials.
func PersistStartOnBoot(ctx context.Context, d GroupDeps) error {
	s := d.GroupSpec()
	if s.GroupReplication == nil {
		return errors.New("group configuration unavailable")
	}
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	var state string
	if err = db.QueryRowContext(ctx, "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&state); err != nil {
		return err
	}
	if state != "ONLINE" {
		return errors.New("group member is not online")
	}
	if err = durableGroupFile(groupReceiptPath(d.GroupReceiptsDir(), s), []byte("joined\n")); err != nil {
		return err
	}
	if refresh, ok := d.(interface{ RefreshGroupConfig(context.Context) error }); ok {
		return refresh.RefreshGroupConfig(ctx)
	}
	return nil
}

// START credentials are session supplied, memory-only, and redacted by MySQL.
// CHANGE REPLICATION SOURCE is deliberately avoided: it persists plaintext in
// replication metadata. The supervisor supplies credentials again after restart.
func startGroupSQL(ctx context.Context, c *sql.Conn, s myspec.Spec) error {
	_, err := c.ExecContext(ctx, "START GROUP_REPLICATION USER="+groupQuote(s.Replication.Username)+", PASSWORD="+groupQuote(s.Replication.Password))
	return err
}
func groupQuote(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "''") + "'"
}
func awaitGroupOnline(ctx context.Context, c *sql.Conn, d GroupDeps) error {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		var state string
		err := c.QueryRowContext(ctx, "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&state)
		if err != nil {
			return err
		}
		if state == "ONLINE" {
			if err = c.Close(); err != nil {
				return err
			}
			return PersistStartOnBoot(ctx, d)
		}
		if state == "ERROR" || state == "OFFLINE" {
			return errors.New("group member failed to join")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func StartGroup(ctx context.Context, d GroupDeps) error {
	groupActionMu.Lock()
	defer groupActionMu.Unlock()
	s := d.GroupSpec()
	if s.GroupReplication == nil {
		return errors.New("group configuration unavailable")
	}
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	var state string
	err = c.QueryRowContext(ctx, "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_ID=@@server_uuid").Scan(&state)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if state == "ONLINE" || state == "RECOVERING" {
		return awaitGroupOnline(ctx, c, d)
	}
	if err = startGroupSQL(ctx, c, s); err != nil {
		return err
	}
	return awaitGroupOnline(ctx, c, d)
}
func BootstrapGroup(ctx context.Context, d GroupDeps, r myspec.GroupRequest) error {
	groupActionMu.Lock()
	defer groupActionMu.Unlock()
	if err := r.Validate(); err != nil {
		return err
	}
	s := d.GroupSpec()
	if !r.Bootstrap || s.GroupReplication == nil || !s.GroupReplication.Bootstrap || r.GroupName != s.GroupReplication.GroupName || r.FormationEpoch != s.GroupReplication.FormationEpoch {
		return errors.New("bootstrap identity rejected")
	}
	identity, err := d.GroupIdentity()
	if err != nil {
		return err
	}
	if !identity.Initialized || identity.FormationEpoch >= r.FormationEpoch {
		return errors.New("bootstrap epoch already consumed or identity absent")
	}
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	// Consume before touching SQL: ambiguous interruption fails closed, not another bootstrap.
	identity.FormationEpoch = r.FormationEpoch
	if err = d.SaveGroupIdentity(identity); err != nil {
		return err
	}
	bootstrapCleared := false
	defer func() {
		if bootstrapCleared {
			return
		}
		// A canceled START may have invalidated its session. Clear the GLOBAL
		// flag on a fresh connection, not that canceled or closed session.
		_ = c.Close()
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = db.ExecContext(cleanup, "SET GLOBAL group_replication_bootstrap_group=OFF")
	}()
	if _, err = c.ExecContext(ctx, "SET GLOBAL group_replication_bootstrap_group=ON"); err != nil {
		return err
	}
	if err = startGroupSQL(ctx, c, s); err != nil {
		return err
	}
	if _, err = c.ExecContext(ctx, "SET GLOBAL group_replication_bootstrap_group=OFF"); err != nil {
		return err
	}
	bootstrapCleared = true
	return awaitGroupOnline(ctx, c, d)
}
func StopGroup(ctx context.Context, d GroupDeps) error {
	groupActionMu.Lock()
	defer groupActionMu.Unlock()
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "STOP GROUP_REPLICATION")
	return err
}
func SwitchoverGroup(ctx context.Context, d GroupDeps, r myspec.SwitchoverRequest) error {
	if err := r.Validate(); err != nil {
		return err
	}
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	// GR validates the UUID in the UDF initializer, before prepared-statement
	// parameters have values. Validate above, then send a literal UUID so the
	// initializer sees the actual argument instead of rejecting an empty one.
	_, err = db.ExecContext(ctx, "SELECT group_replication_set_as_primary("+groupQuote(r.TargetUUID)+")")
	return err
}

type GroupMember struct {
	UUID, Host, State, Role          string
	Port                             int
	ApplierQueue, TransactionsBehind uint64
}

func GroupMembers(ctx context.Context, db *sql.DB) ([]GroupMember, error) {
	rows, err := db.QueryContext(ctx, "SELECT m.MEMBER_ID,m.MEMBER_HOST,m.MEMBER_PORT,m.MEMBER_STATE,m.MEMBER_ROLE,COALESCE(s.COUNT_TRANSACTIONS_IN_QUEUE,0),COALESCE(s.COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE,0) FROM performance_schema.replication_group_members m LEFT JOIN performance_schema.replication_group_member_stats s ON m.MEMBER_ID=s.MEMBER_ID")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []GroupMember
	for rows.Next() {
		var m GroupMember
		if err = rows.Scan(&m.UUID, &m.Host, &m.Port, &m.State, &m.Role, &m.TransactionsBehind, &m.ApplierQueue); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}
func HandoffGroup(ctx context.Context, d GroupDeps) error {
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	var id string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&id); err != nil {
		return err
	}
	members, err := GroupMembers(ctx, db)
	if err != nil {
		return err
	}
	primary := false
	var best *GroupMember
	for i := range members {
		m := &members[i]
		if m.UUID == id && m.Role == "PRIMARY" {
			primary = true
		}
		if m.State == "ONLINE" && m.Role == "SECONDARY" && (best == nil || m.ApplierQueue < best.ApplierQueue) {
			best = m
		}
	}
	if !primary {
		return nil
	}
	if best == nil {
		return errors.New("no online secondary for handoff")
	}
	return SwitchoverGroup(ctx, d, myspec.SwitchoverRequest{TargetUUID: best.UUID})
}

// cloneNativeStatus identifies the native operation, not just its terminal state.
// BEGIN_TIME and SOURCE also distinguish operations across mysqld restarts,
// where native IDs alone need not remain unique.
type cloneNativeStatus struct {
	ID        uint64 `json:"id"`
	BeginTime string `json:"beginTime"`
	Source    string `json:"source"`
	State     string `json:"state"`
}

type cloneReceipt struct {
	Request   myspec.CloneRequest `json:"request"`
	Baseline  *cloneNativeStatus  `json:"baseline,omitempty"`
	Completed bool                `json:"completed"`
}

func readCloneStatus(ctx context.Context, db *sql.DB) (cloneNativeStatus, error) {
	var status cloneNativeStatus
	err := db.QueryRowContext(ctx, "SELECT ID,CAST(BEGIN_TIME AS CHAR),SOURCE,STATE FROM performance_schema.clone_status ORDER BY ID DESC LIMIT 1").Scan(&status.ID, &status.BeginTime, &status.Source, &status.State)
	if errors.Is(err, sql.ErrNoRows) {
		return status, nil
	}
	return status, err
}

func authorizeClone(s myspec.Spec, r myspec.CloneRequest) error {
	g := s.GroupReplication
	if g == nil || len(s.Peers) < 2 || g.Bootstrap || g.CloneDonor == nil || *g.CloneDonor != r.Donor || r.Donor.Name == s.Member || r.Version != s.Version {
		return errors.New("clone is not authorized by current group configuration")
	}
	member, donor := false, false
	for _, p := range s.Peers {
		member = member || p.Name == s.Member
		donor = donor || p == r.Donor
	}
	if !member || !donor || (g.Reclone && g.RecloneReceipt != r.Receipt) {
		return errors.New("clone member, donor, or recovery receipt is not authorized")
	}
	return nil
}

func verifyCloneOperation(record cloneReceipt, current cloneNativeStatus) error {
	if record.Baseline == nil || current.ID == 0 || current.BeginTime == "" || current.Source != net.JoinHostPort(record.Request.Donor.Host, strconv.Itoa(record.Request.Donor.Port)) {
		return errors.New("clone has no matching native receipt")
	}
	baseline := *record.Baseline
	if current.BeginTime <= baseline.BeginTime {
		return errors.New("clone native receipt predates this operation")
	}
	return nil
}

func verifyCloneCompletion(record cloneReceipt, current cloneNativeStatus) error {
	if err := verifyCloneOperation(record, current); err != nil {
		return err
	}
	if current.State != "Completed" {
		return errors.New("clone has no matching completed native receipt")
	}
	return nil
}

func CloneGroup(ctx context.Context, d GroupDeps, r myspec.CloneRequest) error {
	groupActionMu.Lock()
	defer groupActionMu.Unlock()
	if err := r.Validate(); err != nil {
		return err
	}
	s := d.GroupSpec()
	if err := authorizeClone(s, r); err != nil {
		return err
	}
	record := cloneReceipt{Request: r}
	// The intent is serialized only after its native baseline has been captured.
	receipt := filepath.Join(d.GroupReceiptsDir(), "clone-"+r.Receipt)
	if old, err := os.ReadFile(receipt); err == nil {
		if err = json.Unmarshal(old, &record); err != nil {
			return errors.New("invalid clone receipt")
		}
		if record.Request != r {
			return errors.New("clone receipt conflicts")
		}
		if record.Completed {
			return nil
		}
		db, err := d.LocalAccessDB(ctx)
		// A failed SQL probe is not evidence that CLONE stopped copying.
		if err == nil {
			err = db.PingContext(ctx)
		}
		if err != nil {
			if !d.CloneRecipientStopped() {
				return err
			}
			if err = d.RestartAfterClone(ctx); err != nil {
				return err
			}
			db, err = d.LocalAccessDB(ctx)
			if err != nil {
				return err
			}
		}
		return resumeClone(ctx, d, db, receipt, record)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	var actual string
	if err = db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&actual); err != nil {
		return err
	}
	if actual != r.Version {
		return errors.New("local clone version mismatch")
	}
	identity, err := d.GroupIdentity()
	if err != nil {
		return err
	}
	if !s.GroupReplication.Reclone {
		if identity.FormationEpoch != 0 || GroupJoined(d.GroupReceiptsDir(), s) {
			return errors.New("existing member requires explicit recovery clone authorization")
		}
		var gtid string
		var schemas int
		if err = db.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed,(SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('mysql','sys','information_schema','performance_schema'))").Scan(&gtid, &schemas); err != nil {
			return err
		}
		if gtid != "" || schemas != 0 {
			return errors.New("clone would overwrite existing data without recovery authorization")
		}
	}
	// Probe donor over verified TLS with the same recovery identity before destructive clone.
	pool := x509Pool(s.CAPEM)
	if pool == nil {
		return errors.New("invalid clone CA")
	}
	cfg := mysqldriver.NewConfig()
	cfg.User = s.Replication.Username
	cfg.Passwd = s.Replication.Password
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(r.Donor.Host, strconv.Itoa(r.Donor.Port))
	cfg.Timeout = 5 * time.Second
	cfg.TLS = tlsConfig(pool, r.Donor.Host)
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		return err
	}
	donor := sql.OpenDB(connector)
	defer donor.Close()
	var donorVersion string
	if err = donor.QueryRowContext(ctx, "SELECT VERSION()").Scan(&donorVersion); err != nil {
		return err
	}
	if donorVersion != actual {
		return errors.New("donor clone version mismatch")
	}
	baseline, err := readCloneStatus(ctx, db)
	if err != nil {
		return err
	}
	if baseline.ID != 0 && baseline.State != "Completed" && baseline.State != "Failed" {
		return errors.New("another native clone is in progress")
	}
	record.Baseline = &baseline
	request, err := json.Marshal(record)
	if err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, "SET GLOBAL clone_valid_donor_list="+groupQuote(cfg.Addr)); err != nil {
		return err
	}
	if err = durableGroupFile(receipt, request); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "CLONE INSTANCE FROM "+groupQuote(s.Replication.Username)+"@"+groupQuote(r.Donor.Host)+":"+strconv.Itoa(r.Donor.Port)+" IDENTIFIED BY "+groupQuote(s.Replication.Password)+" REQUIRE SSL")
	// Native mysqld has no service manager: 3707 requests a restart. If the
	// server closes the connection before replying, the driver deliberately
	// returns ErrInvalidConn (not retryable driver.ErrBadConn): CLONE may have
	// executed. Never resend it or interrupt a still-active copy: observe the
	// correlated native restart readiness before restarting the recipient.
	var native *mysqldriver.MySQLError
	if errors.Is(err, mysqldriver.ErrInvalidConn) {
		return recoverCloneReply(ctx, d, receipt, record)
	}
	if err != nil && !(errors.As(err, &native) && native.Number == 3707) {
		return err
	}
	return finishClone(ctx, d, receipt, record)
}

func recoverCloneReply(ctx context.Context, d GroupDeps, receipt string, record cloneReceipt) error {
	db, err := d.LocalAccessDB(ctx)
	if err != nil {
		return err
	}
	return resumeClone(ctx, d, db, receipt, record)
}

func resumeClone(ctx context.Context, d GroupDeps, db *sql.DB, receipt string, record cloneReceipt) error {
	current, err := readCloneStatus(ctx, db)
	if err != nil {
		return err
	}
	if err = verifyCloneOperation(record, current); err != nil {
		return err
	}
	if current.State != "Completed" {
		// A recipient can still answer SQL after returning native 3707.
		// FILE SYNC must be complete before restarting; RESTART may still
		// be Not Started when native mysqld has no supervisor (3707).
		// Never interrupt an active copy or an unrelated/failed operation.
		var restart bool
		if err = db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM performance_schema.clone_progress WHERE ID=? AND STAGE='FILE SYNC' AND STATE='Completed') AND EXISTS(SELECT 1 FROM performance_schema.clone_progress WHERE ID=? AND STAGE='RESTART' AND STATE IN ('Not Started','In Progress'))", current.ID, current.ID).Scan(&restart); err != nil {
			return err
		}
		if current.State != "In Progress" || !restart {
			return errors.New("clone has no matching completed or restart-ready native receipt")
		}
	}
	return finishClone(ctx, d, receipt, record)
}
func finishClone(ctx context.Context, d GroupDeps, receipt string, record cloneReceipt) error {
	if err := d.RestartAfterClone(ctx); err != nil {
		return err
	}
	identity, err := d.GroupIdentity()
	if err != nil {
		return err
	}
	restartCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		db, err := d.LocalAccessDB(restartCtx)
		if err == nil {
			err = db.QueryRowContext(restartCtx, "SELECT @@server_uuid").Scan(&identity.ServerUUID)
		}
		if err == nil {
			current, statusErr := readCloneStatus(restartCtx, db)
			if statusErr != nil {
				return statusErr
			}
			if err = verifyCloneCompletion(record, current); err != nil {
				return err
			}
			identity.FormationEpoch = d.GroupSpec().GroupReplication.FormationEpoch
			if err = d.SaveGroupIdentity(identity); err != nil {
				return err
			}
			record.Completed = true
			b, err := json.Marshal(record)
			if err != nil {
				return err
			}
			return durableGroupFile(receipt, b)
		}
		select {
		case <-restartCtx.Done():
			return restartCtx.Err()
		case <-ticker.C:
		}
	}
}
func RegisterGroupRoutes(mux *http.ServeMux, d GroupDeps) {
	for _, action := range []string{myspec.ActionBootstrap, myspec.ActionStartGroup, myspec.ActionStopGroup, myspec.ActionClone, myspec.ActionSwitchover} {
		mux.HandleFunc("POST /v1/mysql/"+action, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
			defer cancel()
			var err error
			decode := func(v any) error {
				dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
				dec.DisallowUnknownFields()
				if e := dec.Decode(v); e != nil {
					return e
				}
				if e := dec.Decode(new(any)); e != io.EOF {
					return errors.New("trailing request")
				}
				return nil
			}
			switch action {
			case myspec.ActionBootstrap:
				var req myspec.GroupRequest
				err = decode(&req)
				if err == nil {
					err = BootstrapGroup(ctx, d, req)
				}
			case myspec.ActionStartGroup:
				err = StartGroup(ctx, d)
			case myspec.ActionStopGroup:
				err = StopGroup(ctx, d)
			case myspec.ActionClone:
				var req myspec.CloneRequest
				err = decode(&req)
				if err == nil {
					err = CloneGroup(ctx, d, req)
				}
			case myspec.ActionSwitchover:
				var req myspec.SwitchoverRequest
				err = decode(&req)
				if err == nil {
					err = SwitchoverGroup(ctx, d, req)
				}
			}
			if err != nil {
				if action == myspec.ActionBootstrap || action == myspec.ActionStartGroup || action == myspec.ActionSwitchover {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(agentapi.ErrorResponse{Error: groupActionDiagnostic(d, action, err)})
				} else {
					http.Error(w, "MySQL group action unavailable", http.StatusServiceUnavailable)
				}
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, "{}")
		})
	}
}
