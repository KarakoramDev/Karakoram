//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

type nativeISTWriteRow struct{ id, port int64 }
type nativeISTBacklogRow struct {
	id     int64
	digest string
}
type nativeISTData struct {
	writes  []nativeISTWriteRow
	backlog []nativeISTBacklogRow
	gtid    string
}

// Galera reports a writeset aborted by certification or a concurrent
// membership change as ER_LOCK_DEADLOCK after rolling it back; clients retry.
func nativeGaleraExec(ctx context.Context, db *sql.DB, query string, args ...any) error {
	for attempt := 1; ; attempt++ {
		_, err := db.ExecContext(ctx, query, args...)
		var native *mysqldriver.MySQLError
		if err == nil || attempt == 5 || !errors.As(err, &native) || native.Number != 1213 {
			return err
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func nativeISTDataSnapshot(ctx context.Context, db *sql.DB) (nativeISTData, error) {
	var out nativeISTData
	rows, err := db.QueryContext(ctx, "SELECT id,writer_port FROM galera_runner.writes ORDER BY id")
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var row nativeISTWriteRow
		if err = rows.Scan(&row.id, &row.port); err != nil {
			rows.Close()
			return out, err
		}
		out.writes = append(out.writes, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	rows, err = db.QueryContext(ctx, "SELECT id,SHA2(payload,256) FROM galera_runner.ist_backlog ORDER BY id")
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var row nativeISTBacklogRow
		if err = rows.Scan(&row.id, &row.digest); err != nil {
			rows.Close()
			return out, err
		}
		out.backlog = append(out.backlog, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	err = db.QueryRowContext(ctx, "SELECT @@gtid_executed").Scan(&out.gtid)
	return out, err
}
func nativeISTMemberMatches(ctx context.Context, a *groupRunnerAgent, want nativeISTData, stateUUID string, size int) error {
	observation, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var status myspec.GaleraStatusResponse
	if err := a.client.Engine(observation, "mysql", myspec.ActionGaleraStatus, struct{}{}, &status); err != nil {
		return err
	}
	if !status.Eligible() || status.StateUUID != stateUUID || size > 0 && status.ClusterSize != size {
		return fmt.Errorf("member%s is not in expected Synced component", a.spec.Member)
	}
	db, err := a.runner.LocalAccessDB(observation)
	if err != nil {
		return err
	}
	got, err := nativeISTDataSnapshot(observation, db)
	if err != nil {
		return err
	}
	if !slices.Equal(got.writes, want.writes) || !slices.Equal(got.backlog, want.backlog) {
		return errors.New("member rows or payload hashes differ from original quorum")
	}
	expectedGTID, err := parseRecoveryGTIDs(want.gtid)
	if err != nil {
		return err
	}
	actualGTID, err := parseRecoveryGTIDs(got.gtid)
	if err != nil {
		return err
	}
	if !actualGTID.contains(expectedGTID) || !expectedGTID.contains(actualGTID) {
		return errors.New("member GTIDs differ from original quorum")
	}
	return nil
}

// Deliberate unauthorized IST connections are destructive fault tests, not an
// availability claim. Run them only after all positive lifecycle acceptance.
// The independent agent API fences the fault, then the unchanged product path
// must recover that very member and its data without repair or bootstrap.
func nativeGaleraISTFaultRecoveryProof(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, dir string) {
	t.Helper()
	if len(nodes) != 5 {
		t.Fatal("IST fault recovery requires the real five-member cluster")
	}
	resetObserved := false
	for index, kind := range []string{"SQL-CA", "self-signed"} {
		target := nodes[3+index]
		source := nodes[0]
		db, err := source.runner.LocalAccessDB(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before, err := nativeISTDataSnapshot(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		var original myspec.GaleraStatusResponse
		source.action(ctx, myspec.ActionGaleraStatus, struct{}{}, &original)
		observation, cancelObservation := context.WithTimeout(ctx, 2*time.Second)
		process, err := target.client.Status(observation)
		cancelObservation()
		if err != nil || process.Processes["mysqld"].PID <= 0 {
			t.Fatalf("IST fault target has no live owned process: %v", err)
		}
		logPath := filepath.Join(dataPaths(filepath.Join(dir, target.spec.Member)).root, "mysql-error.log")
		logInfo, err := os.Stat(logPath)
		if err != nil {
			t.Fatal(err)
		}
		began := time.Now()
		probe := nativeISTAdmissionProbe(t, ctx, target.spec, kind)
		t.Logf("IST fault trigger: version=%s member=%s cert=%s ISTport=%d ownedPID=%d", target.spec.Version, target.spec.Member, kind, target.spec.Galera.ISTPort, process.Processes["mysqld"].PID)
		if err = syscall.Kill(process.Processes["mysqld"].PID, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		writeID := 4000 + index
		if err = nativeGaleraExec(ctx, db, "INSERT INTO galera_runner.writes VALUES(?,@@port)", writeID); err != nil {
			t.Fatal(err)
		}
		// A bounded real writeset extends the live receiver window; there are no
		// provider hooks, fake streams, certificate substitutions or script patches.
		var backlog strings.Builder
		backlog.WriteString("INSERT INTO galera_runner.ist_backlog VALUES")
		for row := range 128 {
			if row > 0 {
				backlog.WriteByte(',')
			}
			fmt.Fprintf(&backlog, "(%d,REPEAT('x',131072))", 100000+index*1000+row)
		}
		if err = nativeGaleraExec(ctx, db, backlog.String()); err != nil {
			t.Fatal(err)
		}
		if err = probe.wait(); err != nil {
			t.Fatalf("IST %s did not produce strict remote rejection of the presented leaf: %v", kind, err)
		}
		t.Logf("IST fault remote rejection proven: version=%s member=%s cert=%s ISTport=%d elapsed=%s", target.spec.Version, target.spec.Member, kind, target.spec.Galera.ISTPort, time.Since(began))
		// The first attempt is fenced through the agent API and released by an
		// explicit CP-style start; the second is left entirely to the supervisor.
		fenced := index == 0
		if fenced {
			nativeGaleraFenceStop(t, ctx, target)
		}
		aborted := nativeISTAbortEvidence(t, target.spec, kind, logPath, logInfo.Size(), time.Since(began))
		reset := nativeGaleraResetObserved(t, dir, target.spec)
		resetObserved = resetObserved || reset
		// Only an observed receiver abort exercises interrupted-transfer
		// recovery; otherwise this attempt proves rejection plus ordinary IST.
		exercised := map[bool]string{true: "exercised", false: "not-exercised"}[aborted]
		t.Logf("IST fault post-attempt state: version=%s member=%s cert=%s fenced=%v receiverAbort=%v upstreamTransferResetObserved=%v interruptedTransferRecovery=%s", target.spec.Version, target.spec.Member, kind, fenced, aborted, reset, exercised)
		expected, err := nativeISTDataSnapshot(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		if len(expected.writes) != len(before.writes)+1 || !slices.Equal(expected.writes[:len(before.writes)], before.writes) || len(expected.backlog) != len(before.backlog)+128 || !slices.Equal(expected.backlog[:len(before.backlog)], before.backlog) {
			t.Fatal("original quorum lost existing rows during the IST fault")
		}
		oldGTID, err := parseRecoveryGTIDs(before.gtid)
		if err != nil {
			t.Fatal(err)
		}
		newGTID, err := parseRecoveryGTIDs(expected.gtid)
		if err != nil || !newGTID.contains(oldGTID) {
			t.Fatal("original quorum lost prior GTIDs during the IST fault", err)
		}
		quorumSize := 0
		if fenced {
			quorumSize = 4
		}
		nativeGroupPoll(t, 60*time.Second, "original quorum retains all rows/GTIDs after rejected IST", func() bool {
			for _, a := range nodes[:3] {
				if nativeISTMemberMatches(ctx, a, expected, original.StateUUID, quorumSize) != nil {
					return false
				}
			}
			return true
		})
		if fenced {
			nativeGaleraReleaseRecover(t, ctx, nodes, target, expected, original.StateUUID, "fenced attacked member recovers normally to five Synced equal-data members")
		} else {
			nativeGaleraAwaitFive(t, ctx, nodes, expected, original.StateUUID, "supervisor alone recovers attacked member to five Synced equal-data members")
		}
		nativeGaleraGTIDQuorumProof(t, ctx, nodes, "hostile "+kind+" IST recovery")
		nativeGaleraTLSProof(t, ctx, nodes, dir, *source.spec.Galera.TrafficCert)
		t.Logf("IST fault normal-product recovery complete: version=%s member=%s cert=%s fenced=%v interruptedTransferRecovery=%s resetObserved=%v elapsed=%s rows=%d GTIDs=%s", target.spec.Version, target.spec.Member, kind, fenced, exercised, reset, time.Since(began), len(expected.writes), expected.gtid)
	}
	// Reset recovery is claimed only when upstream produced it natively;
	// file-level admission of the reset is covered by unit regressions.
	t.Logf("IST fault summary: version=%s upstreamTransferResetObserved=%v", nodes[0].spec.Version, resetObserved)
}

func nativeGaleraFenceStop(t *testing.T, ctx context.Context, target *groupRunnerAgent) {
	t.Helper()
	operation, cancelOperation := context.WithTimeout(ctx, 45*time.Second)
	err := target.client.Maintenance(operation, agentapi.Maintenance{Enabled: true})
	if err == nil {
		err = target.client.Process(operation, agentapi.ProcessAction{Process: "mysqld", Action: "stop"})
	}
	cancelOperation()
	if err != nil {
		t.Fatalf("product API could not fence/stop member%s: %v", target.spec.Member, err)
	}
	observation, cancelObservation := context.WithTimeout(ctx, 2*time.Second)
	stopped, err := target.client.Status(observation)
	cancelObservation()
	if err != nil || !stopped.Maintenance || stopped.Processes["mysqld"].PID != 0 {
		t.Fatalf("member%s not fenced/stopped: %v", target.spec.Member, err)
	}
}

// This is the only recovery path: release production safety controls and
// start the existing CP-authorized member. No receipt edits, new formation,
// manual recovery-position injection or identity exceptions.
func nativeGaleraReleaseRecover(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, target *groupRunnerAgent, expected nativeISTData, stateUUID, label string) {
	t.Helper()
	operation, cancelOperation := context.WithTimeout(ctx, 45*time.Second)
	err := target.client.Maintenance(operation, agentapi.Maintenance{Enabled: false})
	if err == nil {
		err = target.client.Process(operation, agentapi.ProcessAction{Process: "mysqld", Action: "start"})
	}
	cancelOperation()
	if err != nil {
		t.Fatalf("PRODUCT DEFECT: normal recovery of member%s is blocked: %v", target.spec.Member, err)
	}
	nativeGaleraAwaitFive(t, ctx, nodes, expected, stateUUID, label)
}

// Recovery after an interrupted transfer includes the launcher's stale-donor
// hold-off before the state transfer itself.
func nativeGaleraAwaitFive(t *testing.T, ctx context.Context, nodes []*groupRunnerAgent, expected nativeISTData, stateUUID, label string) {
	t.Helper()
	nativeGroupPoll(t, galeraTransferHoldoff+3*time.Minute, label, func() bool {
		for _, a := range nodes {
			if nativeISTMemberMatches(ctx, a, expected, stateUUID, 5) != nil {
				return false
			}
		}
		return true
	})
}

func nativeGaleraResetObserved(t *testing.T, dir string, s myspec.Spec) bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dataPaths(filepath.Join(dir, s.Member)).data, "grastate.dat"))
	if err != nil {
		t.Logf("member%s Galera state unreadable while fenced: %v", s.Member, err)
		return false
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return fields["uuid"] == galeraTransferRequiredUUID && fields["seqno"] == "-1" && fields["safe_to_bootstrap"] == "0"
}

// nativeISTAbortEvidence reports whether the receiving mysqld aborted with
// signal 6 after the trigger, logging the bounded native signature.
func nativeISTAbortEvidence(t *testing.T, s myspec.Spec, kind, path string, offset int64, elapsed time.Duration) bool {
	t.Helper()
	// The receiver writes its crash report just after the remote alert.
	var raw []byte
	var err error
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		raw, err = os.ReadFile(path)
		if err == nil && int64(len(raw)) >= offset && strings.Contains(strings.ToLower(string(raw[offset:])), "got signal 6") || time.Now().After(deadline) {
			break
		}
	}
	if err != nil || int64(len(raw)) < offset {
		t.Logf("IST fault signature unavailable: version=%s cert=%s elapsed=%s err=%v", s.Version, kind, elapsed, err)
		return false
	}
	count, aborted := 0, false
	for _, line := range strings.Split(string(raw[offset:]), "\n") {
		lower := strings.ToLower(line)
		aborted = aborted || strings.Contains(lower, "got signal 6")
		if count < 20 && (strings.Contains(lower, "abort") || strings.Contains(lower, "terminate") || strings.Contains(lower, "signal 6") || strings.Contains(lower, "attempting backtrace") || strings.Contains(lower, "stack_bottom") || strings.HasPrefix(strings.TrimSpace(line), "#")) {
			t.Logf("IST fault observed native signature: version=%s cert=%s elapsed=%s %s", s.Version, kind, elapsed, nativeGaleraRedact(s, "native-galera-token-"+s.Member, line))
			count++
		}
	}
	if count == 0 {
		t.Logf("IST fault: no abort signature observed in this post-trigger log window (version=%s cert=%s elapsed=%s)", s.Version, kind, elapsed)
	}
	return aborted
}
