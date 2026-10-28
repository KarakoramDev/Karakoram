// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

type recoveryEvents struct{ events []domain.Event }

func (r *recoveryEvents) Record(_ context.Context, e domain.Event) error {
	r.events = append(r.events, e)
	return nil
}

func TestDriftedAdministratorRotationRecoversOffline(t *testing.T) {
	p, rc, _, a := setup(t)
	converge(t, p, rc)
	events := &recoveryEvents{}
	rc.Events = events
	old, _ := secret(t.Context(), rc, "users/neo4j")
	a.password = "unknown-drifted-password"
	status, err := p.Status(t.Context(), rc)
	if err != nil || len(status.Conditions) == 0 || !strings.Contains(status.Conditions[0].Message, "rotate the neo4j system user") {
		t.Fatal("missing repair guidance", status.Conditions, err)
	}
	op := &domain.Operation{ID: "repair-drift", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"neo4j"}`)}
	step := func() {
		t.Helper()
		r, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
	}
	step() // Stage the next password.
	next, _ := secret(t.Context(), rc, "operations/"+op.ID+"/password")
	generatedPasswordContract.Assert(t, "staged offline recovery password", string(next))
	step() // Explicit authentication rejection; authorize offline recovery.
	var cp operationCheckpoint
	json.Unmarshal(op.Checkpoint, &cp)
	if cp.Step != "recover-admin" {
		t.Fatal(cp.Step)
	}
	step() // Submit the durable job, which remains running.
	stillOld, _ := secret(t.Context(), rc, "users/neo4j")
	if string(stillOld) != string(old) {
		t.Fatal("committed before recovery")
	}
	a.failRecoveryReply = true
	if _, err = p.RunOperation(t.Context(), rc, op); err == nil || strings.Contains(err.Error(), "do-not-leak") {
		t.Fatal("missing safe interrupted-job error", err)
	}
	// Restart the provider while retaining the pre-completion checkpoint. The
	// same agent receipt must be polled, never replaced by another recovery.
	p = New(Options{})
	runOp(t, p, rc, op)
	committed, _ := secret(t.Context(), rc, "users/neo4j")
	if string(committed) != string(next) || a.password != string(next) || a.recoveries != 1 || a.recoveryReceipt != op.ID+"-recover-admin" {
		t.Fatal("offline repair did not converge idempotently")
	}
	generatedPasswordContract.Assert(t, "native offline recovery password", a.recoveryPassword)
	if a.recoveryPassword != string(next) {
		t.Fatal("native recovery request differs from staged password")
	}
	if len(events.events) != 1 || events.events[0].Reason != "AdministratorCredentialsRecovered" || events.events[0].Operation != op.ID || !strings.Contains(events.events[0].Message, "outage") {
		t.Fatal(events.events)
	}
	raw, _ := json.Marshal(events.events)
	if strings.Contains(string(raw), string(next)) || strings.Contains(string(op.Checkpoint), string(next)) {
		t.Fatal("repair leaked staged password")
	}
	runOp(t, p, rc, op)
	if a.recoveries != 1 || len(events.events) != 1 {
		t.Fatal("terminal retry repeated recovery")
	}
}

func TestAdministratorTransientFailureDoesNotRecoverOffline(t *testing.T) {
	for _, code := range []int{401, 403, 500, 503} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			p, rc, _, a := setup(t)
			converge(t, p, rc)
			a.adminErrorCode = code
			op := &domain.Operation{ID: "no-repair", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"neo4j"}`)}
			r, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = r.Checkpoint
			if _, err = p.RunOperation(t.Context(), rc, op); err == nil {
				t.Fatal("missing transient action failure")
			}
			if a.recoveries != 0 {
				t.Fatal("non-authentication failure caused outage")
			}
		})
	}
}

func TestFailedAdministratorRecoveryDoesNotCommitSecret(t *testing.T) {
	p, rc, _, a := setup(t)
	converge(t, p, rc)
	old, _ := secret(t.Context(), rc, "users/neo4j")
	a.password = "drifted"
	a.failRecoveryJob = true
	op := &domain.Operation{ID: "failed-repair", Kind: domain.OperationRotateSystemUser, Payload: json.RawMessage(`{"user":"neo4j"}`)}
	for range 3 {
		r, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = r.Checkpoint
	}
	_, err := p.RunOperation(t.Context(), rc, op)
	if err == nil || !strings.Contains(err.Error(), "offline administrator recovery failed") || strings.Contains(err.Error(), "do-not-leak") {
		t.Fatal(err)
	}
	current, e := secret(t.Context(), rc, "users/neo4j")
	if e != nil || string(current) != string(old) {
		t.Fatal("failed recovery committed secret", e)
	}
	staged, e := secret(t.Context(), rc, "operations/"+op.ID+"/password")
	if e != nil || len(staged) == 0 {
		t.Fatal("lost durable repair credential", e)
	}
}
