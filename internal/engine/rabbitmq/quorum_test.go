// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func TestRollingQuorumBlockHasQueuesConditionOperationMessageAndSingleEvent(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	events := &definitionsTestEvents{}
	rc.Events = events
	a.broker("demom0").critical = true
	a.failures["demom0:await-quorum"] = 503
	a.queueSource = func() []mgmt.Queue {
		return []mgmt.Queue{{Name: "orders", Vhost: "/", Type: "quorum", Members: []string{"rabbit@demom0"}}}
	}
	a.log.clear()
	before := f.mutations
	op := &domain.Operation{Kind: domain.OperationRestart}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	for range 2 {
		result, err = p.RunOperation(t.Context(), rc, op)
		if err != nil || result.Done || result.RequeueAfter != 30*time.Second || !strings.Contains(result.Message, `"orders" in vhost "/"`) {
			t.Fatal(result, err)
		}
		op.Checkpoint = result.Checkpoint
		rc.Cluster = cloneCluster(t, rc.Cluster)
	}
	if len(events.events) != 1 || events.events[0].Reason != "RollingRestartBlocked" || events.events[0].Type != "Warning" {
		t.Fatal(events.events)
	}
	if f.mutations != before || !slices.Contains(a.log.snapshot(), "agent:demom0:await-quorum") || slices.Contains(a.log.snapshot(), "agent:demom0:drain") {
		t.Fatal("missing wait or unsafe disruption", a.log.snapshot())
	}
	// Running operations isolate ordinary Status; the provider must project the
	// condition directly into the atomically persisted operation/cluster state.
	if !strings.Contains(rc.Cluster.Status.Message, "orders") || !slices.ContainsFunc(rc.Cluster.Status.Conditions, func(c domain.Condition) bool {
		return c.Type == "QuorumCriticalQueues" && c.Reason == "RollingRestartBlocked"
	}) {
		t.Fatal(rc.Cluster.Status)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status.Message, "orders") {
		t.Fatal(status)
	}
	var commands string
	for _, condition := range status.Conditions {
		if condition.Type == "QuorumReplicaRecovery" {
			commands = condition.Message
		}
	}
	for _, value := range []string{"https://demom0:15671/api/queues/quorum/%2F/orders/replicas/add", `{"node":"rabbit@demom1"}`, `{"node":"rabbit@demom2"}`, "/status", "--user admin", "--cacert ca.pem"} {
		if !strings.Contains(commands, value) {
			t.Fatalf("missing %q: %s", value, commands)
		}
	}
	a.broker("demom0").critical = false
	delete(a.failures, "demom0:await-quorum")
	for range 2 {
		result, err = p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
	}
	if slices.ContainsFunc(rc.Cluster.Status.Conditions, func(c domain.Condition) bool { return c.Type == "QuorumCriticalQueues" }) {
		t.Fatal(rc.Cluster.Status)
	}
	if len(events.events) != 2 || events.events[1].Type != "Normal" || events.events[1].Reason != "RollingRestartRecovered" {
		t.Fatal(events.events)
	}
}

func TestQuorumBlockedOperationsUseSafetyWaitIntervalAndMessage(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationRotateTLS, domain.OperationUpdate, domain.OperationUpgrade} {
		for _, gate := range []string{"quorum", "critical"} {
			t.Run(kind+"/"+gate, func(t *testing.T) {
				p, rc, _, a := setup(t)
				ready(t, p, rc)
				s := state(t, rc)
				m := s.find("demom0")
				beginChange(rc, s, m, "deploy")
				m.ChangeStep = gate
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				op := &domain.Operation{Kind: kind}
				if kind == domain.OperationRestart || kind == domain.OperationRotateTLS {
					var err error
					op.Checkpoint, err = json.Marshal(operationState{Waiting: m.Name})
					if err != nil {
						t.Fatal(err)
					}
				}
				a.broker(m.Name).critical = true
				if gate == "quorum" {
					a.failures[m.Name+":await-quorum"] = 503
				}
				for range 2 {
					result, err := p.RunOperation(t.Context(), rc, op)
					if err != nil || result.Done || result.RequeueAfter != 30*time.Second || !strings.Contains(result.Message, `"orders" in vhost "/"`) {
						t.Fatal(result, err)
					}
					op.Checkpoint = result.Checkpoint
					rc.Cluster = cloneCluster(t, rc.Cluster)
					if current := state(t, rc).find(m.Name); current.ChangeStep != gate {
						t.Fatal("blocked operation advanced", current)
					}
				}
				a.broker(m.Name).critical = false
				delete(a.failures, m.Name+":await-quorum")
				// Passing the final pre-drain gate restores the ordinary cadence.
				for range 2 {
					result, err := p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatal(err)
					}
					op.Checkpoint = result.Checkpoint
					if state(t, rc).find(m.Name).ChangeStep == "drain" {
						if result.RequeueAfter != time.Second || result.Message != "" {
							t.Fatal("recovered operation retained the blocked cadence or message", result)
						}
						return
					}
				}
				t.Fatal("recovered operation did not reach the pre-drain gate")
			})
		}
	}
}

func TestQuorumWaitFailureRunsDiagnosis(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom0")
	beginChange(rc, s, m, "resources")
	a.failures["demom0:await-quorum"] = 503
	a.requestHook = func(action string) {
		if action == "await-quorum" {
			a.broker("demom0").critical = true
		}
	}
	a.log.clear()
	_, err := p.changeStep(t.Context(), rc, s, m)
	if err != nil {
		t.Fatal(err)
	}
	if countCall(a.log.snapshot(), "agent:demom0:quorum-critical") != 1 || len(m.QuorumBlockers) != 1 {
		t.Fatal(a.log.snapshot(), m)
	}
}

func TestQuorumBlockedSyncWaitsWithoutReconcileErrors(t *testing.T) {
	for _, gate := range []string{"quorum", "critical"} {
		t.Run(gate, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			events := &definitionsTestEvents{}
			rc.Events = events
			s := state(t, rc)
			m := s.find("demom0")
			beginChange(rc, s, m, "deploy")
			m.ChangeStep = gate
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if gate == "quorum" {
				a.failures[m.Name+":await-quorum"] = 503
			}
			a.log.clear()
			before := f.mutations
			for _, queue := range []string{"orders", "orders", "payments", "payments"} {
				a.broker(m.Name).blockers = []rmqspec.QuorumBlocker{{VHost: "/", Queue: queue, Type: "quorum"}}
				result, err := p.Sync(t.Context(), rc)
				// Returning an error here makes the reconciler emit ReconcileError
				// on every pass, duplicating the transition-only blocked event.
				if err != nil || result.RequeueAfter != 30*time.Second {
					t.Fatal(result, err)
				}
				rc.Cluster = cloneCluster(t, rc.Cluster)
				current := state(t, rc).find(m.Name)
				if current.ChangeStep != gate || len(current.QuorumBlockers) != 1 || current.QuorumBlockers[0].Queue != queue {
					t.Fatal("blocked change advanced or lost its diagnosis", current)
				}
			}
			if len(events.events) != 2 {
				t.Fatal("expected one warning per distinct blocking reason", events.events)
			}
			for i, queue := range []string{"orders", "payments"} {
				event := events.events[i]
				if event.Reason != "RollingRestartBlocked" || event.Type != "Warning" || !strings.Contains(event.Message, queue) {
					t.Fatal(event)
				}
			}
			if f.mutations != before || slices.Contains(a.log.snapshot(), "agent:"+m.Name+":drain") {
				t.Fatal("blocked restart disrupted a broker", a.log.snapshot())
			}
			a.failures[m.Name+":quorum-critical"] = 503
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("failed safety observation was treated as a known blocker")
			}
		})
	}
}

func TestQuorumMessagesAreCappedAndCommandsQuoteIdentities(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom0")
	for i := range 12 {
		m.QuorumBlockers = append(m.QuorumBlockers, rmqspec.QuorumBlocker{Queue: fmt.Sprintf("queue-%02d", i), VHost: "/", Type: "quorum"})
	}
	message := quorumMessage(m)
	if !strings.Contains(message, "and 2 more") || strings.Contains(message, "queue-10") {
		t.Fatal(message)
	}
	m.QuorumBlockers = []rmqspec.QuorumBlocker{{Queue: "orders' /?", VHost: "/prod", Type: "quorum", Members: []string{"rabbit@demom0"}}}
	commands := quorumRecovery(rc, s, m)
	if !strings.Contains(commands, "%2Fprod/orders%27%20%2F%3F") {
		t.Fatal("queue identity is not encoded", commands)
	}
	if shellQuote("a'$(id)") != "'a'\"'\"'$(id)'" {
		t.Fatal("shell quoting allows command substitution")
	}
	if strings.Contains(commands, "--insecure") || strings.Contains(commands, "rabbitmq-queues") {
		t.Fatal(commands)
	}
}

func TestRemovalBlockedMemberStaysVisibleAndWarnsOnce(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	events := &definitionsTestEvents{}
	rc.Events = events
	a.queueSource = func() []mgmt.Queue {
		return []mgmt.Queue{{Name: "classic-orders", Vhost: "/customers", Type: "classic", Durable: true, Node: "rabbit@demom2"}}
	}
	s := state(t, rc)
	s.find("demom2").Removing = true
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 2
	for range 2 {
		if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrConflict) {
			t.Fatal(err)
		}
		rc.Cluster = cloneCluster(t, rc.Cluster)
	}
	if len(events.events) != 1 || events.events[0].Reason != "MemberRemovalBlocked" || !strings.Contains(events.events[0].Message, "demom2") {
		t.Fatal(events.events)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Members) != 3 {
		t.Fatal(status.Members)
	}
	leaving := status.Members[2]
	if leaving.Name != "demom2" || leaving.State != "LEAVING_BLOCKED" || leaving.Health || !strings.Contains(leaving.Agent.Message, "classic-orders") {
		t.Fatal(leaving)
	}
	if !slices.ContainsFunc(status.Conditions, func(c domain.Condition) bool {
		return c.Type == "MemberRemovalBlocked" && strings.Contains(c.Message, "classic-orders")
	}) {
		t.Fatal(status.Conditions)
	}
	// The broker remains visible after unblocking until it is actually removed.
	s = state(t, rc)
	s.find("demom2").RemovalBlocked = ""
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	status, err = p.Status(t.Context(), rc)
	if err != nil || len(status.Members) != 3 || status.Members[2].State != "LEAVING" || status.Members[2].Health {
		t.Fatal(status, err)
	}
}

func TestNativeMemberProvisioningStagesAreProjected(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	for _, tc := range []struct {
		stage           string
		bootstrap, down bool
		want            string
	}{
		{"planned", false, false, "PLANNED: waiting for platform import"},
		{"imported", false, false, "IMPORTED: waiting for package deployment"},
		{"deployed", false, true, "DEPLOYED: waiting for agent and broker readiness"},
		{"deployed", false, false, "AGENT_READY: bootstrapping first broker"},
		{"deployed", true, false, "AGENT_READY: joining RabbitMQ cluster"},
		{"resume-deployed", true, false, "DEPLOYED: starting retained broker"},
		{"resume-revive", true, false, "AGENT_READY: reviving retained broker"},
		{"ready", true, false, "RUNNING"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			s := state(t, rc)
			s.find("demom0").Stage = tc.stage
			s.Bootstrapped = tc.bootstrap
			a.broker("demom0").down = tc.down
			if err := s.save(rc.Cluster); err != nil {
				t.Fatal(err)
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Members[0].State != tc.want || (tc.stage != "ready" && status.Members[0].Health) {
				t.Fatal(status.Members[0])
			}
		})
	}
}

func TestRollingQuorumWaitAllowsRecoveringReplicas(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	events := &definitionsTestEvents{}
	rc.Events = events
	s := state(t, rc)
	m := s.find("demom0")
	beginChange(rc, s, m, "restart")
	a.broker(m.Name).critical = true
	a.requestHook = func(action string) {
		if action == "await-quorum" {
			a.broker(m.Name).critical = false
		}
	}
	a.log.clear()
	for range 2 {
		if _, err := p.changeStep(t.Context(), rc, s, m); err != nil {
			t.Fatal(err)
		}
	}
	assertBefore(t, a.log.snapshot(), "agent:demom0:await-quorum", "agent:demom0:quorum-critical")
	if len(m.QuorumBlockers) != 0 || len(events.events) != 0 || m.ChangeStep != "drain" {
		t.Fatal(m, events.events)
	}
}

func TestQuorumWarningIgnoresMembershipInventoryChanges(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	events := &definitionsTestEvents{}
	rc.Events = events
	s := state(t, rc)
	m := s.find("demom0")
	a.broker(m.Name).blockers = []rmqspec.QuorumBlocker{{VHost: "/", Queue: "orders", Type: "quorum"}}
	for _, members := range [][]string{nil, {"rabbit@demom0"}, nil} {
		a.queueSource = func() []mgmt.Queue {
			return []mgmt.Queue{{Name: "orders", Vhost: "/", Members: members}}
		}
		if blocked, err := p.checkQuorum(t.Context(), rc, m); !blocked || err != nil {
			t.Fatal(err)
		}
	}
	if len(events.events) != 1 {
		t.Fatal(events.events)
	}
}

func TestQuorumBlockersDoNotReplaceCredentialRotationMessage(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	s.find("demom0").QuorumBlockers = []rmqspec.QuorumBlocker{{VHost: "/", Queue: "orders", Type: "quorum"}}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	result, err := p.RunOperation(t.Context(), rc, &domain.Operation{
		ID: "rotation", Kind: domain.OperationRotateSystemUser, Payload: []byte(`{"user":"admin"}`),
	})
	if err != nil || result.Step != "password-staged" || result.Message != "" {
		t.Fatal(result, err)
	}
}

func TestQuorumBlockersValidateAgentAndInventoryIdentities(t *testing.T) {
	for _, tc := range []struct {
		name      string
		blocker   rmqspec.QuorumBlocker
		inventory []mgmt.Queue
	}{
		{"queue control", rmqspec.QuorumBlocker{VHost: "/", Queue: "queue\ncommand", Type: "quorum"}, nil},
		{"vhost control", rmqspec.QuorumBlocker{VHost: "/\x7f", Queue: "queue", Type: "quorum"}, nil},
		{"queue length", rmqspec.QuorumBlocker{VHost: "/", Queue: strings.Repeat("q", 256), Type: "quorum"}, nil},
		{"vhost length", rmqspec.QuorumBlocker{VHost: strings.Repeat("v", 256), Queue: "queue", Type: "quorum"}, nil},
		{"empty identity", rmqspec.QuorumBlocker{VHost: "", Queue: "queue", Type: "quorum"}, nil},
		{"type", rmqspec.QuorumBlocker{VHost: "/", Queue: "queue", Type: "quorum\ncommand"}, nil},
		{"agent member", rmqspec.QuorumBlocker{VHost: "/", Queue: "queue", Type: "quorum", Members: []string{"rabbit@node\ncommand"}}, nil},
		{"inventory member", rmqspec.QuorumBlocker{VHost: "/", Queue: "queue", Type: "quorum"}, []mgmt.Queue{{Vhost: "/", Name: "queue", Members: []string{"rabbit@$(command)"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			events := &definitionsTestEvents{}
			rc.Events = events
			m := state(t, rc).find("demom0")
			m.QuorumBlockers = []rmqspec.QuorumBlocker{{VHost: "/", Queue: "previous", Type: "quorum"}}
			a.broker(m.Name).blockers = []rmqspec.QuorumBlocker{tc.blocker}
			a.queueSource = func() []mgmt.Queue { return tc.inventory }
			blocked, err := p.checkQuorum(t.Context(), rc, m)
			if blocked || err == nil || errors.Is(err, engine.ErrConflict) || strings.Contains(err.Error(), "command") {
				t.Fatal("unsafe blocker accepted or exposed", err)
			}
			if len(m.QuorumBlockers) != 1 || m.QuorumBlockers[0].Queue != "previous" || len(events.events) != 0 {
				t.Fatal(m.QuorumBlockers, events.events)
			}
		})
	}
}

func TestQuorumBlockersBoundStoredStateAndKeepTotalCount(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom0")
	for i := range 60 {
		a.broker(m.Name).blockers = append(a.broker(m.Name).blockers,
			rmqspec.QuorumBlocker{VHost: "/", Queue: fmt.Sprintf("queue-%03d", i), Type: "quorum"})
	}
	if blocked, err := p.checkQuorum(t.Context(), rc, m); !blocked || err != nil {
		t.Fatal(err)
	}
	if len(m.QuorumBlockers) != 50 {
		t.Fatalf("stored %d blockers, want 50", len(m.QuorumBlockers))
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	s = state(t, rc)
	m = s.find("demom0")
	for _, message := range []string{quorumMessage(m), quorumRecovery(rc, s, m)} {
		if !strings.Contains(message, "50 more") || strings.Contains(message, "queue-010") {
			t.Fatal(message)
		}
	}
	a.broker(m.Name).blockers = nil
	if blocked, err := p.checkQuorum(t.Context(), rc, m); blocked || err != nil {
		t.Fatal(err)
	}
	m.QuorumBlockers = []rmqspec.QuorumBlocker{{VHost: "/", Queue: "single", Type: "quorum"}}
	if strings.Contains(quorumMessage(m), "more") {
		t.Fatal("recovered blocker count was retained", quorumMessage(m))
	}
}

func TestQuorumFailedWaitDoesNotAnnounceRecoveryWithoutBlockers(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	events := &definitionsTestEvents{}
	rc.Events = events
	s := state(t, rc)
	m := s.find("demom0")
	beginChange(rc, s, m, "restart")
	m.QuorumBlockers = []rmqspec.QuorumBlocker{{VHost: "/", Queue: "orders", Type: "quorum"}}
	m.QuorumBlockerCount = 1
	a.failures[m.Name+":await-quorum"] = 503
	done, err := p.changeStep(t.Context(), rc, s, m)
	if done || err == nil || m.ChangeStep != "quorum" {
		t.Fatal(done, err, m.ChangeStep)
	}
	if len(events.events) != 0 || len(m.QuorumBlockers) != 0 || m.QuorumBlockerCount != 0 {
		t.Fatal("failed wait announced recovery or retained stale blockers", events.events, m)
	}
}
