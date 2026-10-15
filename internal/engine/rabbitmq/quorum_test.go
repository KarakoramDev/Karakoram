// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

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
		if !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), `"orders" in vhost "/"`) || !strings.Contains(result.Message, "orders") {
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
	if err == nil || !strings.Contains(err.Error(), "orders") || strings.Contains(err.Error(), "HTTP 503") {
		t.Fatal(err)
	}
	if countCall(a.log.snapshot(), "agent:demom0:quorum-critical") != 1 || len(m.QuorumBlockers) != 1 {
		t.Fatal(a.log.snapshot(), m)
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
		if err := p.checkQuorum(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
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
