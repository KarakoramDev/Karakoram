// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

func TestRestartQuorumRejectsTwoUnavailableVoters(t *testing.T) {
	// A persisted API snapshot may still say all three are healthy. Only the
	// current direct probes enter this assertion, so two definitively down
	// endpoints fail even when the third endpoint is unknown.
	samples := []restartMemberSample{
		{Name: "m0", State: restartUnknown},
		{Name: "m1", State: restartDown, Failure: "connection refused"},
		{Name: "m2", State: restartDown, Failure: "not PRIMARY/SECONDARY"},
	}
	if conclusive, err := restartQuorum(samples); !conclusive || err == nil {
		t.Fatal("two definitively down voting endpoints passed the quorum assertion")
	}
	samples[0].State = restartLive
	samples[1].State = restartLive
	if conclusive, err := restartQuorum(samples); !conclusive || err != nil {
		t.Fatalf("one unavailable rolling member should retain voting majority: %v", err)
	}
}

func TestRestartQuorumTreatsRoutingTimeoutsAsUnknown(t *testing.T) {
	for _, tc := range []struct {
		name       string
		states     []restartProbeState
		conclusive bool
	}{
		{"unavailable project routing", []restartProbeState{restartUnknown, restartUnknown, restartUnknown}, false},
		{"one live two unknown", []restartProbeState{restartLive, restartUnknown, restartUnknown}, false},
		{"one down one live one unknown", []restartProbeState{restartDown, restartLive, restartUnknown}, false},
		{"live majority one unknown", []restartProbeState{restartLive, restartLive, restartUnknown}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			samples := make([]restartMemberSample, len(tc.states))
			for i, state := range tc.states {
				samples[i].State = state
			}
			conclusive, err := restartQuorum(samples)
			if err != nil || conclusive != tc.conclusive {
				t.Fatalf("conclusive=%t err=%v want conclusive=%t", conclusive, err, tc.conclusive)
			}
		})
	}
}

func TestRestartWindowRequiresConclusiveRounds(t *testing.T) {
	for _, summary := range []restartProbeSummary{{}, {InconclusiveRounds: 100}} {
		if err := summary.requireConclusive(); err == nil {
			t.Fatalf("unobserved restart window passed: %+v", summary)
		}
	}
	if err := (restartProbeSummary{ConclusiveRounds: 1, InconclusiveRounds: 100}).requireConclusive(); err != nil {
		t.Fatal(err)
	}
}

func TestRestartProbeErrorClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want restartProbeState
	}{
		{"connection refused", &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, restartDown},
		{"embedded refused", topology.ServerSelectionError{Wrapped: errors.New("connection refused")}, restartDown},
		{"dial timeout", &net.OpError{Op: "dial", Err: context.DeadlineExceeded}, restartUnknown},
		{"io timeout", &net.OpError{Op: "read", Err: context.DeadlineExceeded}, restartUnknown},
		{"server selection without reply", topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}, restartUnknown},
		{"wrapped selection", fmt.Errorf("probe: %w", topology.ServerSelectionError{Wrapped: context.DeadlineExceeded}), restartUnknown},
		{"network without reply", mongo.CommandError{Labels: []string{"NetworkError"}}, restartUnknown},
		{"unhealthy reply", mongo.CommandError{Code: 91}, restartDown},
		{"authorization reply", mongo.CommandError{Code: 13, Labels: []string{"NetworkError"}}, restartDown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := restartErrorState(tc.err); got != tc.want {
				t.Fatalf("state=%s want=%s err=%v", got, tc.want, tc.err)
			}
		})
	}
}

func TestRestartUptimeResetRejectsUnrestartedMember(t *testing.T) {
	base := time.Date(2000, 1, 1, 12, 0, 0, 0, time.UTC)
	before := restartMemberSample{State: restartLive, Started: base.Add(100 * time.Second), Finished: base.Add(100*time.Second + 10*time.Millisecond), UptimeMillis: 100000}
	cases := []struct {
		name  string
		after restartMemberSample
		reset bool
	}{
		{
			name:  "no-op operation only increases uptime",
			after: restartMemberSample{State: restartLive, Started: base.Add(200 * time.Second), Finished: base.Add(200*time.Second + 10*time.Millisecond), UptimeMillis: 200000},
		},
		{
			name:  "command timing uncertainty is not a reset",
			after: restartMemberSample{State: restartLive, Started: base.Add(200 * time.Second), Finished: base.Add(200*time.Second + 10*time.Millisecond), UptimeMillis: 199995},
		},
		{
			name:  "actual uptime decrease",
			after: restartMemberSample{State: restartLive, Started: base.Add(110 * time.Second), Finished: base.Add(110*time.Second + 10*time.Millisecond), UptimeMillis: 1000},
			reset: true,
		},
		{
			name:  "slow real restart has greater final uptime than baseline",
			after: restartMemberSample{State: restartLive, Started: base.Add(300 * time.Second), Finished: base.Add(300*time.Second + 10*time.Millisecond), UptimeMillis: 150000},
			reset: true,
		},
		{
			name:  "unavailable endpoint cannot prove reset",
			after: restartMemberSample{Started: base.Add(110 * time.Second), Finished: base.Add(110*time.Second + 10*time.Millisecond), UptimeMillis: 1000},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := restartUptimeReset(before, tc.after); got != tc.reset {
				t.Fatalf("reset=%t want=%t before=%+v after=%+v", got, tc.reset, before, tc.after)
			}
		})
	}
}
