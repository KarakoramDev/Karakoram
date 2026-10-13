// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mongoadmin

import (
	"context"
	"testing"
	"time"
)

func TestDialRejectsMissingHostsAndInvalidCA(t *testing.T) {
	if _, e := Dial(context.Background(), DialOptions{}); e == nil {
		t.Fatal("empty hosts accepted")
	}
	if _, e := Dial(context.Background(), DialOptions{Hosts: []string{"127.0.0.1:1"}, TLS: true, CAPEM: []byte("invalid")}); e == nil {
		t.Fatal("invalid CA accepted")
	}
}
func TestReplicationLag(t *testing.T) {
	base := time.Date(2024, 1, 5, 12, 0, 0, 0, time.UTC)
	primary := Member{Name: "primary:27017", OptimeDate: base}
	secondary := Member{Name: "secondary:27017", OptimeDate: base.Add(-5 * time.Second)}
	s := Status{Members: []Member{primary, secondary}}
	if got := s.Lag(secondary); got != 5 {
		t.Fatal(got)
	}
	if got := s.Lag(primary); got != 0 {
		t.Fatal(got)
	}
	if got := s.Lag(Member{}); got < 1e8 {
		t.Fatal("unknown optime mistaken for caught up", got)
	}
}

func TestPromotionResumeAcceptsHealthyVotersIncludingPrimary(t *testing.T) {
	for _, test := range []struct {
		state              string
		votes              int
		health             float64
		changed, wantError bool
	}{
		{"PRIMARY", 1, 1, false, false},
		{"SECONDARY", 1, 1, false, false},
		{"SECONDARY", 0, 1, true, false},
		{"PRIMARY", 0, 1, false, true},
		{"RECOVERING", 1, 1, false, true},
		{"PRIMARY", 1, 0, false, true},
	} {
		config := Config{Members: []ConfigMember{{Host: "member:27017", Votes: test.votes}}}
		status := Status{Members: []Member{{Name: "member:27017", State: test.state, Health: test.health, OptimeDate: time.Now()}}}
		changed, err := promoteConfig(&config, status, "member:27017")
		if changed != test.changed || (err != nil) != test.wantError {
			t.Fatalf("%s votes=%d health=%v: changed=%v err=%v", test.state, test.votes, test.health, changed, err)
		}
	}
}
