// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestSpecAuditUsesTransactionalPreviousSpec(t *testing.T) {
	s, _ := testStore(t)
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Version: "7"}}
	if err := s.CreateCluster(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first := &SpecUpdateAudit{Build: func(before, after domain.ClusterSpec, generation int64, _ []SpecSecretChange) domain.Event {
		if before.Version != "7" || after.Version != "8" || generation != 2 {
			t.Errorf("first snapshot: %s -> %s generation %d", before.Version, after.Version, generation)
		}
		close(entered)
		<-release
		return domain.Event{Cluster: c.Name, Type: "Normal", Message: "first", Details: json.RawMessage(`{"changes":[{"path":"version","before":"7","after":"8"}]}`)}
	}}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- s.UpdateClusterSpec(WithSpecUpdateAudit(t.Context(), first), c.Ref(), domain.ClusterSpec{Version: "8"})
	}()
	<-entered
	second := &SpecUpdateAudit{Build: func(before, after domain.ClusterSpec, generation int64, _ []SpecSecretChange) domain.Event {
		if before.Version != "8" || after.Version != "9" || generation != 3 {
			t.Errorf("second snapshot: %s -> %s generation %d", before.Version, after.Version, generation)
		}
		return domain.Event{Cluster: c.Name, Type: "Normal", Message: "second", Details: json.RawMessage(`{"changes":[{"path":"version","before":"8","after":"9"}]}`)}
	}}
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- s.UpdateClusterSpec(WithSpecUpdateAudit(t.Context(), second), c.Ref(), domain.ClusterSpec{Version: "9"})
	}()
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	if first.Event == nil || second.Event == nil || first.Event.ID >= second.Event.ID {
		t.Fatal("events not committed in mutation order")
	}
	events, err := s.ListEvents(t.Context(), EventFilter{Cluster: c.Name})
	if err != nil || len(events) != 2 || string(events[1].Details) != string(second.Event.Details) {
		t.Fatal("details did not persist", events, err)
	}
	var details, payload []byte
	if err = s.reader.QueryRowContext(t.Context(), "SELECT data,details FROM events WHERE id=?", first.Event.ID).Scan(&payload, &details); err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err = json.Unmarshal(payload, &object); err != nil {
		t.Fatal(err)
	}
	if _, ok := object["details"]; ok {
		t.Fatal("details duplicated in legacy payload")
	}
	if string(details) != string(first.Event.Details) {
		t.Fatal("details column differs")
	}
}

func TestSpecAuditFailureRollsBackSpecAndSecrets(t *testing.T) {
	for _, withSecrets := range []bool{false, true} {
		t.Run(map[bool]string{false: "spec", true: "secrets"}[withSecrets], func(t *testing.T) {
			s, _ := testStore(t)
			c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Version: "7"}}
			if err := s.CreateCluster(t.Context(), &c); err != nil {
				t.Fatal(err)
			}
			if _, err := s.writer.ExecContext(t.Context(), `CREATE TRIGGER reject_event BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'test audit failure'); END`); err != nil {
				t.Fatal(err)
			}
			audit := &SpecUpdateAudit{Build: func(before, after domain.ClusterSpec, generation int64, secrets []SpecSecretChange) domain.Event {
				return domain.Event{Cluster: c.Name, Type: "Normal", Message: "update"}
			}}
			ctx := WithSpecUpdateAudit(t.Context(), audit)
			var values map[string][]byte
			if withSecrets {
				values = map[string][]byte{"tls/user.key": []byte("confidential-input")}
			}
			if err := s.UpdateClusterSpecWithSecrets(ctx, c.Ref(), domain.ClusterSpec{Version: "8"}, values); err == nil {
				t.Fatal("audit failure did not abort update")
			}
			got, err := s.GetCluster(t.Context(), c.Ref())
			if err != nil || got.Spec.Version != "7" || got.Generation != 1 || audit.Event != nil {
				t.Fatal("spec escaped failed transaction", got, err)
			}
			var count int
			if err = s.reader.QueryRowContext(t.Context(), "SELECT count(*) FROM records WHERE kind='secret'").Scan(&count); err != nil || count != 0 {
				t.Fatal("secret escaped failed transaction", count, err)
			}
		})
	}
}

func TestEventDetailsNullableAndPersistent(t *testing.T) {
	s, path := testStore(t)
	for _, details := range []json.RawMessage{nil, json.RawMessage(`{"changes":[{"path":"version","after":"8"}]}`)} {
		e := domain.Event{Type: "Normal", Message: "event", Details: details}
		if err := s.AppendEvent(t.Context(), &e); err != nil {
			t.Fatal(err)
		}
	}
	s2, err := Open(path, s.sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	events, err := s2.ListEvents(t.Context(), EventFilter{})
	if err != nil || len(events) != 2 || events[0].Details != nil || len(events[1].Details) == 0 {
		t.Fatal("nullable details not preserved", events, err)
	}
}
