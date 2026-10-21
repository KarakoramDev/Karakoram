// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/pki"
	"testing"
	"time"
)

func TestUpgradeOrdersNonVotersBeforeVoters(t *testing.T) {
	f := coreSetup(t, 4)
	f.rc.Cluster.Spec.Version = "1.39.12"
	coreConverge(t, f)
	f.rc.Cluster.Spec.Version = "1.40.1"
	if _, e := f.p.Sync(context.Background(), f.rc); e != nil {
		t.Fatal(e)
	}
	s, _ := load(f.rc.Cluster)
	if s.Rolling == nil || len(s.Rolling.Order) != 4 || s.Rolling.Order[0] != s.Members[3].Name {
		t.Fatal(s.Rolling)
	}
	order := append([]string(nil), s.Rolling.Order...)
	lastPosition := 0
	for range 150 {
		if _, e := f.p.Sync(context.Background(), f.rc); e != nil {
			t.Fatal(e)
		}
		s, _ = load(f.rc.Cluster)
		if s.Rolling == nil {
			break
		}
		if s.Rolling.Position > lastPosition+1 {
			t.Fatal("skipped rolling member")
		}
		lastPosition = s.Rolling.Position
		for i, m := range s.Members {
			if m.DesiredVersion == "1.40.1" && m.Name != order[0] && lastPosition == 0 {
				t.Fatalf("voter %d disrupted ahead of non-voter", i)
			}
		}
	}
	s = coreConverge(t, f)
	for _, m := range s.Members {
		if m.Version != "1.40.1" {
			t.Fatal(m.Version)
		}
	}
}
func TestTLSLeafRefreshUsesDurableRoll(t *testing.T) {
	f := coreSetup(t, 1)
	s := coreConverge(t, f)
	m := s.Members[0]
	oldHash := m.ConfigHash
	crt, e := secret(context.Background(), f.rc, secretCA)
	if e != nil {
		t.Fatal(e)
	}
	key, e := secret(context.Background(), f.rc, secretCAKey)
	if e != nil {
		t.Fatal(e)
	}
	ca, e := pki.LoadCA(crt, key)
	if e != nil {
		t.Fatal(e)
	}
	leaf, key, e := ca.Issue(pki.CertRequest{CommonName: m.Name, DNSNames: []string{m.Name, "localhost"}, Server: true, Validity: time.Hour})
	if e != nil {
		t.Fatal(e)
	}
	if e = putSecret(context.Background(), f.rc, "tls/members/"+m.Name+".key", key); e != nil {
		t.Fatal(e)
	}
	if e = putSecret(context.Background(), f.rc, "tls/members/"+m.Name+".crt", leaf); e != nil {
		t.Fatal(e)
	}
	if _, e = f.p.Sync(context.Background(), f.rc); e != nil {
		t.Fatal(e)
	}
	s, _ = load(f.rc.Cluster)
	if s.Rolling == nil {
		t.Fatal("certificate refresh bypassed roll")
	}
	s = coreConverge(t, f)
	if s.Members[0].ConfigHash == oldHash {
		t.Fatal("certificate did not change config identity")
	}
}
func TestCleanupDeletesRuntimesBeforeVolumes(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	done := false
	for range 150 {
		var e error
		done, e = f.p.Cleanup(context.Background(), f.rc, domain.DeleteOptions{})
		if e != nil {
			t.Fatal(e)
		}
		s, _ := load(f.rc.Cluster)
		if s.Pending != nil && s.Pending.Action == "delete-volume" {
			for _, m := range s.Members {
				runtime, e := f.backend.GetService(context.Background(), m.Name)
				if e != nil || runtime != nil {
					t.Fatal("volume delete before all runtime absence", e)
				}
			}
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("cleanup did not finish")
	}
	s, _ := load(f.rc.Cluster)
	if !s.CleanupSecrets {
		t.Fatal("secrets cleanup checkpoint missing")
	}
}
func TestOwnedWorkflowFieldsSurviveSave(t *testing.T) {
	f := coreSetup(t, 1)
	f.rc.Cluster.Internal = json.RawMessage(`{"backupWorkflow":{"receipt":"native"}}`)
	s, e := load(f.rc.Cluster)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.save(f.rc.Cluster); e != nil {
		t.Fatal(e)
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(f.rc.Cluster.Internal, &fields)
	if fields["backupWorkflow"] == nil {
		t.Fatal("independently owned workflow state was erased")
	}
}
