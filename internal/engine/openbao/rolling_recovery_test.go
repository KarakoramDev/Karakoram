// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"errors"
	"slices"
	"testing"
)

func TestRollingRetryReobservesNativeBoundary(t *testing.T) {
	for _, action := range []string{"deploy", "resources"} {
		for _, failure := range []string{"sealed", "unreachable", "leadership"} {
			t.Run(action+"/"+failure, func(t *testing.T) {
				p, rc, f, a := setup(t)
				ready(t, p, rc)
				s := state(t, rc)
				m := s.find("demom1")
				step := "deploy"
				if action == "resources" {
					step = "runtime-resources"
				}
				m.Change = &memberChange{Kind: action, Step: step, Version: rc.Cluster.Spec.Version, Resources: m.Resources, Disk: m.Disk}
				f.failures[action+":"+m.Name] = errors.New("temporary mutation failure")
				if err := p.changeStep(t.Context(), rc, s, m, true); err == nil || s.Retry == nil {
					t.Fatalf("missing failed mutation checkpoint: %v", err)
				}
				delete(f.failures, action+":"+m.Name)
				rc.Clock.(*testClock).at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
				switch failure {
				case "sealed":
					a.member("demom2").sealed = true
					a.member("demom2").healthy = false
				case "unreachable":
					a.member("demom2").unreachable = true
				case "leadership":
					a.member("demom0").active = false
					a.member(m.Name).active = true
				}
				mutations := f.mutations
				f.log.clear()
				// Recreate the controller: persisted healthy/standby observations
				// must not authorize the retry after native state has changed.
				p = New(p.options)
				a.p = p
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if f.mutations != mutations || state(t, rc).Retry == nil {
					t.Fatal("retry bypassed fresh native admission")
				}
				if failure == "leadership" && !slices.Contains(f.log.snapshot(), "step-down:"+m.Name) {
					t.Fatal("re-elected retry target was not stepped down")
				}
				if failure == "unreachable" {
					a.member("demom2").unreachable = false
				}
				for range 20 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					if f.mutations > mutations {
						if failure == "sealed" && !slices.Contains(f.log.snapshot(), "unsealed:demom2") {
							t.Fatal("mutation retried before sealed voter recovered")
						}
						if action == "deploy" && state(t, rc).find(m.Name).Change.Step != "wait" {
							t.Fatal("successful deployment retry did not advance to wait")
						}
						return
					}
				}
				t.Fatal("safe retry did not resume after native recovery")
			})
		}
	}
}

func TestRollingApplyRecoversEstablishedSealedPeer(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	m.Change = &memberChange{Kind: "restart", Step: "apply", Version: rc.Cluster.Spec.Version}
	generation := m.ProcessGeneration
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	a.member("demom2").sealed = true
	a.member("demom2").healthy = false
	a.holdVoter = "demom2"
	f.log.clear()
	for range 6 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).find(m.Name).ProcessGeneration != generation {
			t.Fatal("rolling apply advanced before recovered peer became healthy")
		}
	}
	if a.member("demom2").sealed || state(t, rc).Unsealing != "demom2" {
		t.Fatal("blocked rolling apply did not serially recover established voter")
	}
	// Losing observation must retain the recovery fence, not clear it or
	// restart a different member based on the previous unsealed observation.
	a.member("demom2").unreachable = true
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if state(t, rc).Unsealing != "demom2" || state(t, rc).find(m.Name).ProcessGeneration != generation {
		t.Fatal("missing observation released recovery fence")
	}
	a.member("demom2").unreachable = false
	a.member("demom2").healthy = true
	a.holdVoter = ""
	for range 20 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if state(t, rc).find(m.Name).ProcessGeneration > generation {
			return
		}
	}
	t.Fatal("rolling apply did not resume after peer recovery")
}

func TestRollingRecoveryDoesNotStealFormationOrWaitFence(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	m.Change = &memberChange{Kind: "restart", Step: "wait"}
	s.Unsealing = m.Name
	a.member(m.Name).sealed = true
	p.observeMembers(t.Context(), rc, s)
	before := a.member(m.Name).unseals
	if busy, err := p.recoverEstablishedPeers(t.Context(), rc, s, m); busy || err != nil || a.member(m.Name).unseals != before {
		t.Fatalf("ordinary recovery stole rolling wait: busy=%v err=%v", busy, err)
	}
	// A newly added member is not eligible for established-voter recovery.
	newMember := s.find("demom2")
	newMember.Voter = false
	newMember.Sealed = true
	s.Unsealing = newMember.Name
	before = a.member(newMember.Name).unseals
	if busy, err := p.recoverEstablishedPeers(t.Context(), rc, s, m); !busy || err != nil || a.member(newMember.Name).unseals != before || s.Unsealing != newMember.Name {
		t.Fatalf("recovery stole formation fence: busy=%v err=%v", busy, err)
	}
}

func TestRollingFailedDeploymentReceiptRecoversBeforeRetry(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	m.Change = &memberChange{Kind: "deploy", Step: "deploy", Version: rc.Cluster.Spec.Version}
	if err := p.changeStep(t.Context(), rc, s, m, true); err != nil {
		t.Fatal(err)
	}
	if m.Change.Step != "wait" || m.ResumeDeploy == nil {
		t.Fatal("deployment did not enter wait")
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.failed = true
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("scripted failed deployment receipt was not observed")
	}
	f.failed = false
	s = state(t, rc)
	if s.Retry == nil {
		t.Fatal("failed deployment lost retry checkpoint")
	}
	rc.Clock.(*testClock).at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
	mutations := f.mutations
	f.log.clear()
	for range 20 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if f.mutations > mutations {
			if !slices.Contains(f.log.snapshot(), "unsealed:"+m.Name) {
				t.Fatal("failed sealed target was retried before recovery")
			}
			return
		}
	}
	t.Fatal("rolling wait checkpoint stranded failed sealed deployment retry")
}
