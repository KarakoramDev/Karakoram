// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type recordedEvents struct{ events []domain.Event }

func (r *recordedEvents) Record(_ context.Context, e domain.Event) error {
	r.events = append(r.events, e)
	return nil
}

func TestCreationConverges(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		t.Run(string(rune('0'+replicas)), func(t *testing.T) {
			p, rc, f, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = replicas
			a.beforeInit = func(name string) {
				s := state(t, rc)
				if s.InitOwner != name || s.InitOwner != "demom0" {
					t.Errorf("init owner not durably elected: %s", rc.Cluster.Internal)
				}
				if _, err := secret(t.Context(), rc, SecretInitKey); err != nil {
					t.Error("init called before key persisted")
				}
			}
			ready(t, p, rc)
			s := state(t, rc)
			if !s.Initialized || len(s.Members) != replicas {
				t.Fatal("incomplete formation")
			}
			for _, m := range s.Members {
				if !m.Healthy || !m.Voter || m.Sealed {
					t.Errorf("member not a healthy unsealed voter: %+v", m)
				}
			}
			if a.member("demom0").initCalls != 1 {
				t.Fatal("owner initialized more than once")
			}
			for name, b := range a.members {
				if name != "demom0" && b.initRequests != 0 {
					t.Fatal("non-owner initialized")
				}
			}
			before := f.mutations
			result, err := p.Sync(t.Context(), rc)
			if err != nil || result.RequeueAfter != 30*time.Second {
				t.Fatalf("converged sync: %+v %v", result, err)
			}
			if f.mutations != before {
				t.Fatal("converged sync mutated infrastructure")
			}
			p = New(p.options)
			a.p = p
			ready(t, p, rc)
			if a.member("demom0").initCalls != 1 {
				t.Fatal("provider reconstruction reinitialized owner")
			}
		})
	}
}
func TestLostInitializationResponseRecoversReceipt(t *testing.T) {
	p, rc, _, a := setup(t)
	a.loseResponse = true
	seenLost := false
	for range 100 {
		_, err := p.Sync(t.Context(), rc)
		if err != nil {
			if !strings.Contains(err.Error(), "initialization receipt") {
				t.Fatal(err)
			}
			seenLost = true
			break
		}
	}
	if !seenLost {
		t.Fatal("lost response not exercised")
	}
	p = New(p.options)
	a.p = p
	ready(t, p, rc)
	b := a.member("demom0")
	if b.initCalls != 1 || b.initRequests != 2 {
		t.Fatalf("receipt replay counts: init=%d requests=%d", b.initCalls, b.initRequests)
	}
}

type failMaterialStore struct {
	engine.SecretStore
	failed bool
}

func (s *failMaterialStore) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, b []byte) error {
	if key == SecretUnseal && !s.failed {
		s.failed = true
		return errors.New("simulated interrupted material commit")
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, b)
}
func TestReceiptRecoversInterruptedMaterialCommit(t *testing.T) {
	p, rc, _, a := setup(t)
	f := &failMaterialStore{SecretStore: rc.Secrets}
	rc.Secrets = f
	for range 100 {
		_, err := p.Sync(t.Context(), rc)
		if err != nil {
			if !f.failed {
				t.Fatal(err)
			}
			break
		}
	}
	if !f.failed {
		t.Fatal("material write not interrupted")
	}
	p = New(p.options)
	a.p = p
	ready(t, p, rc)
	if a.member("demom0").initCalls != 1 || a.member("demom0").initRequests != 2 {
		t.Fatal("did not recover original receipt")
	}
}
func TestInitializationOutcomeLostFailsClosed(t *testing.T) {
	p, rc, _, a := setup(t)
	a.initWithoutReceipt = true
	lost := false
	for range 100 {
		r, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		if r.ConvergenceError != nil {
			lost = true
			break
		}
	}
	if !lost || state(t, rc).InitializationOutcomeLost == "" {
		t.Fatal("missing terminal initialization loss")
	}
	before := a.member("demom0").initRequests
	p = New(p.options)
	a.p = p
	for range 5 {
		r, err := p.Sync(t.Context(), rc)
		if err != nil || r.ConvergenceError == nil {
			t.Fatal("lost initialization did not stay terminal")
		}
	}
	if a.member("demom0").initRequests != before {
		t.Fatal("terminal failure retried initialization")
	}
}
func TestRestartedMemberAutomaticallyUnsealed(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	b := a.member("demom1")
	before := b.unseals
	b.sealed = true
	b.healthy = false
	rc.Cluster.Generation++
	ready(t, p, rc)
	if b.sealed || b.unseals-before != 3 {
		t.Fatalf("restart not unsealed with threshold shares: %+v", b)
	}
}
func TestInitializationSharesStayControlPlaneOnly(t *testing.T) {
	p, rc, _, a := setup(t)
	events := &recordedEvents{}
	rc.Events = events
	ready(t, p, rc)
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	statusJSON, _ := json.Marshal(status)
	eventJSON, _ := json.Marshal(events.events)
	for _, m := range state(t, rc).Members {
		cfg, err := p.MemberConfig(t.Context(), rc, m.Name)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(cfg)
		for _, share := range a.shares {
			decoded, _ := base64.StdEncoding.DecodeString(share)
			for _, surface := range [][]byte{rc.Cluster.Internal, raw, statusJSON, eventJSON} {
				if strings.Contains(string(surface), share) || strings.Contains(string(surface), string(decoded)) {
					t.Fatal("share leaked outside encrypted secret store")
				}
			}
		}
		if strings.Contains(string(raw), a.root) || strings.Contains(string(raw), a.management) {
			t.Fatal("token leaked to member")
		}
	}
}
func TestManagementTokenRegeneration(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	a.tokenValid = false
	before := a.management
	rc.Cluster.Generation++
	ready(t, p, rc)
	if a.generated != 1 || a.revoked != 1 || a.management == before {
		t.Fatalf("management recovery incomplete: generated=%d revoked=%d", a.generated, a.revoked)
	}
}
func TestScaleOneToThreeJoinsSerialHealthyVoters(t *testing.T) {
	p, rc, _, a := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	ready(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	rc.Cluster.Generation++
	a.holdVoter = "demom1"
	syncUntil(t, p, rc, func(s *internalState) bool { return s.Unsealing == "demom1" && !a.member("demom1").sealed })
	for range 4 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
	}
	if a.member("demom2").unseals != 0 {
		t.Fatal("second follower unsealed before preceding voter healthy")
	}
	a.member("demom1").healthy = true
	a.holdVoter = ""
	ready(t, p, rc)
	for name, b := range a.members {
		if name != "demom0" && b.initCalls != 0 {
			t.Fatal("follower initialized")
		}
	}
}
func TestCleanupKeepsUnsealMaterialWithVolumes(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	for range 100 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			if _, err = readUnseal(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			for _, m := range state(t, rc).Members {
				if f.services[m.Name] != nil || f.services[m.Volume] == nil {
					t.Fatal("suspension did not preserve only volume")
				}
			}
			return
		}
	}
	t.Fatal("cleanup did not finish")
}

func TestSuspendedClusterResumesWithoutInitialization(t *testing.T) {
	p, rc, _, a := setup(t)
	ready(t, p, rc)
	// Suspend with durable admitted intent but no remaining member receipt.
	s := state(t, rc)
	s.RollingKind, s.RollingGeneration = "resources", rc.Cluster.Generation
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	stopped := false
	for range 100 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			stopped = true
			break
		}
	}
	if !stopped {
		t.Fatal("suspension did not complete")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseSuspended {
		t.Fatalf("retained roll hid suspension: %+v %v", status, err)
	}
	rc.Cluster.Generation++
	ready(t, p, rc)
	if s := state(t, rc); s.RollingKind != "" || s.RollingGeneration != 0 {
		t.Fatalf("resumed convergence retained rolling intent: %q/%d", s.RollingKind, s.RollingGeneration)
	}
	if a.member("demom0").initCalls != 1 {
		t.Fatal("resumption reinitialized data")
	}
	for _, m := range state(t, rc).Members {
		if m.Sealed || !m.Voter || !m.Healthy {
			t.Fatalf("resumed member is not healthy: %+v", m)
		}
	}
}

func TestFullDeletionRemovesInitializationSecrets(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	for range 100 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if !done {
			continue
		}
		if len(f.services) != 0 {
			t.Fatal("owned infrastructure remains")
		}
		for _, key := range []string{SecretUnseal, SecretManagementToken, SecretInitKey, "tls/ca.crt", "tls/ca.key"} {
			if _, err = secret(t.Context(), rc, key); !missing(err) {
				t.Fatalf("secret %s remains: %v", key, err)
			}
		}
		return
	}
	t.Fatal("deletion did not complete")
}
