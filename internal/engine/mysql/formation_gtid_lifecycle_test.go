// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestPXCReplacementCancelRetiresAbandonedJoinAdmission(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	before, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	oldMembers := gtidMemberIdentities(before, nil)
	previous := before.Formation.GTIDCertification
	op := &domain.Operation{ID: "cancel-pxc-before-join", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Actor: "api", Payload: json.RawMessage(`{"member":"demom0"}`)}
	st := rc.Store.(*store.Store)
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	op.Phase = domain.PhaseCancelled
	if err := st.UpdateOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	settled := false
	for range 80 {
		p = New(Options{Provider: "pxc"})
		if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if current.Replacement == nil && len(current.data()) == 3 && rc.Cluster.Status.Phase == domain.PhaseReady {
			if !reflect.DeepEqual(current.Formation.GTIDCertification.Members, previous.Members) || current.Formation.GTIDCertification.EmptyMembers != nil {
				t.Fatal("cancelled admission fabricated a certificate or new empty-data authority")
			}
			if !reflect.DeepEqual(gtidMemberIdentities(current, nil), oldMembers) {
				t.Fatal("replacement cancellation changed an admitted data incarnation")
			}
			settled = true
			break
		}
	}
	if !settled {
		t.Fatal("cancelled PXC join left a stale cohort admission blocking healthy original members")
	}
}

func TestPXCBlockedScaleUpAllowsOnlyCohortScaleBack(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	baseline, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	oldMembers := gtidMemberIdentities(baseline, nil)
	oldNames := map[string]bool{}
	for _, m := range baseline.data() {
		oldNames[m.Name] = true
	}
	at := time.Now().UTC()
	rc.Clock = trafficRenewalClock{at: at}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
		if r.URL.Path == "/v1/mysql/status" && !oldNames[member] {
			native := b.native[member]
			native.MemberState = "Joining"
			_ = json.NewEncoder(w).Encode(native)
			return true
		}
		return prior != nil && prior(w, r, member)
	}
	started := false
	for range 100 {
		if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		c := current.Formation.GTIDCertification
		if c != nil && c.JoinOnly && !c.StartedAt.IsZero() {
			started = true
			break
		}
	}
	if !started {
		t.Fatal("scale-up did not exercise a real admitted joining cohort")
	}
	// Record the acquisition phase/count at the same clock before timing out.
	if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal(err)
	}
	b.apply(t, p, rc)
	rc.Clock = trafficRenewalClock{at: at.Add(pxcFormationAdmissionTimeout + time.Minute)}
	_, stalledErr := p.Sync(t.Context(), rc)
	blocked, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	c := blocked.Formation.GTIDCertification
	if stalledErr == nil || c == nil || c.Blocked == "" || !strings.Contains(c.Blocked, "scale back to the admitted member count") {
		t.Fatalf("native stall was not honestly blocked with the existing exit path: %v", stalledErr)
	}
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
		t.Fatal("blocked cohort admitted arbitrary maintenance")
	}
	if err := maintenanceAllowed(t.Context(), rc, blocked); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("blocked cohort admitted ordinary backup/restart/resource changes")
	}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	settled := false
	for range 180 {
		p = New(Options{Provider: "pxc"})
		if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if len(current.data()) == 3 && rc.Cluster.Status.Phase == domain.PhaseReady {
			if !reflect.DeepEqual(gtidMemberIdentities(current, nil), oldMembers) {
				t.Fatal("scale-back erased or replaced an admitted old data incarnation")
			}
			if current.Formation.GTIDCertification.EmptyMembers != nil {
				t.Fatal("scale-back invented new automatic SST erase permission")
			}
			settled = true
			break
		}
	}
	if !settled {
		t.Fatal("blocked 3-to-5 join could not use the existing desired-3 scale-back exit")
	}
}

func TestPXCReplacementMaintenanceWaitIsNotNativeJoinStall(t *testing.T) {
	for _, synced := range []bool{true, false} {
		t.Run(fmt.Sprintf("native-synced=%t", synced), func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			at := time.Now().UTC()
			rc.Clock = trafficRenewalClock{at: at}
			op := &domain.Operation{ID: "replacement-window", Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning, Actor: "api", Payload: json.RawMessage(`{"member":"demom0"}`)}
			if err := rc.Store.(*store.Store).EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			result, err := p.RunOperation(t.Context(), rc, op)
			if err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = result.Checkpoint
			rc.Operation = op
			// The user window closes while the accepted replacement is joining.
			rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: fmt.Sprintf("0 0 1 %d *", int(at.Month())%12+1), Duration: "1h"}}}
			joined := false
			var nextName string
			for range 120 {
				if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				current, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if current.Replacement == nil {
					t.Fatal("replacement retired the old node outside its window")
				}
				nextName = current.Replacement.New
				if next := current.find(nextName); next != nil && next.Stage == "joined" {
					joined = true
					break
				}
			}
			if !joined {
				t.Fatal("accepted join did not progress while waiting for a future disruption window")
			}
			if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
				t.Fatal(err)
			}
			b.apply(t, p, rc)
			if !synced {
				prior := b.action
				b.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
					if member == nextName && r.URL.Path == "/v1/mysql/status" {
						native := b.native[member]
						native.MemberState = "Joining"
						_ = json.NewEncoder(w).Encode(native)
						return true
					}
					return prior != nil && prior(w, r, member)
				}
			}
			rc.Clock = trafficRenewalClock{at: at.Add(pxcFormationAdmissionTimeout + time.Minute)}
			_, err = p.Sync(t.Context(), rc)
			current, loadErr := load(rc.Cluster)
			if loadErr != nil {
				t.Fatal(loadErr)
			}
			if synced {
				if err != nil || current.Formation.GTIDCertification.Blocked != "" || current.Replacement == nil || current.Replacement.Stage != "join" || current.find("demom0") == nil {
					t.Fatalf("healthy Synced maintenance wait was misclassified as a native stall: %v", err)
				}
			} else if err == nil || current.Formation.GTIDCertification.Blocked == "" {
				t.Fatal("genuine non-Synced join stall was hidden by a maintenance window")
			}
		})
	}
}

func TestPXCRetiredJoinAdmissionPreservesExistingIncidentAndOldData(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	previous := s.Formation.GTIDCertification
	stamp := time.Now().UTC()
	incident := gtidIncident{DetectedAt: stamp, SafeThrough: stamp.Add(-time.Minute), Members: []string{s.data()[0].Name}, Audited: true}
	s.GTIDIncidents, s.LastGTIDSafeAt = []gtidIncident{incident}, incident.SafeThrough
	if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	s.Formation.GTIDCertification = &pxcFormationCertification{
		Owner: "removed-join", JoinOnly: true, Epoch: s.Formation.FormationEpoch, Group: s.Formation.GroupName,
		EmptyMembers: map[string]bool{"removed-new-member": true}, FencedMembers: map[string]bool{"removed-new-member": true},
		PreviousMapping: previous, Checkpoint: backupCheckpoint{GTIDCertification: &pxcRestoreCertification{FencesOwned: true}},
	}
	before := gtidMemberIdentities(s, nil)
	if handled, err := reconcilePXCFormationLifecycle(t.Context(), rc, s); err != nil || !handled {
		t.Fatalf("removed cohort was not retired: handled=%t error=%v", handled, err)
	}
	if !reflect.DeepEqual(gtidMemberIdentities(s, nil), before) || !reflect.DeepEqual(s.GTIDIncidents, []gtidIncident{incident}) || !s.LastGTIDSafeAt.Equal(incident.SafeThrough) {
		t.Fatal("retiring a new admission erased old data identity or acknowledged its GTID history")
	}
	if err := loadGTIDJournal(t.Context(), rc, s); err != nil || activeGTIDIncident(s) == nil {
		t.Fatal("admission retirement cleared the sealed safety latch")
	}
	// The retirement capability never narrows proofs for an old-data target.
	exit := &pxcJoinRetirementScope{owner: s.Formation.GTIDCertification, cohort: map[string]bool{"new-member": true}}
	ctx := context.WithValue(t.Context(), pxcJoinRetirementKey{}, exit)
	if !pxcRetirementSurvivor(ctx, s, s.data()[0].Name, "new-member") {
		t.Fatal("new-cohort exit capability authorized old-member retirement")
	}
}

func TestPXCScaleBackRecoversImportedCohortWithoutPendingReceipt(t *testing.T) {
	for _, keepPlan := range []bool{true, false} {
		t.Run(fmt.Sprintf("persisted-plan=%t", keepPlan), func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			before, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			original := gtidMemberIdentities(before, nil)
			rc.Cluster.Generation++
			rc.Cluster.Spec.Components.Engine.Replicas = 5
			var current *state
			var imported *member
			for range 100 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				current, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range current.data() {
					if m.Stage == "planned" && m.ImportBatch != nil {
						imported = m
						break
					}
				}
				if imported != nil {
					break
				}
			}
			if imported == nil || current.Pending != nil || imported.ServiceID != "" || imported.VolumeID != "" {
				t.Fatal("did not reach the import-submit crash boundary")
			}
			token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), imported.Name)
			if err != nil || token == nil {
				t.Fatal("planned import has no ownership credential", err)
			}
			// Infrastructure accepted and completed the original batch, but the
			// response/Pending checkpoint and imported IDs were never persisted.
			if _, err := imported.ImportBatch.Submit(t.Context(), rc, imported.Name, token.Token); err != nil {
				t.Fatal(err)
			}
			if b.services[imported.Name] == nil || b.services[imported.Volume] == nil {
				t.Fatal("lost import did not create real runtime and volume")
			}
			if !keepPlan {
				imported.ImportBatch = nil
			}
			if err := save(rc.Cluster, current); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Generation++
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			for range 180 {
				p = New(Options{Provider: "pxc"})
				if _, err := p.Sync(t.Context(), rc); err != nil && !errors.Is(err, engine.ErrUnavailable) {
					t.Fatal(err)
				}
				b.apply(t, p, rc)
				current, err = load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if len(current.data()) != 3 || rc.Cluster.Status.Phase != domain.PhaseReady {
					continue
				}
				if b.services[imported.Name] != nil || b.services[imported.Volume] != nil {
					t.Fatal("cohort exit orphaned the completed import's runtime or volume")
				}
				if !reflect.DeepEqual(gtidMemberIdentities(current, nil), original) {
					t.Fatal("lost import cleanup changed an admitted data incarnation")
				}
				return
			}
			t.Fatalf("lost import cleanup did not finish: pending=%+v members=%+v", current.Pending, current.data())
		})
	}
}

type pxcJoinRetirementLookupBackend struct {
	infra.Backend
	lookupErr error
}

func (b pxcJoinRetirementLookupBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if b.lookupErr != nil {
		return nil, b.lookupErr
	}
	return b.Backend.GetService(ctx, name)
}

func TestPXCUnimportedJoinRetirementRequiresProvenBackendAbsence(t *testing.T) {
	_, rc, b := setupPXC(t)
	m := &member{Name: "never-imported", Volume: "never-importedv", Stage: "planned"}
	if token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), m.Name); !errors.Is(err, store.ErrNotFound) || token != nil {
		t.Fatal("regression requires the real store's missing-token error", token, err)
	}
	if err := verifyPXCJoinRetirementIdentity(t.Context(), rc, m); err != nil {
		t.Fatal("nil service observations did not prove a never-imported identity absent", err)
	}
	rc.Backend = pxcJoinRetirementLookupBackend{Backend: b, lookupErr: infra.ErrNotFound}
	if err := verifyPXCJoinRetirementIdentity(t.Context(), rc, m); err != nil {
		t.Fatal("explicit backend not-found observations did not prove absence", err)
	}
	unavailable := errors.New("backend lookup unavailable")
	rc.Backend = pxcJoinRetirementLookupBackend{Backend: b, lookupErr: unavailable}
	if err := verifyPXCJoinRetirementIdentity(t.Context(), rc, m); !errors.Is(err, unavailable) {
		t.Fatal("backend lookup failure was swallowed as absence", err)
	}
	rc.Backend = b
	for _, name := range []string{m.Name, m.Volume} {
		b.services[name] = &infra.Service{ID: "foreign-" + name, Hostname: name}
		if err := verifyPXCJoinRetirementIdentity(t.Context(), rc, m); !errors.Is(err, engine.ErrConflict) {
			t.Fatal("missing credentials authorized an existing unverified resource", name, err)
		}
		delete(b.services, name)
	}
}
