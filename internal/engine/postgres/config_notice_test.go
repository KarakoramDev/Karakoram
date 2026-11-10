// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

type configNoticeNoReads struct {
	infra.Backend
	snapshot *infra.TargetSnapshot
}

func (b configNoticeNoReads) PlatformSnapshot() *infra.TargetSnapshot { return b.snapshot }

func (configNoticeNoReads) GetService(context.Context, string) (*infra.Service, error) {
	return nil, fmt.Errorf("notification unexpectedly fetched a live peer")
}
func TestConfigNoticeContentIgnoresObservationJournals(t *testing.T) {
	for _, name := range []string{"demop0", "demopx"} {
		t.Run(name, func(t *testing.T) {
			p, rc, _ := setup(t, "ha")
			converge(t, p, rc)
			rc.Backend = configNoticeNoReads{Backend: rc.Backend, snapshot: infra.PlatformSnapshot(t.Name() + "|journal")}
			before, err := p.MemberConfigNoticeContent(t.Context(), rc, name)
			if err != nil || before == "" {
				t.Fatal(before, err)
			}
			for tick := range 20 {
				var internal map[string]json.RawMessage
				if err := json.Unmarshal(rc.Cluster.Internal, &internal); err != nil {
					t.Fatal(err)
				}
				internal["observationJournal"] = json.RawMessage(fmt.Sprint(tick))
				rc.Cluster.Internal, _ = json.Marshal(internal)
				rc.Cluster.Status.Phase = domain.PhaseRunning
				if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/gtid/history", []byte(fmt.Sprint(tick))); err != nil {
					t.Fatal(err)
				}
				got, err := p.MemberConfigNoticeContent(t.Context(), rc, name)
				if err != nil || got != before {
					t.Fatal("observation-only changes churned rendered config content", tick, err)
				}
			}
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.find(name).ConfigVersion++
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			changed, err := p.MemberConfigNoticeContent(t.Context(), rc, name)
			if err != nil || changed == before {
				t.Fatal("real config change did not change notice content", err)
			}
		})
	}
}

type configNoticeLivePeers struct {
	infra.Backend
	snapshot *infra.TargetSnapshot
	reads    int
}

func (b *configNoticeLivePeers) PlatformSnapshot() *infra.TargetSnapshot { return b.snapshot }
func (b *configNoticeLivePeers) GetService(ctx context.Context, name string) (*infra.Service, error) {
	b.reads++
	return b.Backend.GetService(ctx, name)
}

func TestConfigNoticeUsesListingAndPullForPlatformOnlyPeers(t *testing.T) {
	for _, mode := range []string{"recovery-mutation", "recovery-external", "removed-external", "replaced-external"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, backend := setup(t, "ha")
			s := converge(t, p, rc)
			peer := s.data()[0].Name
			now := time.Now()
			start := now
			defer infra.SetSnapshotClockForTest(func() time.Time { return now })()
			fingerprint := "before"
			snapshot := infra.PlatformSnapshot(t.Name() + "|live-peer")
			snapshot.SetListing(func(context.Context) (string, error) { return fingerprint, nil })
			if !snapshot.EnsureListing(t.Context()) {
				t.Fatal("listing baseline unavailable")
			}
			dependencies := &configNoticeLivePeers{Backend: backend, snapshot: snapshot}
			rc.Backend = dependencies
			rc.Cluster.Status.Phase = domain.PhaseRunning
			internal := append([]byte(nil), rc.Cluster.Internal...)
			recovery := mode == "recovery-mutation" || mode == "recovery-external"
			if recovery {
				backend.services[peer].Status = infra.StatusStopped
			}
			hasPeer := func(cfg agentapi.MemberConfig) bool {
				t.Helper()
				var spec pgspec.Spec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				for _, entry := range spec.Peers {
					if entry.Name == peer {
						if entry.Host != peer {
							t.Fatal("routing retained an instance address instead of logical service DNS")
						}
						return true
					}
				}
				return false
			}
			before, err := p.MemberConfig(t.Context(), rc, "demopx")
			if err != nil || hasPeer(before) == recovery {
				t.Fatal("initial live-peer gate was not exercised", err)
			}
			oldInputs := engine.ConfigPullRevision(rc, "demopx", nil, [32]byte{})
			payload, err := p.MemberConfigNoticeContent(t.Context(), rc, "demopx")
			if err != nil || payload == "" {
				t.Fatal(err)
			}
			oldChange, oldContent := engine.ConfigNoticeRevisions(rc, payload, false)
			if mode == "recovery-mutation" {
				finish := snapshot.BeginMutation(t.Context())
				backend.services[peer].Status = infra.StatusRunning
				finish()
			} else if recovery {
				backend.services[peer].Status = infra.StatusRunning
			} else if mode == "removed-external" {
				delete(backend.services, peer)
			} else {
				replacement := *backend.services[peer]
				replacement.ID = "replacement-runtime"
				backend.services[peer] = &replacement
			}
			reads := dependencies.reads
			payload, err = p.MemberConfigNoticeContent(t.Context(), rc, "demopx")
			change, content := engine.ConfigNoticeRevisions(rc, payload, false)
			if err != nil || dependencies.reads != reads || change != oldChange || content != oldContent {
				t.Fatal("platform-only mutation fanned out notices or performed live reads", err)
			}
			if !bytes.Equal(internal, rc.Cluster.Internal) {
				t.Fatal("fixture changed persisted membership")
			}
			periodicInputs := engine.ConfigPullRevision(rc, "demopx", nil, [32]byte{})
			if (periodicInputs != oldInputs) != (mode == "recovery-mutation") {
				t.Fatal("certificate lost independent mutation evidence")
			}
			// Worst fallback: no heartbeat is usable. Listing detects at
			// 60s, then the following ordinary pull resolves at 120s.
			now = start.Add(infra.SnapshotListingInterval)
			fingerprint = "after"
			if !snapshot.EnsureListing(t.Context()) {
				t.Fatal("listing drift was not detected")
			}
			payload, err = p.MemberConfigNoticeContent(t.Context(), rc, "demopx")
			change, content = engine.ConfigNoticeRevisions(rc, payload, false)
			if err != nil || change == oldChange || content == oldContent {
				t.Fatal("listing drift did not notify a healthy heartbeat path", err)
			}
			now = now.Add(time.Minute)
			if !snapshot.EnsureListing(t.Context()) {
				t.Fatal("scheduled pull could not renew its listing")
			}
			currentInputs := engine.ConfigPullRevision(rc, "demopx", nil, [32]byte{})
			after, unchanged, err := engine.PullMemberConfig(oldInputs, currentInputs, func() (agentapi.MemberConfig, error) {
				return p.MemberConfig(t.Context(), rc, "demopx")
			})
			wantPeer := mode != "removed-external"
			if err != nil || unchanged || hasPeer(after) != wantPeer || now.Sub(start) > 2*time.Minute {
				t.Fatal("listing-plus-pull fallback did not resolve current routing", mode, unchanged, err)
			}
		})
	}
}

func TestConfigNoticeOwnGenerationPeersAndCredentialsStayImmediate(t *testing.T) {
	p, rc, _ := setup(t, "ha")
	s := converge(t, p, rc)
	snapshot := infra.PlatformSnapshot(t.Name() + "|own")
	rc.Backend = configNoticeNoReads{Backend: rc.Backend, snapshot: snapshot}
	last := ""
	check := func() {
		t.Helper()
		payload, err := p.MemberConfigNoticeContent(t.Context(), rc, "demopx")
		change, content := engine.ConfigNoticeRevisions(rc, payload, false)
		if err != nil || content == "" || content == last || change == "" {
			t.Fatal("own change was not immediately noticeable while nonsteady", err)
		}
		last = content
	}
	check()
	rc.Cluster.Generation++
	check()
	s.data()[0].Removing = true
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	check()
	if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "users/postgres", []byte("changed")); err != nil {
		t.Fatal(err)
	}
	check()
}
