// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/agentpush"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	provider "github.com/KarakoramDev/Karakoram/internal/infra/dokploy"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/server"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// Isolate target-evidence fan-out with otherwise eligible, unchanged PG baselines.
// Real-manager trigger/steady transitions are tested by ProductionConfigPullBudget.
type sharedTargetConfigBaseline struct{}

func (sharedTargetConfigBaseline) Trigger(domain.ClusterRef) {}
func (sharedTargetConfigBaseline) SteadyConfigBaseline(_ context.Context, c *domain.Cluster) bool {
	return c.Status.Phase == domain.PhaseReady && c.Status.ObservedGeneration == c.Generation
}
func (sharedTargetConfigBaseline) ConfigInvalidationGeneration(domain.ClusterRef) uint64 { return 0 }

func TestPostgresSharedTargetConfigMutationBudget(t *testing.T) {
	for _, drifts := range []int{0, 2, -2} {
		t.Run(fmt.Sprintf("listing-drifts-%d", drifts), func(t *testing.T) {
			p := postgres.New(postgres.Options{})
			rc, members, advance, mutate, listingDrift, gets := provider.ExportSharedTargetConfigFixture(t, p, drifts < 0)
			ownDrift := drifts < 0
			if ownDrift {
				drifts = -drifts
			}
			st := rc.Store.(*store.Store)
			sealer, err := secrets.New(strings.Repeat("x", 32))
			if err != nil {
				t.Fatal(err)
			}
			authentication, err := auth.New(auth.Options{Store: st, Sealer: sealer, PublicURL: "http://localhost"})
			if err != nil {
				t.Fatal(err)
			}
			policy, err := rbac.New(st)
			if err != nil {
				t.Fatal(err)
			}
			registry, err := engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			cp, err := server.New(server.Options{Store: st, Auth: authentication, RBAC: policy, Registry: registry, Context: *rc, Namespaces: engine.StaticNamespaces{DefaultBackend: rc.Backend}, Trigger: sharedTargetConfigBaseline{}, AgentPush: agentpush.NewStore(nil), Hub: events.NewHub(128), Logger: logger})
			if err != nil {
				t.Fatal(err)
			}
			defer cp.Close()
			handler := cp.InternalHandler()
			snapshot := rc.Backend.(infra.PlatformSnapshotter).PlatformSnapshot()
			if !snapshot.EnsureListing(t.Context()) {
				t.Fatal("initial listing unavailable")
			}
			startDrift := snapshot.DriftGeneration()
			pull := func(member, revision string) (agentapi.MemberConfig, int, int) {
				t.Helper()
				path := "/internal/agent/v1/config"
				if revision != "" {
					path += "?revision=" + url.QueryEscape(revision)
				}
				request := httptest.NewRequest("GET", path, nil)
				request.Header.Set("Authorization", "Bearer "+member+"-token")
				before := gets()
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				var cfg agentapi.MemberConfig
				if response.Code == 200 {
					if err := json.Unmarshal(response.Body.Bytes(), &cfg); err != nil {
						t.Fatal(err)
					}
				} else if response.Code != 304 {
					t.Fatal(response.Code, response.Body.String())
				}
				return cfg, response.Code, gets() - before
			}
			type instance struct {
				member string
				cfg    agentapi.MemberConfig
			}
			var instances []instance
			for _, member := range members {
				for range 2 {
					cfg, status, reads := pull(member, "")
					if status != 200 || cfg.PullRevision == "" || reads != 11 {
						t.Fatalf("initial three-peer full render: status=%d GETs=%d (want 11)", status, reads)
					}
					instances = append(instances, instance{member, cfg})
				}
			}
			reloadNotices, noticeRequests := 0, 0
			heartbeat := func(index int) bool {
				t.Helper()
				applied := instances[index]
				data, _ := json.Marshal(agentapi.HeartbeatRequest{Session: fmt.Sprintf("instance-%d", index), ConfigRevision: applied.cfg.ChangeRevision})
				request := httptest.NewRequest("POST", agentapi.HeartbeatPath, bytes.NewReader(data))
				request.Header.Set("Authorization", "Bearer "+applied.member+"-token")
				request.Header.Set(agentapi.ConfigContentRevisionHeader, applied.cfg.ContentRevision)
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != 200 {
					t.Fatal(response.Code, response.Body.String())
				}
				var result agentapi.HeartbeatResponse
				if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.ReloadConfig {
					reloadNotices++
				}
				return result.ReloadConfig
			}
			noticeRenders, periodicRenders, noticeGETs, periodicGETs := 0, 0, 0, 0
			for second := 1; second <= 120; second++ {
				advance(time.Second)
				switch second {
				case 10, 20, 40, 50, 70, 80, 100, 110:
					mutate(second)
				}
				// Leave room for the next proof renewal: successful mutation-
				// driven checks also renew the shared listing's minute lease.
				if drifts == 2 && (second == 30 || second == 90) {
					listingDrift(second == 30)
				}
				if second%5 == 0 && !snapshot.EnsureListing(t.Context()) {
					t.Fatal("listing renewal failed")
				}
				if second%3 == 0 {
					for i := range instances {
						if heartbeat(i) {
							noticeRequests++
							cfg, status, reads := pull(instances[i].member, instances[i].cfg.PullRevision)
							if status == 200 {
								instances[i].cfg = cfg
								noticeRenders++
								noticeGETs += reads
							}
						}
					}
				}
				// Cadence is an input, not a rate bound measured by this loop.
				// agent.TestPeriodicConfigPullSchedulerContract independently
				// checks production Run's 60s ticker and single reload dispatch.
				if second%60 == 0 {
					for i := range instances {
						cfg, status, reads := pull(instances[i].member, instances[i].cfg.PullRevision)
						if status == 200 {
							instances[i].cfg = cfg
							periodicRenders++
							periodicGETs += reads
						}
					}
				}
			}
			t.Logf("10 clusters,20 proxies,8 unrelated mutation events,2min: listingDrifts=%d reloadNotices=%d noticeRequests=%d noticeRenders=%d noticeGETs=%d periodicRenders=%d periodicGETs=%d", drifts, reloadNotices, noticeRequests, noticeRenders, noticeGETs, periodicRenders, periodicGETs)
			if actual := snapshot.DriftGeneration() - startDrift; actual != uint64(drifts) {
				t.Fatalf("fixture produced %d listing drifts, want %d", actual, drifts)
			}
			wantRenders := 0
			if ownDrift {
				wantRenders = drifts * 2 // only this cluster's two proxy instances
			}
			if reloadNotices != wantRenders || noticeRequests != wantRenders {
				t.Fatal("listing drift escaped its cluster or repeated notification", reloadNotices, noticeRequests, wantRenders)
			}
			if noticeRenders != wantRenders || noticeGETs != wantRenders*11 {
				t.Fatal("listing drift escaped its cluster or repeated full rendering", noticeRenders, noticeGETs, wantRenders)
			}
			if periodicRenders != 0 || periodicGETs != 0 {
				t.Fatal("unrelated mutation or settled drift invalidated periodic certificates", periodicRenders, periodicGETs)
			}
			for range 3 {
				_, status, _ := pull(members[0], "")
				if status != 200 {
					t.Fatal("pre-heartbeat agent without a revision received304")
				}
			}
		})
	}
}
