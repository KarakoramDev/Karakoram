// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
)

func TestRetainedRaftStatisticsReadiness(t *testing.T) {
	// Synthetic identities, native 1.39.12 response shape after a retained-store
	// restart: bootstrapped is omitted, but the sole voter is ready and leader.
	const response = `{"statistics":[{"candidates":{},"dbLoaded":true,"initialLastAppliedIndex":4,"isVoter":true,"leaderAddress":"192.0.2.1:8300","leaderId":"demon000000x","name":"demon000000x","open":true,"raft":{"appliedIndex":"6","commitIndex":"6","latestConfiguration":[{"address":"192.0.2.1:8300","id":"demon000000x","suffrage":0}],"numPeers":"0","state":"Leader"},"ready":true,"status":"HEALTHY"}],"synchronized":true}`
	s := &state{Members: []*member{{Name: "demon000000x"}}, Voters: []string{"demon000000x"}}
	for _, tc := range []struct {
		name   string
		change func(*wvapi.ClusterStatistics)
		want   bool
	}{
		{"omitted-bootstrap", func(*wvapi.ClusterStatistics) {}, true},
		{"false-bootstrap", func(v *wvapi.ClusterStatistics) { v.Statistics[0].Bootstrapped = false }, true},
		{"true-bootstrap", func(v *wvapi.ClusterStatistics) { v.Statistics[0].Bootstrapped = true }, true},
		{"unsynchronized", func(v *wvapi.ClusterStatistics) { v.Synchronized = false }, false},
		{"not-ready", func(v *wvapi.ClusterStatistics) { v.Statistics[0].Ready = false }, false},
		{"database-not-loaded", func(v *wvapi.ClusterStatistics) { v.Statistics[0].DBLoaded = false }, false},
		{"store-closed", func(v *wvapi.ClusterStatistics) { v.Statistics[0].Open = false }, false},
		{"unhealthy", func(v *wvapi.ClusterStatistics) { v.Statistics[0].Status = "UNHEALTHY" }, false},
		{"unknown-leader", func(v *wvapi.ClusterStatistics) { v.Statistics[0].LeaderID = json.RawMessage(`"other"`) }, false},
		{"wrong-voter-role", func(v *wvapi.ClusterStatistics) { v.Statistics[0].IsVoter = false }, false},
		{"wrong-voter-identity", func(v *wvapi.ClusterStatistics) {
			v.Statistics[0].Raft.LatestConfiguration = json.RawMessage(`[{"id":"other","suffrage":0}]`)
		}, false},
		{"missing-member", func(v *wvapi.ClusterStatistics) { v.Statistics = nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stats wvapi.ClusterStatistics
			if err := json.Unmarshal([]byte(response), &stats); err != nil {
				t.Fatal(err)
			}
			tc.change(&stats)
			got, err := validateClusterStatistics(s, stats)
			if err != nil || got != tc.want {
				t.Fatalf("ready = %v, err = %v; want %v", got, err, tc.want)
			}
		})
	}
}
