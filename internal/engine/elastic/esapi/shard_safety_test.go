// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
)

func TestCommittedVoters(t *testing.T) {
	c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_cluster/state/metadata" || r.URL.Query().Get("filter_path") != "metadata.cluster_coordination.last_committed_config" {
			t.Error("voting configuration request", r.URL)
		}
		_, _ = w.Write([]byte(`{"metadata":{"cluster_coordination":{"last_committed_config":["id-a","id-b","id-c"]}}}`))
	})
	voters, err := c.CommittedVoters(context.Background())
	if err != nil || !reflect.DeepEqual(voters, []string{"id-a", "id-b", "id-c"}) {
		t.Fatal(voters, err)
	}
}

func TestUnassignedReplicasUnplaceable(t *testing.T) {
	row := func(index, shard, primary, state, node string) map[string]string {
		return map[string]string{"index": index, "shard": shard, "prirep": primary, "state": state, "node": node}
	}
	primary := row("logs", "0", "p", "STARTED", "n0")
	replica := row("logs", "0", "r", "STARTED", "n1")
	missing := row("logs", "0", "r", "UNASSIGNED", "")
	for _, tc := range []struct {
		name  string
		nodes int
		rows  []map[string]string
		want  bool
	}{
		{"single-node", 1, []map[string]string{primary, missing}, true},
		{"full-two-node", 2, []map[string]string{primary, replica, missing}, true},
		{"placeable", 2, []map[string]string{primary, missing}, false},
		{"missing-intended-node", 3, []map[string]string{primary, replica, missing}, false},
		{"unassigned-primary", 1, []map[string]string{row("logs", "0", "p", "UNASSIGNED", "")}, false},
		{"other-index", 2, []map[string]string{primary, replica, missing, row("other", "0", "p", "STARTED", "n0"), row("other", "0", "r", "UNASSIGNED", "")}, false},
		{"other-shard", 2, []map[string]string{primary, replica, missing, row("logs", "1", "p", "STARTED", "n0"), row("logs", "1", "r", "UNASSIGNED", "")}, false},
		{"duplicate-node", 2, []map[string]string{primary, row("logs", "0", "r", "STARTED", "n0"), missing}, false},
		{"initializing-copy", 2, []map[string]string{primary, row("logs", "0", "r", "INITIALIZING", "n1"), missing}, false},
		{"no-shards", 1, nil, false},
		{"no-missing-copy", 1, []map[string]string{primary}, false},
		{"no-data-nodes", 0, []map[string]string{missing}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/_cat/shards" || r.URL.Query().Get("h") != "index,shard,state,prirep,node" {
					t.Error("shard capacity request", r.URL)
				}
				_ = json.NewEncoder(w).Encode(tc.rows)
			})
			got, err := c.UnassignedReplicasUnplaceable(context.Background(), tc.nodes)
			if err != nil || got != tc.want {
				t.Fatalf("unplaceable=%v err=%v, want %v", got, err, tc.want)
			}
		})
	}
}
