// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Info is the cluster identity and responding node version.
type Info struct {
	Name        string `json:"name"`
	ClusterName string `json:"cluster_name"`
	ClusterUUID string `json:"cluster_uuid"`
	Version     struct {
		Number string `json:"number"`
	} `json:"version"`
}

// Info retrieves cluster identity.
func (c *Client) Info(ctx context.Context) (Info, error) {
	var v Info
	err := c.do(ctx, "GET", "/", nil, nil, &v)
	return v, err
}

// HealthOptions controls an optional server-side health wait.
type HealthOptions struct {
	WaitForStatus string
	Timeout       time.Duration
	Local         bool
}

// Health is the shard health and node count response.
type Health struct {
	Status              string `json:"status"`
	NumberOfNodes       int    `json:"number_of_nodes"`
	NumberOfDataNodes   int    `json:"number_of_data_nodes"`
	ActivePrimaryShards int    `json:"active_primary_shards"`
	ActiveShards        int    `json:"active_shards"`
	RelocatingShards    int    `json:"relocating_shards"`
	InitializingShards  int    `json:"initializing_shards"`
	UnassignedShards    int    `json:"unassigned_shards"`
	TimedOut            bool   `json:"timed_out"`
}

// Health retrieves shard health, optionally waiting for a minimum status.
func (c *Client) Health(ctx context.Context, o HealthOptions) (Health, error) {
	q := boolQuery("local", o.Local)
	if o.WaitForStatus != "" {
		q.Set("wait_for_status", o.WaitForStatus)
	}
	if o.Timeout != 0 {
		q.Set("timeout", duration(o.Timeout))
	}
	var v Health
	err := c.do(ctx, "GET", "/_cluster/health", q, nil, &v)
	return v, err
}

// Node is the observed identity, version and roles of a cluster node.
type Node struct {
	ID      string   `json:"-"`
	Name    string   `json:"name"`
	Version string   `json:"version"`
	Roles   []string `json:"roles"`
}

// Nodes returns nodes ordered by name and ID.
func (c *Client) Nodes(ctx context.Context) ([]Node, error) {
	var v struct {
		Nodes map[string]Node `json:"nodes"`
	}
	err := c.do(ctx, "GET", "/_nodes", url.Values{"filter_path": {"nodes.*.name,nodes.*.version,nodes.*.roles,nodes.*.host,nodes.*.ip"}}, nil, &v)
	if err != nil {
		return nil, err
	}
	nodes := make([]Node, 0, len(v.Nodes))
	for id, n := range v.Nodes {
		n.ID = id
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Name == nodes[j].Name {
			return nodes[i].ID < nodes[j].ID
		}
		return nodes[i].Name < nodes[j].Name
	})
	return nodes, nil
}

// CommittedVoters returns node IDs in the last committed voting configuration.
func (c *Client) CommittedVoters(ctx context.Context) ([]string, error) {
	var v struct {
		Metadata struct {
			Coordination struct {
				Voters []string `json:"last_committed_config"`
			} `json:"cluster_coordination"`
		} `json:"metadata"`
	}
	err := c.do(ctx, "GET", "/_cluster/state/metadata", url.Values{"filter_path": {"metadata.cluster_coordination.last_committed_config"}}, nil, &v)
	return v.Metadata.Coordination.Voters, err
}

// MasterNode returns the elected master's node name, or empty when none is elected.
func (c *Client) MasterNode(ctx context.Context) (string, error) {
	var v []struct {
		Node string `json:"node"`
	}
	err := c.do(ctx, "GET", "/_cat/master", url.Values{"format": {"json"}}, nil, &v)
	if err != nil || len(v) == 0 {
		return "", err
	}
	if v[0].Node == "-" {
		return "", nil
	}
	return v[0].Node, nil
}

// ShardCounts returns allocated shard counts by node name, excluding UNASSIGNED.
func (c *Client) ShardCounts(ctx context.Context) (map[string]int, error) {
	var v []struct {
		Node   string `json:"node"`
		Shards string `json:"shards"`
	}
	if err := c.do(ctx, "GET", "/_cat/allocation", url.Values{"format": {"json"}, "bytes": {"b"}}, nil, &v); err != nil {
		return nil, err
	}
	out := make(map[string]int, len(v))
	for _, row := range v {
		if row.Node == "UNASSIGNED" {
			continue
		}
		n, err := strconv.Atoi(row.Shards)
		if err != nil {
			return nil, errors.New("invalid Elasticsearch shard count")
		}
		out[row.Node] = n
	}
	return out, nil
}

// ClusterSettings holds explicit persistent and transient settings in flat form.
type ClusterSettings struct {
	Persistent map[string]any `json:"persistent,omitempty"`
	Transient  map[string]any `json:"transient,omitempty"`
}

// ClusterSettings retrieves settings without implicit defaults.
func (c *Client) ClusterSettings(ctx context.Context) (ClusterSettings, error) {
	var v ClusterSettings
	err := c.do(ctx, "GET", "/_cluster/settings", boolQuery("flat_settings", true), nil, &v)
	return v, err
}

// PutClusterSettings updates explicit settings; nil values reset a setting.
func (c *Client) PutClusterSettings(ctx context.Context, persistent, transient map[string]any) error {
	return c.do(ctx, "PUT", "/_cluster/settings", nil, ClusterSettings{persistent, transient}, nil)
}

// SetAllocationEnable sets persistent allocation.enable; nil restores its default.
func (c *Client) SetAllocationEnable(ctx context.Context, value *string) error {
	var v any
	if value != nil {
		v = *value
	}
	return c.PutClusterSettings(ctx, map[string]any{"cluster.routing.allocation.enable": v}, nil)
}

// AllocationExcludeNames merges both explicit layers so migration preserves
// operator exclusions hidden by a transient override.
func (c *Client) AllocationExcludeNames(ctx context.Context) ([]string, error) {
	v, err := c.ClusterSettings(ctx)
	if err != nil {
		return nil, err
	}
	key := "cluster.routing.allocation.exclude._name"
	var names []string
	for _, x := range []any{v.Persistent[key], v.Transient[key]} {
		if x == nil {
			continue
		}
		s, ok := x.(string)
		if !ok {
			return nil, errors.New("invalid Elasticsearch allocation exclusion")
		}
		for _, n := range strings.Split(s, ",") {
			if n = strings.TrimSpace(n); n != "" && !slices.Contains(names, n) {
				names = append(names, n)
			}
		}
	}
	return names, nil
}

// SetAllocationExcludeNames replaces effective exclusions, clearing transient precedence.
func (c *Client) SetAllocationExcludeNames(ctx context.Context, names []string) error {
	var v any
	if len(names) > 0 {
		v = strings.Join(names, ",")
	}
	key := "cluster.routing.allocation.exclude._name"
	return c.PutClusterSettings(ctx, map[string]any{key: v}, map[string]any{key: nil})
}

// Flush flushes indices, returning conflicts for the caller to handle.
func (c *Client) Flush(ctx context.Context) error { return c.do(ctx, "POST", "/_flush", nil, nil, nil) }

// AddVotingExclusion excludes named master-eligible nodes and waits for voting reconfiguration.
func (c *Client) AddVotingExclusion(ctx context.Context, nodeNames []string, timeout time.Duration) error {
	q := url.Values{"node_names": {strings.Join(nodeNames, ",")}}
	if timeout != 0 {
		q.Set("timeout", duration(timeout))
	}
	return c.do(ctx, "POST", "/_cluster/voting_config_exclusions", q, nil, nil)
}

// ClearVotingExclusions clears exclusions, optionally waiting for excluded nodes to leave.
func (c *Client) ClearVotingExclusions(ctx context.Context, waitForRemoval bool) error {
	return c.do(ctx, "DELETE", "/_cluster/voting_config_exclusions", boolQuery("wait_for_removal", waitForRemoval), nil, nil)
}

// ReloadSecureSettings reloads node keystores and reports any node-level failure.
func (c *Client) ReloadSecureSettings(ctx context.Context) error {
	var v struct {
		Nodes map[string]struct {
			ReloadException json.RawMessage `json:"reload_exception"`
		} `json:"nodes"`
		Summary struct {
			Failed int `json:"failed"`
		} `json:"_nodes"`
	}
	if err := c.do(ctx, "POST", "/_nodes/reload_secure_settings", nil, nil, &v); err != nil {
		return err
	}
	if v.Summary.Failed > 0 {
		return errors.New("secure settings reload failed on Elasticsearch")
	}
	for _, n := range v.Nodes {
		if len(n.ReloadException) > 0 && string(n.ReloadException) != "null" {
			return errors.New("node secure settings reload failed on Elasticsearch")
		}
	}
	return nil
}

// MaxIndexReplicas returns the largest replica requirement, including auto-expand.
func (c *Client) MaxIndexReplicas(ctx context.Context) (int, error) {
	var indices map[string]struct {
		Settings map[string]json.RawMessage `json:"settings"`
	}
	q := url.Values{"flat_settings": {"true"}, "expand_wildcards": {"all"}, "filter_path": {"*.settings.index.number_of_replicas,*.settings.index.auto_expand_replicas"}}
	if err := c.do(ctx, "GET", "/_all/_settings", q, nil, &indices); err != nil {
		return 0, err
	}
	maximum, dataNodes := 0, -1
	for _, index := range indices {
		raw, ok := index.Settings["index.number_of_replicas"]
		if !ok {
			return 0, errors.New("index replica count unavailable")
		}
		text := string(raw)
		if len(raw) > 0 && raw[0] == '"' {
			if err := json.Unmarshal(raw, &text); err != nil {
				return 0, err
			}
		}
		count, err := strconv.Atoi(text)
		if err != nil || count < 0 {
			return 0, errors.New("invalid index replica count")
		}
		if raw := index.Settings["index.auto_expand_replicas"]; len(raw) > 0 {
			text = string(raw)
			if raw[0] == '"' {
				if err = json.Unmarshal(raw, &text); err != nil {
					return 0, err
				}
			}
			if text != "false" {
				lower, upper, ok := strings.Cut(text, "-")
				minimum, e := strconv.Atoi(lower)
				if !ok || e != nil || minimum < 0 {
					return 0, errors.New("invalid index auto-expand replicas")
				}
				if upper == "all" {
					if dataNodes < 0 {
						h, e := c.Health(ctx, HealthOptions{})
						if e != nil {
							return 0, e
						}
						dataNodes = h.NumberOfDataNodes
					}
					count = max(0, dataNodes-1)
				} else {
					count, err = strconv.Atoi(upper)
					if err != nil || count < minimum {
						return 0, errors.New("invalid index auto-expand replicas")
					}
				}
			}
		}
		maximum = max(maximum, count)
	}
	return maximum, nil
}

// UnassignedReplicasUnplaceable proves that every unassigned copy is a replica
// whose shard already has a started copy on every intended data node. A large
// replica count on a different index or shard is not evidence of safety.
func (c *Client) UnassignedReplicasUnplaceable(ctx context.Context, dataNodes int) (bool, error) {
	var shards []struct {
		Index   string `json:"index"`
		Shard   string `json:"shard"`
		State   string `json:"state"`
		Primary string `json:"prirep"`
		Node    string `json:"node"`
	}
	if err := c.do(ctx, "GET", "/_cat/shards", url.Values{"format": {"json"}, "h": {"index,shard,state,prirep,node"}}, nil, &shards); err != nil {
		return false, err
	}
	type key struct{ index, shard string }
	started := map[key]map[string]bool{}
	var unassigned []key
	for _, shard := range shards {
		k := key{shard.Index, shard.Shard}
		switch shard.State {
		case "UNASSIGNED":
			if shard.Primary != "r" || shard.Index == "" || shard.Shard == "" {
				return false, nil
			}
			unassigned = append(unassigned, k)
		case "STARTED":
			if shard.Node != "" {
				if started[k] == nil {
					started[k] = map[string]bool{}
				}
				started[k][shard.Node] = true
			}
		}
	}
	if dataNodes < 1 || len(unassigned) == 0 {
		return false, nil
	}
	for _, k := range unassigned {
		if len(started[k]) < dataNodes {
			return false, nil
		}
	}
	return true, nil
}
