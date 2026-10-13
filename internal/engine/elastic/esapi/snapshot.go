// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"time"
)

// Repository configures a snapshot repository.
type Repository struct {
	Type     string         `json:"type"`
	Settings map[string]any `json:"settings"`
}

func repositoryPath(name string) string     { return "/_snapshot/" + segment(name) }
func snapshotPath(repo, name string) string { return repositoryPath(repo) + "/" + segment(name) }

// PutRepository registers or updates a repository, optionally verifying access.
func (c *Client) PutRepository(ctx context.Context, name string, repo Repository, verify bool) error {
	return c.do(ctx, "PUT", repositoryPath(name), boolQuery("verify", verify), repo, nil)
}

// GetRepository retrieves a repository definition.
func (c *Client) GetRepository(ctx context.Context, name string) (Repository, error) {
	var v map[string]Repository
	err := c.do(ctx, "GET", repositoryPath(name), nil, nil, &v)
	if err != nil {
		return Repository{}, err
	}
	repo, ok := v[name]
	if !ok {
		return Repository{}, &Error{Status: 404, Type: "repository_missing_exception", Reason: "repository not found"}
	}
	return repo, nil
}

// DeleteRepository unregisters a repository without deleting its snapshots.
func (c *Client) DeleteRepository(ctx context.Context, name string) error {
	return c.do(ctx, "DELETE", repositoryPath(name), nil, nil, nil)
}

// VerifyRepository verifies node access, returning sorted participating node names.
func (c *Client) VerifyRepository(ctx context.Context, name string) ([]string, error) {
	var v struct {
		Nodes map[string]struct {
			Name string `json:"name"`
		} `json:"nodes"`
	}
	if err := c.do(ctx, "POST", repositoryPath(name)+"/_verify", nil, nil, &v); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(v.Nodes))
	for _, n := range v.Nodes {
		names = append(names, n.Name)
	}
	sort.Strings(names)
	return names, nil
}

// AnalyzeRepository runs a repository analysis with caller-selected query parameters.
func (c *Client) AnalyzeRepository(ctx context.Context, name string, params map[string]string) (map[string]any, error) {
	q := make(url.Values, len(params))
	for k, v := range params {
		q.Set(k, v)
	}
	var v map[string]any
	err := c.do(ctx, "POST", repositoryPath(name)+"/_analyze", q, nil, &v)
	return v, err
}

// SnapshotRequest selects data, feature states and metadata to snapshot.
// Omitted FeatureStates uses Elasticsearch's include_global_state-dependent default.
type SnapshotRequest struct {
	Indices            string         `json:"indices,omitempty"`
	IncludeGlobalState bool           `json:"include_global_state"`
	FeatureStates      []string       `json:"feature_states,omitempty"`
	Metadata           map[string]any `json:"metadata,omitempty"`
}

// SnapshotFailure describes a failed snapshot shard.
type SnapshotFailure struct {
	Index     string `json:"index"`
	IndexUUID string `json:"index_uuid"`
	ShardID   int    `json:"shard_id"`
	NodeID    string `json:"node_id"`
	Reason    string `json:"reason"`
	Status    string `json:"status"`
}

// SnapshotShards summarizes snapshot shard outcomes.
type SnapshotShards struct {
	Total      int `json:"total"`
	Failed     int `json:"failed"`
	Successful int `json:"successful"`
}

// SnapshotInfo describes a snapshot and its shard outcomes.
type SnapshotInfo struct {
	Snapshot  string            `json:"snapshot"`
	UUID      string            `json:"uuid"`
	State     string            `json:"state"`
	Version   string            `json:"version"`
	Indices   []string          `json:"indices"`
	StartTime time.Time         `json:"start_time"`
	EndTime   time.Time         `json:"end_time"`
	Failures  []SnapshotFailure `json:"failures"`
	Shards    SnapshotShards    `json:"shards"`
	// Reason is the snapshot-level failure reason, if any.
	Reason string `json:"reason,omitempty"`
}

// CreateSnapshot starts a snapshot, optionally waiting for its completion.
func (c *Client) CreateSnapshot(ctx context.Context, repo, name string, req SnapshotRequest, wait bool) error {
	return c.do(ctx, "PUT", snapshotPath(repo, name), boolQuery("wait_for_completion", wait), req, nil)
}

// GetSnapshot retrieves one snapshot, including in-progress snapshots.
func (c *Client) GetSnapshot(ctx context.Context, repo, name string) (SnapshotInfo, error) {
	v, err := c.snapshots(ctx, repo, name)
	if err != nil {
		return SnapshotInfo{}, err
	}
	if len(v) == 0 {
		return SnapshotInfo{}, &Error{Status: 404, Type: "snapshot_missing_exception", Reason: "snapshot not found"}
	}
	return v[0], nil
}

// ListSnapshots retrieves all snapshots in a repository.
func (c *Client) ListSnapshots(ctx context.Context, repo string) ([]SnapshotInfo, error) {
	return c.snapshots(ctx, repo, "_all")
}
func (c *Client) snapshots(ctx context.Context, repo, name string) ([]SnapshotInfo, error) {
	var v struct {
		Snapshots []SnapshotInfo `json:"snapshots"`
	}
	err := c.do(ctx, "GET", snapshotPath(repo, name), nil, nil, &v)
	return v.Snapshots, err
}

// SnapshotSize reports total snapshot bytes when status information is available.
// Callers may treat an error as unavailable metadata, rather than a failed backup.
func (c *Client) SnapshotSize(ctx context.Context, repo, name string) (int64, error) {
	var v struct {
		Snapshots []struct {
			Stats struct {
				Total struct {
					Bytes int64 `json:"size_in_bytes"`
				} `json:"total"`
			} `json:"stats"`
		} `json:"snapshots"`
	}
	if err := c.do(ctx, "GET", snapshotPath(repo, name)+"/_status", nil, nil, &v); err != nil {
		return 0, err
	}
	if len(v.Snapshots) == 0 {
		return 0, errors.New("snapshot size is unavailable on Elasticsearch")
	}
	return v.Snapshots[0].Stats.Total.Bytes, nil
}

// DeleteSnapshot deletes snapshot contents from a writable repository.
func (c *Client) DeleteSnapshot(ctx context.Context, repo, name string) error {
	return c.do(ctx, "DELETE", snapshotPath(repo, name), nil, nil, nil)
}

// RestoreRequest selects data and state for an asynchronous restore.
type RestoreRequest struct {
	Indices            string   `json:"indices,omitempty"`
	IncludeGlobalState bool     `json:"include_global_state"`
	FeatureStates      []string `json:"feature_states,omitempty"`
	IncludeAliases     bool     `json:"include_aliases"`
	IgnoreUnavailable  bool     `json:"ignore_unavailable"`
}

// RestoreSnapshot starts a restore without waiting for completion.
func (c *Client) RestoreSnapshot(ctx context.Context, repo, name string, req RestoreRequest) error {
	return c.do(ctx, "POST", snapshotPath(repo, name)+"/_restore", boolQuery("wait_for_completion", false), req, nil)
}

// RecoveryInProgress reports whether any active shard recovery remains.
func (c *Client) RecoveryInProgress(ctx context.Context) (bool, error) {
	var rows []map[string]any
	err := c.do(ctx, "GET", "/_cat/recovery", url.Values{"active_only": {"true"}, "format": {"json"}}, nil, &rows)
	return len(rows) > 0, err
}
