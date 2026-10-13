// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
)

// ListIndices lists concrete index names, optionally including hidden indices.
func (c *Client) ListIndices(ctx context.Context, expandHidden bool) ([]string, error) {
	expand := "open,closed"
	if expandHidden {
		expand = "all"
	}
	var rows []struct {
		Index string `json:"index"`
	}
	if err := c.do(ctx, "GET", "/_cat/indices", url.Values{"format": {"json"}, "expand_wildcards": {expand}, "h": {"index"}}, nil, &rows); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r.Index)
	}
	sort.Strings(names)
	return names, nil
}

// DeleteDataStreams deletes matching streams, including hidden streams.
func (c *Client) DeleteDataStreams(ctx context.Context, pattern string) error {
	return c.do(ctx, "DELETE", "/_data_stream/"+segment(pattern), url.Values{"expand_wildcards": {"all"}}, nil, nil)
}

// DeleteIndices deletes explicit indices in bounded-length URL batches.
// Empty input is a no-op, never an implicit delete-all request.
func (c *Client) DeleteIndices(ctx context.Context, names []string) error {
	return c.indexBatches(ctx, "DELETE", names, "")
}

// CloseIndices closes explicit indices in bounded-length URL batches.
func (c *Client) CloseIndices(ctx context.Context, names []string) error {
	return c.indexBatches(ctx, "POST", names, "/_close")
}
func (c *Client) indexBatches(ctx context.Context, method string, names []string, suffix string) error {
	const maxURL = 4096
	overhead := 0
	for _, e := range c.endpoints {
		if n := len(e.String()) + len(suffix) + 1; n > overhead {
			overhead = n
		}
	}
	// Validate all names before any mutation, so invalid input cannot partially delete data.
	for _, name := range names {
		if name == "" || len(segment(name))+overhead > maxURL {
			return errors.New("invalid Elasticsearch index name or URL length")
		}
	}
	for start := 0; start < len(names); {
		end := start
		size := overhead
		for end < len(names) {
			n := len(segment(names[end]))
			if end > start {
				n++
			}
			if size+n > maxURL {
				break
			}
			size += n
			end++
		}
		if err := c.do(ctx, method, "/"+listPath(names[start:end])+suffix, nil, nil, nil); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// Index writes a document by ID, optionally refreshing search visibility.
func (c *Client) Index(ctx context.Context, index, id string, doc any, refresh bool) error {
	return c.do(ctx, "PUT", "/"+segment(index)+"/_doc/"+segment(id), boolQuery("refresh", refresh), doc, nil)
}

// Search returns exact total hits and the source documents from the result page.
func (c *Client) Search(ctx context.Context, index string, query any) (int64, []json.RawMessage, error) {
	var v struct {
		Hits struct {
			Total json.RawMessage `json:"total"`
			Hits  []struct {
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := c.do(ctx, "POST", "/"+segment(index)+"/_search", boolQuery("track_total_hits", true), query, &v); err != nil {
		return 0, nil, err
	}
	var total struct {
		Value int64 `json:"value"`
	}
	if err := json.Unmarshal(v.Hits.Total, &total); err != nil {
		if err = json.Unmarshal(v.Hits.Total, &total.Value); err != nil {
			return 0, nil, errors.New("invalid Elasticsearch search total")
		}
	}
	hits := make([]json.RawMessage, len(v.Hits.Hits))
	for i, h := range v.Hits.Hits {
		hits[i] = h.Source
	}
	return total.Value, hits, nil
}

// Count returns the number of documents in selected indices.
func (c *Client) Count(ctx context.Context, index string) (int64, error) {
	var v struct {
		Count int64 `json:"count"`
	}
	err := c.do(ctx, "GET", "/"+segment(index)+"/_count", nil, nil, &v)
	return v.Count, err
}
