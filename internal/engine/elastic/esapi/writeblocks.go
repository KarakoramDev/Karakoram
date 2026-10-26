// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package esapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
)

// IndexWriteBlocks observes native index write restrictions, including hidden
// system indices. It never removes a disk-pressure or operator-owned block.
func (c *Client) IndexWriteBlocks(ctx context.Context) ([]string, error) {
	const names = "index.blocks.read_only_allow_delete,index.blocks.read_only,index.blocks.write"
	var indices map[string]struct {
		Settings map[string]json.RawMessage `json:"settings"`
	}
	q := url.Values{"flat_settings": {"true"}, "expand_wildcards": {"all"}, "name": {names}}
	if err := c.do(ctx, "GET", "/_all/_settings", q, nil, &indices); err != nil {
		return nil, err
	}
	var blocks []string
	for index, value := range indices {
		for _, key := range []string{"index.blocks.read_only_allow_delete", "index.blocks.read_only", "index.blocks.write"} {
			raw, ok := value.Settings[key]
			if !ok {
				continue
			}
			text := string(raw)
			if len(text) > 0 && text[0] == '"' {
				if err := json.Unmarshal(raw, &text); err != nil {
					return nil, err
				}
			}
			enabled, err := strconv.ParseBool(text)
			if err != nil {
				return nil, fmt.Errorf("invalid Elasticsearch index write-block setting %s", key)
			}
			if enabled {
				blocks = append(blocks, index+" ("+key+")")
			}
		}
	}
	sort.Strings(blocks)
	return blocks, nil
}
