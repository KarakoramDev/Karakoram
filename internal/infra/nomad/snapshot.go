// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// These projections deliberately omit scheduler indices, timestamps and task
// events that change without changing the platform facts used by observations.
type snapshotJob struct {
	ID, Status              string
	Stop                    bool
	Version, JobModifyIndex uint64
	Meta                    map[string]string
}

type snapshotAllocation struct {
	ID, JobID, ClientStatus, DesiredStatus, NodeID string
	JobVersion                                     uint64
}

func (c *client) snapshotListing(ctx context.Context) (string, error) {
	ctx = infra.WithoutSteadyObservation(ctx)
	var listing struct {
		Jobs        []snapshotJob
		Allocations []snapshotAllocation
	}
	if _, err := c.get(ctx, "/v1/jobs", query("meta", "true"), &listing.Jobs); err != nil {
		return "", err
	}
	if _, err := c.get(ctx, "/v1/allocations", nil, &listing.Allocations); err != nil {
		return "", err
	}
	for i := range listing.Jobs {
		for key := range listing.Jobs[i].Meta {
			if !strings.HasPrefix(key, "karakoram.") && !strings.HasPrefix(key, "KARAKORAM_") {
				delete(listing.Jobs[i].Meta, key)
			}
		}
		if len(listing.Jobs[i].Meta) == 0 {
			listing.Jobs[i].Meta = nil
		}
	}
	sort.Slice(listing.Jobs, func(i, j int) bool { return listing.Jobs[i].ID < listing.Jobs[j].ID })
	sort.Slice(listing.Allocations, func(i, j int) bool { return listing.Allocations[i].ID < listing.Allocations[j].ID })
	data, err := json.Marshal(listing)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}
