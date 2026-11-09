// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// snapshotCapture must not turn decoding a small JSON value and draining its
// tail into an unbounded allocation. Overflow disables caching, not reading.
type snapshotCapture struct {
	data     []byte
	overflow bool
}

func (c *snapshotCapture) Write(p []byte) (int, error) {
	n := len(p)
	if c.overflow {
		return n, nil
	}
	if n > infra.SnapshotMaxBody-len(c.data) {
		c.overflow = true
		c.data = nil
		return n, nil
	}
	needed := len(c.data) + n
	if needed > cap(c.data) {
		// Clamp capacity too; append's growth factor can exceed the body limit.
		next := make([]byte, len(c.data), min(infra.SnapshotMaxBody, max(needed, 2*cap(c.data))))
		copy(next, c.data)
		c.data = next
	}
	c.data = append(c.data, p...)
	return n, nil
}

func (c *Client) snapshotListing(ctx context.Context) (string, error) {
	// Include protected stacks too: drift anywhere in the target invalidates it.
	list, err := getPages[ServiceStack](infra.WithoutSteadyObservation(ctx), c, c.projectPath("/service-stack"))
	if err != nil {
		return "", err
	}
	return serviceStackFingerprint(list)
}

func serviceStackFingerprint(list []ServiceStack) (string, error) {
	// Work on copies: canonical ordering must not alter caller-owned inventories.
	list = slices.Clone(list)
	for i := range list {
		list[i].Ports = slices.Clone(list[i].Ports)
		slices.SortFunc(list[i].Ports, func(a, b ServicePort) int {
			if a.Port != b.Port {
				if a.Port < b.Port {
					return -1
				}
				return 1
			}
			if n := strings.Compare(a.Scheme, b.Scheme); n != 0 {
				return n
			}
			if a.HTTPSupport == b.HTTPSupport {
				return 0
			}
			if a.HTTPSupport {
				return 1
			}
			return -1
		})
	}
	slices.SortFunc(list, func(a, b ServiceStack) int { return strings.Compare(a.ID, b.ID) })
	// The typed projection excludes created/lastUpdate and nested project/runtime
	// telemetry. Normalize RawMessage objects so JSON key order is immaterial.
	data, err := json.Marshal(list)
	if err != nil {
		return "", err
	}
	var stable any
	if err := json.Unmarshal(data, &stable); err != nil {
		return "", err
	}
	data, err = json.Marshal(stable)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}
