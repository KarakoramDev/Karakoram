// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type observationKey struct {
	client      *client
	path, query string
	epoch       uint64
	steady      bool
}

type observationResponse struct {
	body  []byte
	index uint64
}

type observationCall struct {
	done  chan struct{}
	value observationResponse
	err   error
}

// Only attempt-local discovery facts are memoized. In particular, pinned dial
// authorization must enumerate allocations again for every connection.
func (c *client) observationGet(ctx context.Context, path string, query url.Values) (observationResponse, error) {
	read := func() (observationResponse, error) {
		body, index, err := c.request(ctx, http.MethodGet, path, query, nil, jsonResponseLimit, 0, false)
		return observationResponse{body: body, index: index}, err
	}
	cache := infra.ObservationCache(ctx)
	if cache == nil {
		return read()
	}
	allocations := strings.HasPrefix(path, "/v1/job/") && strings.HasSuffix(path, "/allocations")
	job := strings.HasPrefix(path, "/v1/job/") && !strings.Contains(strings.TrimPrefix(path, "/v1/job/"), "/")
	if query.Has("index") || query.Has("wait") || (!strings.HasPrefix(path, "/v1/var/") && !job && !allocations) || allocations && infra.FreshTaskInventory(ctx) || c.observationMutations.Load() != 0 {
		return read()
	}
	key := observationKey{client: c, path: path, query: query.Encode(), epoch: c.observationEpoch.Load()}
	if infra.SteadyObservation(ctx) {
		// Fresh facts may serve both classes. Snapshot-eligible reads must
		// never supply authorization, including while their leader is in flight.
		if _, fresh := cache.Load(key); !fresh {
			key.steady = true
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return observationResponse{}, err
		}
		actual, loaded := cache.Load(key)
		if !loaded {
			actual, loaded = cache.LoadOrStore(key, &observationCall{done: make(chan struct{})})
		}
		call := actual.(*observationCall)
		if loaded {
			select {
			case <-call.done:
				if ctx.Err() == nil && (errors.Is(call.err, context.Canceled) || errors.Is(call.err, context.DeadlineExceeded)) {
					cache.CompareAndDelete(key, call)
					continue
				}
				return call.value, call.err
			case <-ctx.Done():
				return observationResponse{}, ctx.Err()
			}
		}
		call.value, call.err = read()
		if call.err != nil {
			cache.CompareAndDelete(key, call)
		}
		close(call.done)
		return call.value, call.err
	}
}
