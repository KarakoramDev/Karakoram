// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"net/http"
	"time"
)

type configListingLatencyTransport struct {
	base    http.RoundTripper
	advance func(time.Duration)
}

func (t configListingLatencyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err == nil && infra.RequestCaller(request.Context()) == infra.CallerListing {
		// Move the proof clock during real fake-API HTTP, rather than pretending
		// the response completed at the scheduled request-start tick.
		t.advance(250 * time.Millisecond)
	}
	return response, err
}

func withConfigListingLatency(client *http.Client, advance func(time.Duration)) *http.Client {
	clone := *client
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = configListingLatencyTransport{transport, advance}
	return &clone
}
