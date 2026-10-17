// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"
)

var adminClient = &http.Client{
	Timeout:       3 * time.Second,
	Transport:     &http.Transport{}, // Never send local administration through a proxy.
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// HostCounts observes Envoy's actual resolved hosts, not the configured DNS names.
// Only names and counts leave loopback; no admin response bodies are exposed.
func HostCounts(ctx context.Context) (map[string]int, error) {
	return hostCounts(ctx, "http://127.0.0.1:"+strconv.Itoa(AdminPort)+"/clusters?format=json")
}

func hostCounts(ctx context.Context, endpoint string) (map[string]int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	response, err := adminClient.Do(req)
	if err != nil {
		return nil, errors.New("gateway admin is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("gateway admin is not ready")
	}
	return parseHostCounts(response.Body)
}

func parseHostCounts(body io.Reader) (map[string]int, error) {
	var clusters struct {
		Statuses []struct {
			Name  string     `json:"name"`
			Hosts []struct{} `json:"host_statuses"`
		} `json:"cluster_statuses"`
	}
	if err := json.NewDecoder(body).Decode(&clusters); err != nil {
		return nil, errors.New("invalid gateway cluster status")
	}
	counts := make(map[string]int, len(clusters.Statuses))
	for _, cluster := range clusters.Statuses {
		counts[cluster.Name] = len(cluster.Hosts)
	}
	return counts, nil
}
