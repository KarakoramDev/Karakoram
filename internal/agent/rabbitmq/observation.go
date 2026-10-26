// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
)

// observe independently samples local service health and cluster membership.
// Membership can block on a paused peer even with statistics disabled. Its
// failure must not discard fresh local service, alarm and runtime identity
// samples, nor may local health manufacture a cluster running-node view.
func observe(ctx context.Context, c agentapi.MemberConfig) (rmqspec.StatusResponse, error) {
	s, err := spec(c)
	if err != nil {
		return rmqspec.StatusResponse{}, err
	}
	credential, ok := c.SystemUsers["karakoram"]
	if !ok || credential.Password == "" {
		return rmqspec.StatusResponse{}, errors.New("management observation credential unavailable")
	}
	client, base, err := localManagementClient(&c, &s)
	if err != nil {
		return rmqspec.StatusResponse{}, err
	}
	defer client.CloseIdleConnections()
	// Return a diagnostic before the control plane's four-second transport
	// deadline; an unresponsive local API must not become an empty sweep.
	client.Timeout = 2 * time.Second
	get := func(probeCtx context.Context, path string, out any, health bool) (bool, error) {
		req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, base+"/api/"+path, nil)
		if err != nil {
			return false, err
		}
		req.SetBasicAuth("karakoram", credential.Password)
		response, err := client.Do(req)
		if err != nil {
			return false, fmt.Errorf("management API unresponsive at %s: %w; inspect local management health and peer connectivity with rabbitmq-diagnostics/cluster_status; do not reset or force-boot brokers", path, err)
		}
		defer response.Body.Close()
		negative := health && response.StatusCode == http.StatusServiceUnavailable
		if !negative && response.StatusCode != http.StatusOK {
			return false, fmt.Errorf("management observation %s: HTTP %d", path, response.StatusCode)
		}
		if negative && out == nil {
			return false, nil
		}
		limit := int64(2 << 20)
		if health {
			limit = 64 << 10
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
		if err != nil || int64(len(data)) > limit {
			return false, fmt.Errorf("invalid management observation %s", path)
		}
		if out != nil && json.Unmarshal(data, out) != nil {
			return false, fmt.Errorf("invalid management observation %s", path)
		}
		if health && !negative {
			var result struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(data, &result) != nil || result.Status != "ok" {
				return false, fmt.Errorf("invalid management health observation %s", path)
			}
		}
		return !negative, nil
	}
	var nodes []struct {
		Name         string   `json:"name"`
		Type         string   `json:"type"`
		BeingDrained bool     `json:"being_drained"`
		Running      bool     `json:"running"`
		MemoryAlarm  bool     `json:"mem_alarm"`
		DiskAlarm    bool     `json:"disk_free_alarm"`
		Partitions   []string `json:"partitions"`
	}
	var inService, alarmFree bool
	var version string
	var alarmStatus struct {
		Alarms []map[string]string `json:"alarms"`
	}
	var adminCredentialStatus string
	var membershipErr error
	var failures [3]error
	var probes sync.WaitGroup
	probes.Go(func() {
		membershipCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		_, membershipErr = get(membershipCtx, "nodes?disable_stats=true", &nodes, false)
	})
	// Each local probe has its own two-second HTTP budget, independent of the
	// shorter peer-dependent membership budget.
	probes.Go(func() { inService, failures[0] = get(ctx, "health/checks/is-in-service", nil, true) })
	probes.Go(func() { alarmFree, failures[1] = get(ctx, "health/checks/local-alarms", &alarmStatus, true) })
	probes.Go(func() { version, failures[2] = observeRuntimeVersion(ctx, client, s.PrometheusPort, s.NodeName) })
	probes.Go(func() {
		// Leave room inside the control plane's observation deadline for
		// dialing and returning the mandatory health response.
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		adminCredentialStatus = observeAdminCredential(probeCtx, client, base, c.SystemUsers["admin"].Password)
	})
	probes.Wait()
	if membershipErr != nil {
		// JSON decoding can populate a prefix before reporting an error.
		// Never publish partial membership or its optional node diagnostics.
		nodes = nil
	}
	if err := errors.Join(failures[:]...); err != nil {
		return rmqspec.StatusResponse{}, err
	}
	result := rmqspec.StatusResponse{NodeName: s.NodeName, Version: version, Maintenance: !inService, AdminCredentialStatus: adminCredentialStatus, ConfigVersion: c.ConfigVersion}
	alarms := alarmStatus.Alarms
	if alarms == nil {
		alarms = []map[string]string{}
	}
	partitions := map[string][]string{}
	local := false
	for _, node := range nodes {
		if node.Name == s.NodeName {
			local = true
		}
		if node.Running {
			result.RunningNodes = append(result.RunningNodes, node.Name)
		}
		if node.Type == "disc" {
			result.DiskNodes = append(result.DiskNodes, node.Name)
		}
		if node.Name == s.NodeName && node.MemoryAlarm {
			alarms = append(alarms, map[string]string{"node": node.Name, "resource": "memory"})
		}
		if node.Name == s.NodeName && node.DiskAlarm {
			alarms = append(alarms, map[string]string{"node": node.Name, "resource": "disk"})
		}
		if len(node.Partitions) > 0 {
			partitions[node.Name] = node.Partitions
		}
	}
	if result.Version == "" {
		return rmqspec.StatusResponse{}, errors.New("local runtime version unavailable")
	}
	if !alarmFree && len(alarms) == 0 {
		alarms = append(alarms, map[string]string{"node": s.NodeName, "resource": "unknown"})
	}
	result.Alarms, _ = json.Marshal(alarms)
	result.Partitions, _ = json.Marshal(partitions)
	if membershipErr == nil && !local {
		membershipErr = errors.New("management membership observation omitted local broker; inspect peer connectivity with rabbitmq-diagnostics/cluster_status")
	}
	if membershipErr != nil {
		result.MembershipError = membershipErr.Error()
		result.RunningNodes, result.DiskNodes, result.Partitions = nil, nil, nil
	}
	return result, nil
}

// Only a fresh, explicit 401 proves drift. Timeouts, transport failures and
// other HTTP statuses cannot distinguish bad credentials from an unavailable
// broker. A successful probe must identify the desired reserved account.
func observeAdminCredential(ctx context.Context, client *http.Client, base, password string) string {
	if password == "" {
		return "unknown"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/api/whoami", nil)
	if err != nil {
		return "unknown"
	}
	req.SetBasicAuth("admin", password)
	response, err := client.Do(req)
	if err != nil {
		return "unknown"
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized {
		return "rejected"
	}
	if response.StatusCode != http.StatusOK {
		return "unknown"
	}
	const limit = 64 << 10
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	var identity struct {
		Name string `json:"name"`
	}
	if err != nil || len(data) > limit || json.Unmarshal(data, &identity) != nil || identity.Name != "admin" {
		return "unknown"
	}
	return "accepted"
}

// Runtime identity is available even when management statistics collection is
// disabled or slow. The Prometheus plugin/listener is mandatory for our member
// plans. Bind it to the same local broker identity as the membership response.
func observeRuntimeVersion(ctx context.Context, client *http.Client, port int, node string) (string, error) {
	if port <= 0 {
		return "", errors.New("runtime version listener unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort("127.0.0.1", strconv.Itoa(port))+"/metrics", nil)
	if err != nil {
		return "", err
	}
	response, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("runtime version observation: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("runtime version observation: HTTP %d", response.StatusCode)
	}
	limited := &io.LimitedReader{R: response.Body, N: (2 << 20) + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), (2<<20)+1)
	var identity []byte
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		// Decode only the two required identity families, not every queue,
		// connection, counter and histogram in the broker's metrics export.
		if bytes.HasPrefix(line, []byte("rabbitmq_build_info{")) || bytes.HasPrefix(line, []byte("rabbitmq_identity_info{")) {
			identity = append(identity, line...)
			identity = append(identity, '\n')
		}
	}
	if scanner.Err() != nil || limited.N == 0 {
		return "", errors.New("invalid runtime version observation")
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(identity))
	if err != nil {
		return "", errors.New("invalid runtime version observation")
	}
	values := [2]string{}
	for i, field := range []struct{ family, label string }{
		{"rabbitmq_build_info", "rabbitmq_version"},
		{"rabbitmq_identity_info", "rabbitmq_node"},
	} {
		family := families[field.family]
		if family == nil || len(family.Metric) != 1 {
			return "", errors.New("runtime version observation omitted local identity")
		}
		metric := family.Metric[0]
		if metric.Gauge == nil && metric.Untyped == nil || metric.Gauge != nil && metric.Gauge.GetValue() != 1 || metric.Untyped != nil && metric.Untyped.GetValue() != 1 {
			return "", errors.New("invalid runtime version identity value")
		}
		for _, label := range metric.Label {
			if label.GetName() == field.label {
				values[i] = label.GetValue()
			}
		}
	}
	if values[0] == "" || values[1] != node {
		return "", errors.New("runtime version observation omitted local broker or version")
	}
	return values[0], nil
}
