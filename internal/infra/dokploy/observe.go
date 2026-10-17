// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func (b *Backend) Containers(ctx context.Context, hostname string) ([]infra.Container, error) {
	a, err := b.application(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if a == nil {
		return []infra.Container{}, nil
	}
	m, _ := parseMetadata(a.Description)
	if m.Kind != infra.KindRuntime {
		return []infra.Container{}, nil
	}
	ts, err := b.tasks(ctx, a.AppName)
	if err != nil {
		return nil, err
	}
	out := []infra.Container{}
	for _, t := range ts {
		running := strings.HasPrefix(strings.ToLower(t.CurrentState), "running")
		if !strings.EqualFold(t.State, "running") && !running {
			continue
		}
		status := infra.StatusCreating
		if running {
			status = infra.ContainerRunning
		}
		// The API returns docker service ps --no-trunc: containerId is the
		// Swarm task ID and name is service.slot. Their combination is the
		// individual task's overlay DNS name, not the load-balanced service.
		taskHostname := ""
		if t.Name != "" && t.ID != "" {
			name := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(t.Name), `\_`))
			taskHostname = name + "." + t.ID
		}
		out = append(out, infra.Container{ID: t.ID, Hostname: taskHostname, Status: status, PlacementGroup: t.Node})
	}
	return out, nil
}
func (b *Backend) Logs(ctx context.Context, hostname string, q infra.LogQuery) ([]infra.LogLine, error) {
	if !b.Capabilities().Logs {
		return nil, infra.ErrUnsupported
	}
	a, _, err := b.require(ctx, hostname)
	if err != nil {
		return nil, err
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 10000 {
		limit = 10000
	}
	since := "all"
	if !q.Since.IsZero() {
		seconds := int64(time.Since(q.Since).Seconds()) + 1
		if seconds < 1 {
			seconds = 1
		}
		since = strconv.FormatInt(seconds, 10) + "s"
	}
	var raw string
	if err = b.c.get(ctx, "application.readLogs", map[string]string{"applicationId": a.ID, "tail": strconv.Itoa(limit), "since": since}, &raw); err != nil {
		return nil, err
	}
	lines := parseLogs(raw, q)
	if len(lines) > limit {
		lines = lines[len(lines)-limit:]
	}
	return lines, nil
}
func parseLogs(raw string, q infra.LogQuery) []infra.LogLine {
	out := []infra.LogLine{}
	for _, text := range strings.Split(raw, "\n") {
		if text == "" {
			continue
		}
		line := infra.LogLine{Stream: "stdout"}
		timestamp, message, ok := strings.Cut(text, " ")
		if ok {
			if parsed, err := time.Parse(time.RFC3339Nano, timestamp); err == nil {
				line.Timestamp = parsed
				text = message
			}
		}
		line.Message = text
		if !q.Since.IsZero() && !line.Timestamp.IsZero() && line.Timestamp.Before(q.Since) {
			continue
		}
		if q.Container != "" && !strings.Contains(line.Container, q.Container) {
			continue
		}
		out = append(out, line)
	}
	return out
}
