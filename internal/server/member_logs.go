// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

func (s *Server) memberLogs(ctx context.Context, rc *engine.Context, p engine.Provider, member string) response {
	q := request(ctx).URL.Query()
	lines := 100
	if v := q.Get("lines"); v != "" {
		var err error
		lines, err = strconv.Atoi(v)
		if err != nil || lines < 1 || lines > 10000 {
			return invalid()
		}
	}
	proc := q.Get("process")
	processes := p.Describe().Processes
	if proc == "" {
		proc = "agent"
		if len(processes) > 0 {
			proc = processes[0]
		}
	}
	if proc != "agent" && !slices.Contains(processes, proc) {
		return invalid()
	}
	a := api.MemberLogs{Member: member, Lines: []api.LogLine{}}
	if proc == "agent" {
		// The agent is the runtime's main process, not a supervised child.
		// Check cluster ownership before querying namespace-wide platform logs.
		if !slices.ContainsFunc(rc.Cluster.Status.Members, func(m domain.MemberStatus) bool { return m.Name == member }) {
			return fail(404, "not_found", "Member not found")
		}
		if !rc.Backend.Capabilities().Logs {
			return s.failure(engine.ErrNotSupported)
		}
		logs, err := rc.Backend.Logs(ctx, member, infra.LogQuery{Limit: lines})
		if err != nil {
			switch {
			case errors.Is(err, infra.ErrNotFound):
				return s.failure(engine.ErrNotFound)
			case errors.Is(err, infra.ErrUnavailable):
				return s.failure(engine.ErrUnavailable)
			default:
				return s.failure(err)
			}
		}
		a.Lines = runtimeLogLines(logs, proc, lines)
	} else {
		dp, yes := p.(engine.DataPlane)
		if !yes {
			return s.unsupportedClusterAction("GetMemberLogs", p)
		}
		logs, err := dp.MemberLogs(ctx, rc, member, proc, lines)
		if err != nil {
			return s.failure(err)
		}
		for _, l := range logs {
			a.Lines = append(a.Lines, api.LogLine{Timestamp: l.Timestamp, Process: l.Process, Message: l.Message})
		}
	}
	return response{status: 200, body: a, noStore: true}
}

// Runtime logs may predate supervised-output redaction. Sanitize each physical
// pipe independently, including multiline values, before exposing platform text.
func runtimeLogLines(logs []infra.LogLine, process string, limit int) []api.LogLine {
	type pipe struct{ container, stream string }
	type tail struct {
		sanitizer redact.Stream
		at        time.Time
	}
	tails := make(map[pipe]*tail)
	order := make([]*tail, 0)
	out := make([]api.LogLine, 0, min(len(logs), limit))
	next := 0
	appendLine := func(at time.Time, message string) {
		line := api.LogLine{Timestamp: at, Process: process, Message: message}
		if len(out) < limit {
			out = append(out, line)
		} else {
			out[next] = line
			next = (next + 1) % limit
		}
	}
	for _, line := range logs {
		key := pipe{line.Container, line.Stream}
		t := tails[key]
		if t == nil {
			t = &tail{}
			tails[key] = t
			order = append(order, t)
		}
		t.at = line.Timestamp
		for message := range strings.SplitSeq(strings.TrimSuffix(line.Message, "\n"), "\n") {
			safe, gap, keep := t.sanitizer.Line(message)
			if gap != "" {
				appendLine(line.Timestamp, gap)
			}
			if keep {
				appendLine(line.Timestamp, safe)
			}
		}
	}
	for _, t := range order {
		if gap := t.sanitizer.Omission(); gap != "" {
			appendLine(t.at, gap)
		}
	}
	if next != 0 {
		// Rotate the bounded ring back into chronological response order.
		slices.Reverse(out[:next])
		slices.Reverse(out[next:])
		slices.Reverse(out)
	}
	return out
}
