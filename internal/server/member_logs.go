// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"slices"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
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
			return s.failure(err)
		}
		for _, l := range logs {
			a.Lines = append(a.Lines, api.LogLine{Timestamp: l.Timestamp, Process: proc, Message: l.Message})
		}
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
