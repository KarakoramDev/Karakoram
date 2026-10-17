// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func (p *Provider) MemberLogs(ctx context.Context, rc *engine.Context, name, process string, lines int) ([]domain.MemberLogLine, error) {
	state, err := loadState(rc.Cluster)
	if err != nil {
		return nil, err
	}
	for _, member := range state.Members {
		if member.Name != name {
			continue
		}
		if process != member.Component {
			return nil, engine.Invalid("process does not belong to Supabase component member")
		}
		client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), name)
		if err != nil {
			return nil, err
		}
		defer client.Close()
		logs, err := client.Logs(ctx, process, lines)
		if err != nil {
			return nil, err
		}
		out := make([]domain.MemberLogLine, 0, len(logs.Lines))
		for _, line := range logs.Lines {
			out = append(out, domain.MemberLogLine{
				Timestamp: line.Timestamp, Member: name, Process: line.Process,
				Stream: line.Stream, Message: line.Message,
			})
		}
		return out, nil
	}
	return p.Core.MemberLogs(ctx, rc, name, process, lines)
}

func (p *Provider) Placement(ctx context.Context, rc *engine.Context) (domain.Placement, error) {
	status, err := p.Status(ctx, rc)
	return status.Placement, err
}
