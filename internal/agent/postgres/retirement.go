// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type patroniCluster struct {
	Members []struct {
		Name string      `json:"name"`
		Role string      `json:"role"`
		Tags patroniTags `json:"tags"`
	} `json:"members"`
}

type patroniTags struct {
	NoFailover    bool `json:"nofailover"`
	NoLoadBalance bool `json:"noloadbalance"`
	CloneFrom     bool `json:"clonefrom"`
}

func retirementTagsApplied(tags patroniTags) bool {
	// Patroni omits false-valued clonefrom from its DCS member record.
	return tags.NoFailover && tags.NoLoadBalance && !tags.CloneFrom
}

func retirementStopped(n *agent.Node, s pgspec.Spec, p paths) bool {
	if !s.Retiring || !n.Stopped("patroni") {
		return false
	}
	states, _ := n.Snapshot()
	if states["patroni"].PID != 0 || states["postgres"].PID != 0 {
		return false
	}
	_, err := os.Stat(filepath.Join(p.pgdata, "postmaster.pid"))
	return os.IsNotExist(err)
}

func retirementCanStop(st pgspec.StatusResponse, member string) bool {
	return st.FailoverFenced && st.Running && st.Role == "replica" && st.Leader != "" && st.Leader != member
}

func (e *Runner) retireStop(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths) error {
	if !s.Retiring {
		return errors.New("retirement configuration has not been published")
	}
	if !n.Stopped("patroni") {
		// Recheck under the same lock as configuration pulls and stop holds.
		// Applied nofailover prevents a subsequent election while stopping.
		st := e.status(ctx, s, p)
		if !retirementCanStop(st, s.Member) {
			return errors.New("retirement requires a fenced replica with a different leader")
		}
	}
	if err := n.Stop(ctx, "patroni"); err != nil {
		return err
	}
	// Patroni can leave PostgreSQL running, for example in maintenance mode.
	if _, err := os.Stat(filepath.Join(p.pgdata, "postmaster.pid")); err == nil {
		if _, err := command(ctx, filepath.Join(s.BinDir, "pg_ctl"), "-D", p.pgdata, "-m", "fast", "-w", "stop"); err != nil {
			return errors.New("PostgreSQL did not stop for retirement")
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	if !retirementStopped(n, s, p) {
		return errors.New("retiring database is not durably stopped")
	}
	return nil
}

func retirementRenderSpec(s pgspec.Spec, st pgspec.StatusResponse) pgspec.Spec {
	if s.Retiring {
		// Never start/reload a primary (or an unobserved member) with
		// nofailover: Patroni would suppress its physical member slots.
		s.Retiring = st.Running && st.Role == "replica" && st.Streaming && st.Leader != "" && st.Leader != s.Member
	}
	return s
}

func (e *Runner) retirementConfig(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths) pgspec.Spec {
	if !s.Retiring || retirementStopped(n, s, p) {
		return s
	}
	return retirementRenderSpec(s, e.status(ctx, s, p))
}

func (e *Runner) reloadRetirement(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths) error {
	// Apply may already have written identical files, or the member may have
	// become primary since Apply. Regenerate and reload from a fresh role view.
	s = e.retirementConfig(ctx, n, s, p)
	text, err := renderPatroni(s, p)
	if err != nil {
		return err
	}
	if err := n.WriteFile("postgres/patroni.yml", []byte(text), 0600); err != nil {
		return err
	}
	return patroniRequest(ctx, s, "POST", "/reload", map[string]any{}, nil)
}
