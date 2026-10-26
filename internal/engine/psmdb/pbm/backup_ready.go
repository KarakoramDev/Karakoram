// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

const backupAgentReadyTimeout = 2 * time.Minute

// Configuration acceptance/resync does not refresh PBM's agent health records.
// Nomination excludes unhealthy agents, even on a writable single-member set.
func waitBackupAgents(ctx context.Context, env Env, op *domain.Operation, cp checkpoint) (engine.OpResult, error) {
	if cp.BackupReadyStartedElapsed == nil {
		elapsed := op.Elapsed
		cp.BackupReadyStartedElapsed = &elapsed
	}
	out, err := command(ctx, env, cp.Member, "", "status", nil, "--out=json")
	var status struct {
		Cluster []struct {
			RS    string `json:"rs"`
			Nodes []struct {
				Host string `json:"host"`
				Role string `json:"role"`
				OK   bool   `json:"ok"`
			} `json:"nodes"`
		} `json:"cluster"`
	}
	if err == nil {
		err = decodeLast(out, &status)
	}
	expected := make(map[string]map[string]bool)
	for _, n := range env.Nodes {
		if expected[n.ReplSet] == nil {
			expected[n.ReplSet] = make(map[string]bool)
		}
		expected[n.ReplSet][n.Member] = true
	}
	ready := make(map[string]bool)
	if err == nil {
		for _, rs := range status.Cluster {
			for _, node := range rs.Nodes {
				host, _, e := net.SplitHostPort(node.Host)
				if e == nil && expected[rs.RS][host] && node.OK && (node.Role == "P" || node.Role == "S") {
					ready[rs.RS] = true
				}
			}
		}
	}
	var missing []string
	for rs := range expected {
		if !ready[rs] {
			missing = append(missing, rs)
		}
	}
	sort.Strings(missing)
	if err == nil && len(expected) > 0 && len(missing) == 0 {
		cp.Step = "start"
		return result(cp, false), nil
	}
	message := "waiting for PBM backup-eligible agents after storage configuration; replica sets: " + strings.Join(missing, ", ")
	if err != nil {
		// Do not expose unbounded native output or consume ordinary retry budget.
		message = "waiting for PBM backup-eligible agents: native status is unavailable or invalid"
	}
	if op.Elapsed-*cp.BackupReadyStartedElapsed >= backupAgentReadyTimeout {
		copyOp := *op
		copyOp.LastError = "PBM backup was not initiated"
		return failBackupObservation(ctx, env, &copyOp, cp, message+"; two-minute observed-execution readiness budget expired")
	}
	res := result(cp, false)
	res.Message = message
	return res, nil
}

// A failed start receipt is immutable. Retrying it cannot repair initiation and
// switching identity could duplicate a command whose response was lost.
func failBackupStart(ctx context.Context, env Env, op *domain.Operation, cp checkpoint, cause error) (engine.OpResult, error) {
	copyOp := *op
	copyOp.LastError = fmt.Sprintf("PBM backup initiation outcome unconfirmed: %v", cause)
	return failBackupObservation(ctx, env, &copyOp, cp, "no operation-specific native identity was returned; no automatic re-initiation will be attempted")
}
