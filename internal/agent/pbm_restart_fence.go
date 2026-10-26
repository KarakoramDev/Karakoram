// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func (a *nodeAgent) preparePBMRestartFence(path string, receipt *restartReceipt) error {
	stamp := time.Now().UTC()
	if stamp.Before(time.Unix(receipt.Request.RestoreCommandTS, 0).Add(agentapi.PBMRestoreFenceClockMargin)) {
		return errors.New("PBM restore fence: member clock is behind command time plus safety margin; old process remains stopped")
	}
	// This runs only after supervisor stopChild observed the old process wait.
	// Persist before launching; a failed guard must not allow autonomous starts.
	receipt.FenceStartedAfter = stamp
	if err := writeRestartReceipt(path, *receipt); err != nil {
		return err
	}
	return a.setProcessStopped(receipt.Request.Process, false)
}

func (a *nodeAgent) confirmPBMRestartFence(receipt *restartReceipt) error {
	a.sup.mu.Lock()
	defer a.sup.mu.Unlock()
	p := a.sup.children[receipt.Request.Process]
	if p == nil || p.cmd == nil || receipt.FenceStartedAfter.IsZero() {
		return errors.New("PBM restore fence: replacement start unconfirmed")
	}
	owner, state, err := processIdentity(p.cmd.Process.Pid)
	if err != nil || state == "T" || state == "t" || state == "Z" || state == "X" || owner == receipt.Old {
		return errors.New("PBM restore fence: old process exit or running replacement unconfirmed")
	}
	stamp := time.Now().UTC()
	if stamp.Before(time.Unix(receipt.Request.RestoreCommandTS, 0).Add(agentapi.PBMRestoreFenceClockMargin)) {
		return errors.New("PBM restore fence: member clock moved behind command safety margin")
	}
	receipt.New = owner
	receipt.PBMFence = &agentapi.PBMRestartFence{OldExited: true, PID: owner.PID, StartedAfter: receipt.FenceStartedAfter, MemberTime: stamp}
	return nil
}

func (a *nodeAgent) validatePBMRestartFence(request agentapi.ProcessAction, receipt restartReceipt) error {
	if receipt.Session != a.sup.session || receipt.PBMFence == nil || !receipt.PBMFence.OldExited {
		return errors.New("PBM restore fence: supervisor session or exit confirmation unavailable")
	}
	a.sup.mu.Lock()
	defer a.sup.mu.Unlock()
	p := a.sup.children[request.Process]
	if p == nil || p.cmd == nil {
		return errors.New("PBM restore fence: replacement no longer running")
	}
	owner, state, err := processIdentity(p.cmd.Process.Pid)
	if err != nil || owner != receipt.New || state == "T" || state == "t" || state == "Z" || state == "X" {
		return errors.New("PBM restore fence: confirmed replacement incarnation changed")
	}
	minimum := time.Unix(request.RestoreCommandTS, 0).Add(agentapi.PBMRestoreFenceClockMargin)
	if receipt.FenceStartedAfter.Before(minimum) || time.Now().UTC().Before(minimum) {
		return errors.New("PBM restore fence: member clock is behind command safety margin")
	}
	return nil
}

func (a *nodeAgent) resumePBMRestartFence(ctx context.Context, path string, receipt *restartReceipt) error {
	a.sup.mu.Lock()
	p := a.sup.children[receipt.Request.Process]
	if p == nil {
		a.sup.mu.Unlock()
		return errors.New("PBM restore fence: process not configured")
	}
	if p.cmd != nil {
		a.sup.mu.Unlock()
		return errors.New("PBM restore fence: pending receipt cannot confirm an old exit and guarded replacement; preserve fence")
	}
	a.sup.mu.Unlock()
	// p.cmd becomes nil only after cmd.Wait in this same supervisor session.
	if err := a.preparePBMRestartFence(path, receipt); err != nil {
		return err
	}
	if err := a.sup.action(ctx, receipt.Request.Process, "start"); err != nil {
		return err
	}
	if err := a.confirmPBMRestartFence(receipt); err != nil {
		return err
	}
	receipt.Complete = true
	return writeRestartReceipt(path, *receipt)
}

func (a *nodeAgent) processActionResult(request agentapi.ProcessAction) (agentapi.ActionResponse, error) {
	out := agentapi.ActionResponse{Success: true}
	if request.RestoreCommandTS == 0 {
		return out, nil
	}
	digest := sha256.Sum256([]byte(request.RequestID))
	path := filepath.Join(a.o.DataDir, "karakoram", "restarts", hex.EncodeToString(digest[:])+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return agentapi.ActionResponse{}, err
	}
	var receipt restartReceipt
	if json.Unmarshal(data, &receipt) != nil || !receipt.Complete || receipt.Request != request {
		return agentapi.ActionResponse{}, errors.New("PBM restore fence: durable completion receipt unavailable")
	}
	if err := a.validatePBMRestartFence(request, receipt); err != nil {
		return agentapi.ActionResponse{}, err
	}
	proof := *receipt.PBMFence
	proof.MemberTime = time.Now().UTC()
	out.PBMFence = &proof
	return out, nil
}

// applyMu is held. An unknown fence must survive config pulls and autonomous
// backoff; only a positively guarded launch may release this local hold.
func (a *nodeAgent) holdPBMRestartFence() error {
	err := a.setProcessStopped("pbm-agent", true)
	a.sup.mu.Lock()
	if process := a.sup.children["pbm-agent"]; process != nil {
		process.desired = false
		process.generation++
	}
	a.sup.mu.Unlock()
	return err
}
