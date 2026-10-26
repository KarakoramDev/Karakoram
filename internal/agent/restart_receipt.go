// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

type restartReceipt struct {
	Request           agentapi.ProcessAction    `json:"request"`
	Complete          bool                      `json:"complete"`
	Error             string                    `json:"error,omitempty"` // Legacy receipts only.
	Session           string                    `json:"session,omitempty"`
	Old               processOwner              `json:"old,omitempty"`
	New               processOwner              `json:"new,omitempty"`
	PBMFence          *agentapi.PBMRestartFence `json:"pbmFence,omitempty"`
	FenceStartedAfter time.Time                 `json:"fenceStartedAfter,omitempty"`
}

func writeRestartReceipt(path string, receipt restartReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	return atomicWrite(path, data, 0600)
}

func (a *nodeAgent) processActionRequest(ctx context.Context, request agentapi.ProcessAction) (err error) {
	if request.RestoreCommandTS != 0 && (request.RestoreCommandTS < 0 || request.Process != "pbm-agent" || request.Action != "restart" || request.RequestID == "") {
		return errors.New("PBM restore fence requires an identified pbm-agent restart")
	}
	// Older control planes do not supply identities. Keep their wire protocol;
	// only identified restarts promise replay safety.
	if request.RequestID == "" || request.Action != "restart" {
		return a.processAction(ctx, request.Process, request.Action)
	}
	if len(request.RequestID) > 512 {
		return errors.New("restart request identity exceeds 512 bytes")
	}
	if len(request.Process) > 128 || !a.allowedProcess(request.Process) {
		return errors.New("unknown process")
	}
	if err := a.applyMu.LockContext(ctx); err != nil {
		return err
	}
	defer a.applyMu.Unlock()
	if request.RestoreCommandTS != 0 {
		defer func() {
			if err != nil {
				err = errors.Join(err, a.holdPBMRestartFence())
			}
		}()
	}
	digest := sha256.Sum256([]byte(request.RequestID))
	dir := filepath.Join(a.o.DataDir, "karakoram", "restarts")
	path := filepath.Join(dir, hex.EncodeToString(digest[:])+".json")
	data, err := os.ReadFile(path)
	if err == nil {
		var receipt restartReceipt
		if json.Unmarshal(data, &receipt) != nil || receipt.Request != request {
			return errors.New("restart receipt corrupt or identity reused for a different action; reconcile before retrying")
		}
		if receipt.Complete {
			if receipt.Error != "" {
				return errors.New(receipt.Error)
			}
			if request.RestoreCommandTS != 0 {
				return a.validatePBMRestartFence(request, receipt)
			}
			return nil
		}
		// Only this supervisor can attribute its autonomous replacement to the
		// accepted stop. A crash between journaling and signalling remains unknown.
		if receipt.Session == "" || receipt.Session != a.sup.session {
			return errors.New("restart outcome unknown after supervisor interruption; reconcile the recorded process generation before retrying")
		}
		if request.RestoreCommandTS != 0 {
			return a.resumePBMRestartFence(ctx, path, &receipt)
		}
		a.sup.mu.Lock()
		p := a.sup.children[request.Process]
		converged := false
		if p != nil && p.cmd != nil {
			owner, state, identityErr := processIdentity(p.cmd.Process.Pid)
			converged = identityErr == nil && state != "T" && state != "t" && state != "Z" && state != "X" && owner != receipt.Old
		}
		a.sup.mu.Unlock()
		if !converged {
			return errors.New("accepted restart is awaiting its replacement process generation; retry the same identity after recovery")
		}
		receipt.Complete = true
		return writeRestartReceipt(path, receipt)
	}
	if !os.IsNotExist(err) {
		return err
	}
	filter, err := loadRestartRetirement(dir)
	if err != nil {
		return err
	}
	if filter != nil && retiredRestart(filter, digest, false) {
		return errors.New("restart identity retired (or conservatively fenced by retirement history); stale retries must not execute again")
	}
	// Preparation, cancellation and controls failures have no process effect and
	// must not install an execution receipt that permanently caches a refusal.
	if err := a.admitProcessAction(ctx, request.Process, request.Action); err != nil {
		return err
	}
	if err := reserveRestartReceipt(dir, digest); err != nil {
		return err
	}
	if err := a.setProcessStopped(request.Process, request.RestoreCommandTS != 0); err != nil {
		return err
	}
	receipt := restartReceipt{Request: request, Session: a.sup.session}
	var beforeStart func() error
	if request.RestoreCommandTS != 0 {
		beforeStart = func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			return a.preparePBMRestartFence(path, &receipt)
		}
	}
	actionErr := a.sup.actionWithAdmission(ctx, request.Process, request.Action, func(p *child) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.cmd != nil {
			owner, _, err := processIdentity(p.cmd.Process.Pid)
			if err != nil {
				return fmt.Errorf("identify restart's original process: %w", err)
			}
			receipt.Old = owner
		}
		if err := writeRestartReceipt(path, receipt); err != nil {
			return fmt.Errorf("persist restart intent: %w", err)
		}
		return nil
	}, beforeStart)
	if actionErr != nil {
		// An accepted stop/start may converge through the supervisor after a
		// timeout or failed launch. Keep it pending, never replay its stop.
		return actionErr
	}
	if request.RestoreCommandTS != 0 {
		if err := a.confirmPBMRestartFence(&receipt); err != nil {
			return err
		}
	}
	receipt.Complete = true
	if err := writeRestartReceipt(path, receipt); err != nil {
		return fmt.Errorf("restart outcome pending; cannot persist receipt: %w", err)
	}
	return nil
}
