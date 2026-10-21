// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

type jobReceipt struct {
	RequestHash string `json:"requestHash"`
	neo4jspec.JobResponse
	// Swap is persisted before each rename; recovery rolls back unfinished loads.
	Swap           string `json:"swap,omitempty"`
	Stage          string `json:"stage,omitempty"`
	Old            string `json:"old,omitempty"`
	ReleasePending bool   `json:"releasePending,omitempty"`
	held           bool   // Runtime admission only; interrupted receipts always release holds.
}

func durableJSON(name string, value any) error {
	return durableJSONWithSync(name, value, syncDir)
}

func durableJSONWithSync(name string, value any, syncParent func(string) error) error {
	if err := os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(name), ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), name); err != nil {
		return err
	}
	return syncParent(filepath.Dir(name))
}
func syncDir(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func jobReceiptPath(root, kind, id string) (string, error) {
	if id == "" || len(id) > 256 || strings.ContainsAny(id, "\x00\r\n") {
		return "", errors.New("durable operation receipt required")
	}
	return filepath.Join(root, "receipts", kind, fmt.Sprintf("%x.json", sha256.Sum256([]byte(id)))), nil
}
func requestHash(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(b)), nil
}
func readReceipt(path string) (*jobReceipt, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r jobReceipt
	err = json.Unmarshal(b, &r)
	return &r, err
}

// interruptReceipt publishes failure only after the member's durable stop hold
// has been released. A failed release leaves the running journal retryable.
func interruptReceipt(name string, r *jobReceipt, start func() error) error {
	if err := start(); err != nil {
		return err
	}
	if r.State == neo4jspec.JobRunning || r.ReleasePending {
		r.State = neo4jspec.JobFailed
		r.Phase = ""
		r.Message = "interrupted; retry with a new operation"
	}
	r.ReleasePending = false
	r.held = false
	return durableJSON(name, r)
}

// releaseJobHold journals release intent before attempting it. Retry latency is
// capped, and the caller's deadline bounds the complete release attempt.
func releaseJobHold(ctx context.Context, name string, r *jobReceipt, start func(context.Context) error) error {
	r.ReleasePending = true
	r.State = neo4jspec.JobRunning
	r.Phase = neo4jspec.PhaseStarting
	if err := durableJSON(name, r); err != nil {
		return err
	}
	delay := 250 * time.Millisecond
	for {
		if err := start(ctx); err == nil {
			r.held = false
			r.ReleasePending = false
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(2*delay, 5*time.Second)
	}
}

func finishJobReceipt(name string, r *jobReceipt, jobErr error) error {
	if r.held || r.ReleasePending {
		r.ReleasePending = true
		r.State = neo4jspec.JobRunning
		r.Phase = neo4jspec.PhaseStarting
		r.Message = "server restart pending"
	} else {
		r.Phase = ""
		r.State = neo4jspec.JobDone
		r.Message = ""
		if jobErr != nil {
			r.State = neo4jspec.JobFailed
			r.Message = "offline operation failed; retry with a new operation"
		}
	}
	return durableJSON(name, r)
}
