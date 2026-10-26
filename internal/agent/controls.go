// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

// controlMutex preserves zero-value mutex use while supporting bounded route waits.
type controlMutex struct {
	once sync.Once
	held chan struct{}
}

func (m *controlMutex) init() {
	m.once.Do(func() { m.held = make(chan struct{}, 1) })
}
func (m *controlMutex) Lock() {
	m.init()
	m.held <- struct{}{}
}
func (m *controlMutex) LockContext(ctx context.Context) error {
	m.init()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.held <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.held
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *controlMutex) Unlock() {
	select {
	case <-m.held:
	default:
		panic("unlock of unlocked control mutex")
	}
}

// localControls preserves explicit restore safety holds independently of pulled configuration.
// Only an explicit maintenance-off or process start/restart releases its respective hold.
type localControls struct {
	Maintenance bool            `json:"maintenance"`
	Stopped     map[string]bool `json:"stopped,omitempty"`
}

// An initializing marker carries the exact first intent, so interrupted initial
// publication can resume without inventing empty controls over a safety hold.
type controlsInitialization struct {
	Phase    string        `json:"phase"`
	Controls localControls `json:"controls"`
}

func loadControls(dir string) (localControls, error) {
	var controls localControls
	data, err := os.ReadFile(filepath.Join(dir, "karakoram", "controls.json"))
	if os.IsNotExist(err) {
		marker, markerErr := os.ReadFile(filepath.Join(dir, "karakoram", "controls.initialized"))
		if markerErr == nil {
			var pending controlsInitialization
			if json.Unmarshal(marker, &pending) == nil && pending.Phase == "initializing" {
				return pending.Controls, nil
			}
			return controls, errors.New("local safety controls missing on an initialized member; recover controls.json from trusted storage before starting processes")
		}
		if !os.IsNotExist(markerErr) {
			return controls, markerErr
		}
		// Base-format members published only config.json during ordinary apply.
		// Existing explicit controls take precedence above; cached maintenance is
		// conservatively retained during the one-time legacy migration.
		cache, cacheErr := os.ReadFile(filepath.Join(dir, "karakoram", "config.json"))
		if os.IsNotExist(cacheErr) {
			return controls, nil
		}
		if cacheErr != nil {
			return controls, cacheErr
		}
		var legacy agentapi.MemberConfig
		if err := json.Unmarshal(cache, &legacy); err != nil {
			return controls, fmt.Errorf("cannot migrate legacy safety controls from configuration: %w", err)
		}
		controls.Maintenance = legacy.Maintenance
		return controls, nil
	}
	if err != nil {
		return controls, err
	}
	if err := json.Unmarshal(data, &controls); err != nil {
		return controls, fmt.Errorf("local safety controls corrupt; recover controls.json from trusted storage before starting processes: %w", err)
	}
	return controls, nil
}
func (a *nodeAgent) saveControls(controls localControls) error {
	data, err := json.Marshal(controls)
	if err != nil {
		return err
	}
	marker := filepath.Join(a.o.DataDir, "karakoram", "controls.initialized")
	existingMarker, markerErr := os.ReadFile(marker)
	var pendingState controlsInitialization
	initializing := markerErr == nil && json.Unmarshal(existingMarker, &pendingState) == nil && pendingState.Phase == "initializing"
	if os.IsNotExist(markerErr) || initializing {
		pending, err := json.Marshal(controlsInitialization{Phase: "initializing", Controls: controls})
		if err != nil {
			return err
		}
		if err := atomicWrite(marker, pending, 0600); err != nil {
			return err
		}
	} else if markerErr != nil {
		return markerErr
	}
	if err = atomicWrite(filepath.Join(a.o.DataDir, "karakoram", "controls.json"), data, 0600); err != nil {
		return err
	}
	if err := atomicWrite(marker, []byte("initialized\n"), 0600); err != nil {
		return err
	}
	a.controlsMu.Lock()
	a.controls = controls
	a.controlsMu.Unlock()
	return nil
}
func (a *nodeAgent) processAction(ctx context.Context, name, action string) error {
	if err := a.applyMu.LockContext(ctx); err != nil {
		return err
	}
	defer a.applyMu.Unlock()
	return a.processActionLocked(ctx, name, action)
}

func (a *nodeAgent) processActionLocked(ctx context.Context, name, action string) error {
	if err := a.admitProcessAction(ctx, name, action); err != nil {
		return err
	}
	if err := a.setProcessStopped(name, action == "stop"); err != nil {
		return err
	}
	return a.sup.action(ctx, name, action)
}

// Admission performs no control or process mutation. Retryable refusals must
// happen before durable restart execution is accepted.
func (a *nodeAgent) admitProcessAction(ctx context.Context, name, action string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.controlsErr != nil {
		return a.controlsErr
	}
	if !a.allowedProcess(name) {
		return errors.New("unknown process")
	}
	if action != "start" && action != "stop" && action != "restart" {
		return errors.New("invalid process action")
	}
	states, _ := a.sup.snapshot()
	if _, ok := states[name]; !ok {
		return errors.New("process is not configured")
	}
	if action != "stop" {
		c := a.config()
		if err := (&Node{a: a, applyConfig: &c}).beforeStart(ctx, name); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

// setProcessStopped is called with applyMu held, before changing process state.
func (a *nodeAgent) setProcessStopped(name string, stopped bool) error {
	controls := localControls{Maintenance: a.controls.Maintenance, Stopped: make(map[string]bool, len(a.controls.Stopped)+1)}
	for process, held := range a.controls.Stopped {
		controls.Stopped[process] = held
	}
	if stopped {
		controls.Stopped[name] = true
	} else {
		delete(controls.Stopped, name)
	}
	return a.saveControls(controls)
}
func (a *nodeAgent) maintenance(enabled bool) error {
	return a.maintenanceContext(context.Background(), enabled)
}

func (a *nodeAgent) maintenanceContext(ctx context.Context, enabled bool) error {
	if err := a.applyMu.LockContext(ctx); err != nil {
		return err
	}
	defer a.applyMu.Unlock()
	if a.controlsErr != nil {
		return a.controlsErr
	}
	controls := a.controls
	controls.Maintenance = enabled
	if err := a.saveControls(controls); err != nil {
		return err
	}
	c := a.config()
	c.Maintenance = enabled
	a.mu.Lock()
	a.c = c
	a.mu.Unlock()
	// Apply the safety hold even when updating the separate config cache fails.
	a.sup.setMaintenance(enabled)
	return cacheConfig(c, a.o.DataDir)
}
