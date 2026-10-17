// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
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

func loadControls(dir string) (localControls, error) {
	var controls localControls
	data, err := os.ReadFile(filepath.Join(dir, "karakoram", "controls.json"))
	if os.IsNotExist(err) {
		return controls, nil
	}
	if err != nil {
		return controls, err
	}
	err = json.Unmarshal(data, &controls)
	return controls, err
}
func (a *nodeAgent) saveControls(controls localControls) error {
	data, err := json.Marshal(controls)
	if err != nil {
		return err
	}
	if err = atomicWrite(filepath.Join(a.o.DataDir, "karakoram", "controls.json"), data, 0600); err != nil {
		return err
	}
	a.controlsMu.Lock()
	a.controls = controls
	a.controlsMu.Unlock()
	return nil
}
func (a *nodeAgent) processAction(ctx context.Context, name, action string) error {
	a.applyMu.Lock()
	defer a.applyMu.Unlock()
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
	if err := a.setProcessStopped(name, action == "stop"); err != nil {
		return err
	}
	return a.sup.action(ctx, name, action)
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
	a.applyMu.Lock()
	defer a.applyMu.Unlock()
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
