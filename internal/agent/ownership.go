// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type processOwner struct {
	PID     int    `json:"pid"`
	Start   string `json:"start"`
	Boot    string `json:"boot"`
	Pending bool   `json:"pending,omitempty"`
}

func processIdentity(pid int) (processOwner, string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return processOwner{}, "", err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return processOwner{}, "", errors.New("invalid process identity")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return processOwner{}, "", errors.New("incomplete process identity")
	}
	boot, err := processBootIdentity()
	if err != nil {
		return processOwner{}, "", err
	}
	return processOwner{PID: pid, Start: fields[19], Boot: boot}, fields[0], nil
}

func validBootIdentity(boot string) bool {
	if len(boot) != 36 {
		return false
	}
	for i := range len(boot) {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if boot[i] != '-' {
				return false
			}
		} else if !(boot[i] >= '0' && boot[i] <= '9' || boot[i] >= 'a' && boot[i] <= 'f') {
			return false
		}
	}
	return true
}

func processBootIdentity() (string, error) {
	data, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	boot := strings.TrimSpace(string(data))
	if !validBootIdentity(boot) {
		return "", errors.New("invalid kernel boot identity")
	}
	return boot, nil
}

func processStopped(pid int) bool {
	_, state, err := processIdentity(pid)
	return err == nil && (state == "T" || state == "t")
}

// A surviving child is not adopted on inference. Keep the new supervisor fenced
// until the exact previous owner exits or an operator safely stops it.
func (s *supervisor) acquireProcess(name string) (func(), error) {
	if s.ownershipDir == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(s.ownershipDir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(s.ownershipDir, name+".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	release := func() { lock.Close() }
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		release()
		return nil, fmt.Errorf("process ownership is being reconciled: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(s.ownershipDir, name+".json"))
	if os.IsNotExist(err) {
		return release, nil
	}
	if err != nil {
		release()
		return nil, err
	}
	var owner processOwner
	if err := json.Unmarshal(data, &owner); err != nil {
		release()
		return nil, fmt.Errorf("process ownership record corrupt; reconcile surviving processes before recovery: %w", err)
	}
	if owner.Pending {
		boot, err := processBootIdentity()
		if err != nil {
			release()
			return nil, fmt.Errorf("cannot verify interrupted launch boot identity: %w", err)
		}
		// A new kernel boot positively excludes every prior child, including
		// one spawned before its PID receipt could be published. Same-boot
		// and legacy ambiguous launches still require explicit reconciliation.
		if validBootIdentity(owner.Boot) && owner.Boot != boot {
			return release, nil
		}
		release()
		return nil, errors.New("process launch outcome unknown after supervisor interruption; reconcile surviving processes before clearing the ownership record")
	}
	current, state, err := processIdentity(owner.PID)
	if err != nil && !os.IsNotExist(err) {
		release()
		return nil, fmt.Errorf("cannot verify prior process ownership: %w", err)
	}
	if err == nil && current == owner && state != "Z" && state != "X" {
		release()
		return nil, fmt.Errorf("unmanaged process conflict: %s PID %d survives the previous supervisor; safely stop that verified owner before retrying", name, owner.PID)
	}
	return release, nil
}

func (s *supervisor) recordProcessIntent(name string) error {
	if s.ownershipDir == "" {
		return nil
	}
	boot, err := processBootIdentity()
	if err != nil {
		return err
	}
	return s.writeProcessOwner(name, processOwner{Pending: true, Boot: boot})
}

func (s *supervisor) recordProcess(name string, pid int) error {
	if s.ownershipDir == "" {
		return nil
	}
	owner, _, err := processIdentity(pid)
	if err != nil {
		return err
	}
	return s.writeProcessOwner(name, owner)
}

func (s *supervisor) writeProcessOwner(name string, owner processOwner) error {
	if s.ownershipDir == "" {
		return nil
	}
	data, err := json.Marshal(owner)
	if err != nil {
		return err
	}
	return atomicWrite(filepath.Join(s.ownershipDir, name+".json"), data, 0600)
}
