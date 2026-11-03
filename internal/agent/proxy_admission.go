// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// This is deliberately outside DataDir: /tmp belongs to the managed Dokploy
// container's writable layer and is never mounted from an engine data volume.
// Provenance also prevents reuse if that layer is copied or the container restarts.
type proxyContainerAdmission struct {
	path     string
	identity string
}

func proxyAdmissionPath(dir string) string {
	return fmt.Sprintf("/tmp/.karakoram-proxy-admission-%d-%x", os.Getuid(), sha256.Sum256([]byte(filepath.Clean(dir))))
}

func proxyAdmissionForContainer(dir string) (proxyContainerAdmission, error) {
	init, _, err := processIdentity(1)
	if err != nil {
		return proxyContainerAdmission{}, err
	}
	namespace, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		return proxyContainerAdmission{}, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return proxyContainerAdmission{}, err
	}
	// PID1 survives an agent child restart/re-exec. All components are stable
	// within that container lifetime and change for a fresh task/PID namespace.
	identity := fmt.Sprintf("v1:%x\n", sha256.Sum256([]byte(init.Boot+"\x00"+init.Start+"\x00"+namespace+"\x00"+hostname)))
	return proxyContainerAdmission{path: proxyAdmissionPath(dir), identity: identity}, nil
}

func (p proxyContainerAdmission) admitted() (bool, error) {
	data, err := os.ReadFile(p.path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return string(data) == p.identity, nil
}

func (p proxyContainerAdmission) record() error {
	file, err := os.CreateTemp(filepath.Dir(p.path), ".karakoram-proxy-admission-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if _, err = file.WriteString(p.identity); err != nil {
		_ = file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	// Atomic publication prevents an agent-only restart from reading a partial
	// successful-admission record. No process credentials are stored here.
	return os.Rename(name, p.path)
}
