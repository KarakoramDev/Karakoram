// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"
)

// Layout describes where a backend's runtimes place the deployed artifact and
// mounted volumes, and which unprivileged user runs the start command.
// Prepare commands may use passwordless sudo on every backend.
type Layout struct {
	// AppDir is the working directory holding the extracted artifact.
	AppDir string `json:"appDir"`
	// VolumeRoot is the directory under which volumes mount by hostname.
	VolumeRoot string `json:"volumeRoot"`
	// User owns AppDir and runs the start command.
	User string `json:"user"`
}

// VolumePath returns the mount path of a volume service.
func (l Layout) VolumePath(volume string) string { return path.Join(l.VolumeRoot, volume) }

// DataDir returns the persistent data directory of a member: its volume
// mount, or a directory inside AppDir for stateless services.
func (l Layout) DataDir(volume string) string {
	if volume == "" {
		return path.Join(l.AppDir, "karakoram-data")
	}
	return l.VolumePath(volume)
}

// Artifact is the deployable release archive: a tar.gz whose root contains
// the executable Binary. Reader is consumed once.
type Artifact struct {
	Reader io.Reader
	// Name is the archive file name, e.g. "karakoram.tar.gz".
	Name string
	// Binary is the executable inside the archive, e.g. "karakoram".
	Binary string
}

// Port is a container port. HTTP marks a port eligible for public HTTPS
// routing (SubdomainAccess).
type Port struct {
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
	HTTP     bool   `json:"http,omitempty"`
}

// Readiness runs the artifact binary with Args until it exits 0, retrying
// every RetryPeriod up to FailureTimeout after each (re)start.
type Readiness struct {
	Args           []string      `json:"args"`
	FailureTimeout time.Duration `json:"failureTimeout"`
	RetryPeriod    time.Duration `json:"retryPeriod"`
}

// RuntimeSpec is the neutral description of what a runtime service runs.
// Backends render it into their own manifests (zerops.yaml, Swarm service,
// Nomad job). Env holds non-secret values only; secrets use ServiceSpec.Env
// or SetEnvSecrets.
type RuntimeSpec struct {
	Base string `json:"base"`
	// Prepare holds shell commands run, in order, once per deployment before
	// the start command, as Layout.User with passwordless sudo.
	Prepare []string          `json:"prepare,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	// Args are the arguments of the artifact binary started in AppDir.
	Args  []string `json:"args"`
	Ports []Port   `json:"ports,omitempty"`
	// Volume mounts this volume service at Layout.VolumePath(Volume).
	Volume    string     `json:"volume,omitempty"`
	Readiness *Readiness `json:"readiness,omitempty"`
	// StopBeforeStart stops the previous instance before the new one starts,
	// so two processes never write the same volume.
	StopBeforeStart bool `json:"stopBeforeStart"`
}

// Deployment ships the artifact with its runtime description.
type Deployment struct {
	Artifact Artifact
	Runtime  RuntimeSpec
}

// Validate rejects a deployment no backend can render safely.
func (d Deployment) Validate() error {
	if d.Artifact.Reader == nil || d.Artifact.Name == "" || !safeWord(d.Artifact.Binary) {
		return errors.New("deployment requires an artifact archive and binary name")
	}
	r := d.Runtime
	if r.Base == "" || len(r.Args) == 0 {
		return errors.New("deployment requires a base image and start arguments")
	}
	args := r.Args
	if r.Readiness != nil {
		if len(r.Readiness.Args) == 0 || r.Readiness.FailureTimeout < time.Second || r.Readiness.FailureTimeout%time.Second != 0 || r.Readiness.RetryPeriod < time.Second || r.Readiness.RetryPeriod%time.Second != 0 {
			return errors.New("deployment readiness requires arguments and whole-second timings of at least one second")
		}
		args = append(append([]string{}, args...), r.Readiness.Args...)
	}
	for _, a := range args {
		if !safeWord(a) {
			return fmt.Errorf("deployment argument %q must be a non-empty word without whitespace or shell syntax", a)
		}
	}
	for _, p := range r.Ports {
		if p.Port < 1 || p.Port > 65535 || (p.Protocol != "tcp" && p.Protocol != "udp") {
			return fmt.Errorf("invalid deployment port %d/%s", p.Port, p.Protocol)
		}
	}
	return nil
}

// safeWord reports whether s can be joined into a command line unquoted.
func safeWord(s string) bool {
	return s != "" && !strings.ContainsAny(s, " \t\r\n'\"`$;&|<>()\\*?![]{}#~")
}
