// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package fake

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// AdmissionMember is a container's local state, distinct from deployment
// completion and application convergence. A queued member has none of these.
type AdmissionMember struct {
	AgentRunning   bool
	Configured     bool
	ProcessStarted bool
	DNSPublished   bool
	NativeReady    bool
	DeploymentDone bool
	LocalInitError error
}

// Admission models bounded platform deployment admission and exec readiness.
// Peers are the configured bootstrap-discovery hosts. Their DNS is published
// when a container starts, never merely by importing its service. The modeled
// application waits for initial peer discovery and a running majority before
// opening its native listener. This is deliberately an adverse bootstrap state,
// not a claim that every database always needs every peer to open its listener.
// Advance is platform progress, independent of engine OpStatus polling.
type Admission struct {
	infra.Backend
	Limit int
	Peers []string
	// LegacyNative restores the old native deployment-readiness policy in tests,
	// without changing the runtime-under-test or any production source file.
	LegacyNative  bool
	Submitted     map[string]infra.RuntimeSpec
	Members       map[string]*AdmissionMember
	Commands      map[string]string
	ProbeErrors   map[string]error
	ProbeAttempts map[string]int
	pending       map[string]bool
	Peak          int
}

// LookupHost fails for imported or queued peers with no running container.
func (b *Admission) LookupHost(name string) error {
	if member := b.Members[name]; member == nil || !member.DNSPublished {
		return &net.DNSError{Name: name, Err: "no such host", IsNotFound: true}
	}
	return nil
}

func (b *Admission) nativeReadiness(name string) error {
	member := b.Members[name]
	if member != nil {
		member.NativeReady = false
	}
	if member == nil || !member.Configured {
		return fmt.Errorf("member configuration unavailable")
	}
	if !member.ProcessStarted {
		return fmt.Errorf("local process has not started")
	}
	started := 0
	for _, peer := range b.Peers {
		if err := b.LookupHost(peer); err != nil {
			return fmt.Errorf("native application waiting for bootstrap peer discovery: %w", err)
		}
		if b.Members[peer].ProcessStarted {
			started++
		}
	}
	if started < len(b.Peers)/2+1 {
		return fmt.Errorf("native application waiting for quorum")
	}
	member.NativeReady = true
	return nil
}

// readiness evaluates the rendered exec command against this member, rather
// than treating a flag's presence or another member's readiness as success.
func (b *Admission) readiness(name string) error {
	args := strings.Fields(b.Commands[name])
	if len(args) < 3 || args[1] != "agent" || args[2] != "readiness" {
		return fmt.Errorf("invalid member readiness command")
	}
	member := b.Members[name]
	if len(args) > 3 && args[3] == "--startup" {
		if len(args) != 5 || !strings.HasPrefix(args[4], "--data-dir=") || !filepath.IsAbs(strings.TrimPrefix(args[4], "--data-dir=")) {
			return fmt.Errorf("invalid startup probe arguments")
		}
		if member == nil || !member.AgentRunning {
			return fmt.Errorf("agent startup socket unavailable")
		}
		if member.LocalInitError != nil {
			return member.LocalInitError
		}
		return nil
	}
	return b.nativeReadiness(name)
}

func (b *Admission) Advance() {
	for name := range b.Members {
		_ = b.nativeReadiness(name)
	}
	for name := range b.pending {
		b.ProbeAttempts[name]++
		err := b.readiness(name)
		b.ProbeErrors[name] = err
		if err == nil {
			b.Members[name].DeploymentDone = true
			delete(b.pending, name)
		}
	}
}

func (b *Admission) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	if len(b.pending) >= b.Limit {
		return infra.Op{}, &infra.DeploymentWaitError{Reason: "DeploymentSlot", Detail: fmt.Sprintf("%d/%d in flight", len(b.pending), b.Limit), RetryAt: time.Now().Add(15 * time.Second)}
	}
	op, err := b.Backend.Deploy(ctx, name, d)
	if err != nil {
		return op, err
	}
	if b.Submitted == nil {
		b.Submitted = make(map[string]infra.RuntimeSpec)
		b.Members = make(map[string]*AdmissionMember)
		b.Commands = make(map[string]string)
		b.ProbeErrors = make(map[string]error)
		b.ProbeAttempts = make(map[string]int)
		b.pending = make(map[string]bool)
	}
	b.Submitted[name] = d.Runtime
	b.Members[name] = &AdmissionMember{AgentRunning: true, Configured: true, ProcessStarted: true, DNSPublished: true}
	args := d.Runtime.Readiness.Args
	if b.LegacyNative {
		args = []string{"agent", "readiness"}
		if engine := d.Runtime.Env["KARAKORAM_ENGINE"]; engine != "" {
			args = append(args, "--engine="+engine)
		}
		args = append(args, "--data-dir="+d.Runtime.Env["KARAKORAM_DATA_DIR"])
	}
	b.Commands[name] = d.Artifact.Binary + " " + strings.Join(args, " ")
	b.pending[name] = true
	b.Peak = max(b.Peak, len(b.pending))
	return infra.Op{ID: "fake-admission-" + name, Kind: "fake-admission", Hostname: name}, nil
}

func (b *Admission) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "fake-admission" {
		return infra.OpState{Done: !b.pending[op.Hostname]}, nil
	}
	return b.Backend.OpStatus(ctx, op)
}

func (b *Admission) GetService(ctx context.Context, name string) (*infra.Service, error) {
	service, err := b.Backend.GetService(ctx, name)
	if service == nil || err != nil || !b.pending[name] {
		return service, err
	}
	copy := *service
	copy.Status = infra.StatusCreating
	return &copy, nil
}

// Ready requires both actual per-member probe execution and native convergence.
// A completed deployment alone is never evidence of a healthy application.
func (b *Admission) Ready() bool {
	if len(b.Members) == 0 {
		return false
	}
	for name, member := range b.Members {
		if !member.DeploymentDone || !member.NativeReady || b.ProbeAttempts[name] == 0 || b.ProbeErrors[name] != nil {
			return false
		}
	}
	return true
}
