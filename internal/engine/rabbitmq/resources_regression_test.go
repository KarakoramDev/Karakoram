// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

func TestResourceChangesRespectDisruptionBoundaries(t *testing.T) {
	for _, kind := range []string{"cpu", "cpuMode", "ram", "disk"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _, a := setup(t)
			ready(t, p, rc)
			a.log.clear()
			r := &rc.Cluster.Spec.Components.Engine.Resources
			switch kind {
			case "cpu":
				r.MinCPU, r.MaxCPU = 2, 2
			case "cpuMode":
				r.CPUMode = "DEDICATED"
			case "ram":
				r.MinRAM, r.MaxRAM = 2, 2
			case "disk":
				r.MinDisk, r.MaxDisk = 10, 10
			}
			// Resource snapshots, not only member stage/version, determine convergence.
			syncUntil(t, p, rc, func(s *internalState) bool {
				if s.Phase != "ready" || s.Pending != nil || s.Retry != nil {
					return false
				}
				for _, m := range s.Members {
					if m.ChangeStep != "" || m.ConfigResources != runtimeResources(*r) || m.ConfigDisk.MinDisk != r.MinDisk || m.ConfigDisk.MaxDisk != r.MaxDisk {
						return false
					}
				}
				return true
			})
			calls := a.log.snapshot()
			for i, m := range state(t, rc).Members {
				target := m.Name
				if kind == "disk" {
					target = m.Volume
				}
				resources := "infra:" + target + ":resources"
				drain := "agent:" + m.Name + ":drain"
				stop := "agent:" + m.Name + ":stop"
				deploy := "infra:" + m.Name + ":deploy"
				assertBefore(t, calls, drain, stop)
				assertBefore(t, calls, stop, deploy)
				if kind == "cpu" || kind == "cpuMode" {
					assertBefore(t, calls, stop, resources)
					assertBefore(t, calls, resources, deploy)
				} else {
					assertBefore(t, calls, resources, drain)
				}
				if i > 0 {
					assertBefore(t, calls, fmt.Sprintf("agent:demom%d:revive", i-1), drain)
				}
				if countCall(calls, deploy) != 1 || countCall(calls, resources) != 1 {
					t.Fatalf("resource change replayed: %v", calls)
				}
				cfg, err := p.MemberConfig(context.Background(), rc, m.Name)
				if err != nil {
					t.Fatal(err)
				}
				var spec rmqspec.MemberSpec
				if err := json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
					t.Fatal(err)
				}
				if spec.TotalMemoryBytes != int64(r.MinRAM*1e9) || spec.MemoryHighWatermarkBytes != int64(r.MinRAM*1e9*0.6) || spec.DiskFreeLimitBytes != int64(max(0.5, r.MinRAM)*1e9) {
					t.Fatalf("resource-dependent RabbitMQ limits did not converge: %+v", spec)
				}
			}
		})
	}
}

type cpuModeCooldownError struct{}

func (cpuModeCooldownError) Error() string     { return "CPU mode was changed too recently" }
func (cpuModeCooldownError) ErrorCode() string { return "lastCpuModeChangeTooRecent" }

func TestCPUModeCooldownPersistsAndResumesSafely(t *testing.T) {
	for _, knownChange := range []bool{false, true} {
		t.Run(fmt.Sprintf("knownChange=%v", knownChange), func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			clock := rc.Clock.(*testClock)
			expected := clock.at.Add(time.Hour)
			if knownChange {
				s := state(t, rc)
				s.Members[0].CPUModeChangedAt = clock.at.Add(-20 * time.Minute)
				expected = clock.at.Add(40 * time.Minute)
				if err := s.save(rc.Cluster); err != nil {
					t.Fatal(err)
				}
			}
			a.log.clear()
			rc.Cluster.Spec.Components.Engine.Resources.CPUMode = "DEDICATED"
			f.failures["resources:demom0"] = cpuModeCooldownError{}
			var waiting *engine.PlatformRetryWait
			for range 20 {
				_, err := p.Sync(context.Background(), rc)
				if err == nil {
					continue
				}
				if !errors.As(err, &waiting) {
					t.Fatal(err)
				}
				break
			}
			if waiting == nil || !waiting.RetryAt.Equal(expected) {
				t.Fatalf("CPU mode deadline: %+v expected %s", waiting, expected)
			}
			s := state(t, rc)
			retry := s.PlatformRetries["demom0/resources"]
			if retry == nil || !retry.CPUModeCooldown || retry.Failures != 0 || s.Pending != nil || s.Retry == nil || s.Members[0].Resources.CPUMode != "SHARED" || a.broker("demom0").running {
				t.Fatalf("cooldown failed to preserve stopped broker/applied resources: %+v", s)
			}
			calls := a.log.snapshot()
			assertBefore(t, calls, "agent:demom0:stop", "infra:demom0:resources")
			// Simulate a control-plane restart using serialized durable state.
			rc.Cluster = cloneCluster(t, rc.Cluster)
			p = New(Options{Store: rc.Store, InternalCAPEM: "control-plane-ca"})
			a.p = p
			before := f.mutations
			clock.at = expected.Add(-time.Nanosecond)
			if _, err := p.Sync(context.Background(), rc); !errors.As(err, &waiting) {
				t.Fatal("persisted cooldown did not block retry", err)
			}
			if f.mutations != before || state(t, rc).Members[0].Resources.CPUMode != "SHARED" {
				t.Fatal("cooldown attempted platform mutation before deadline")
			}
			delete(f.failures, "resources:demom0")
			clock.at = expected
			syncUntil(t, p, rc, func(s *internalState) bool {
				if s.Phase != "ready" || s.Pending != nil || s.Retry != nil {
					return false
				}
				for _, m := range s.Members {
					if m.ConfigResources.CPUMode != "DEDICATED" || m.ChangeStep != "" {
						return false
					}
				}
				return true
			})
			if len(state(t, rc).PlatformRetries) != 0 {
				t.Fatal("successful CPU mode retry did not reset backoff")
			}
			for _, m := range state(t, rc).Members {
				if m.Resources.CPUMode != "DEDICATED" || m.CPUModeChangedAt.IsZero() {
					t.Fatalf("CPU mode success not checkpointed: %+v", m)
				}
			}
		})
	}
}

func TestCPUModeErrorCodeDoesNotMaskOtherResourceFailures(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 2
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 2
	f.failures["resources:demom0"] = errors.New("lastCpuModeChangeTooRecent")
	_, err := p.Sync(context.Background(), rc)
	if err == nil {
		t.Fatal("resource rejection hidden")
	}
	s := state(t, rc)
	r := s.PlatformRetries["demom0/resources"]
	if r == nil || r.CPUModeCooldown || r.Failures != 1 || !r.RetryAfter.Equal(rc.Clock.Now().Add(30*time.Second)) {
		t.Fatalf("uncoded error misclassified as CPU cooldown: %+v", r)
	}
	if s.Members[0].Resources != runtimeResources(domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1, MinDisk: 5, MaxDisk: 5}) {
		t.Fatal("failed resource request changed applied snapshot")
	}
}
