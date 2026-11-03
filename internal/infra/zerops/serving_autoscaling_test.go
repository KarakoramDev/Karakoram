// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestServingAutoscalingRetentionAndReplay(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		from, to                                 int
		resource, replace, lost, pending, failed bool
	}{
		{name: "scale-out", from: 1, to: 3},
		{name: "scale-in", from: 3, to: 1},
		{name: "resources", from: 2, to: 2, resource: true},
		{name: "replaced-scale", from: 1, to: 2, replace: true},
		{name: "replaced-scale-in", from: 3, to: 1, replace: true},
		{name: "replaced-resources", from: 2, to: 2, resource: true, replace: true},
		{name: "lost-count-reply", from: 1, to: 2, lost: true},
		{name: "lost-resource-reply", from: 2, to: 2, resource: true, lost: true},
		{name: "delayed-process", from: 1, to: 2, pending: true},
		{name: "failed-process", from: 1, to: 2, failed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, f := newFakeAPIWithState(t)
			newBackend := func() *Backend {
				b, err := New(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", MaxRetries: -1, PollInterval: time.Millisecond, WaitTimeout: time.Second})
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			b := newBackend()
			var service ServiceStack
			if err := json.Unmarshal(fixture(t, "service"), &service); err != nil {
				t.Fatal(err)
			}
			service.ID, service.Name, service.Base, service.Status = "proxy-id", "proxy", "ubuntu@24.04", "ACTIVE"
			service.ProjectID = "project"
			putJSON(service.CurrentAutoscaling, "horizontalAutoscaling", map[string]int{"minContainerCount": tc.from, "maxContainerCount": tc.from})
			f.services[service.ID] = &service
			before, err := b.Containers(context.Background(), service.Name)
			if err != nil {
				t.Fatal(err)
			}
			f.replaceContainers, f.loseAutoscalingReply, f.autoscalingPending = tc.replace, tc.lost, tc.pending
			submit := func(b *Backend) (infra.Op, error) {
				if tc.resource {
					return b.SetResources(context.Background(), "proxy", infra.Resources{MaxCPU: 2})
				}
				return b.SetContainerCount(context.Background(), "proxy", tc.to, tc.to)
			}
			op, err := submit(b)
			if err != nil {
				t.Fatal(err)
			}
			if op.Kind != "serving-autoscaling" {
				t.Fatalf("missing serving receipt: %+v", op)
			}
			// A lost engine checkpoint must recover the original baseline and process,
			// rather than recapturing post-update IDs or submitting another PUT.
			b = newBackend()
			replay, err := submit(b)
			if err != nil {
				t.Fatal(err)
			}
			if replay.ID != op.ID || f.autoscalingWrites != 1 {
				t.Fatalf("replayed submission: writes=%d op=%+v", f.autoscalingWrites, replay)
			}
			if tc.failed {
				f.mu.Lock()
				for id, p := range f.processes {
					if p.ActionName == "autoscaling" {
						p.Status = "FAILED"
						f.processes[id] = p
					}
				}
				f.mu.Unlock()
			}
			status, err := b.OpStatus(context.Background(), replay)
			if err != nil {
				t.Fatal(err)
			}
			if tc.failed {
				if !status.Done || !status.Failed {
					t.Fatalf("failed process accepted: %+v", status)
				}
				return
			}
			if tc.pending {
				if status.Done {
					t.Fatalf("completed pending process: %+v", status)
				}
				f.mu.Lock()
				for id, p := range f.processes {
					if p.ActionName == "autoscaling" {
						p.Status = "FINISHED"
						f.processes[id] = p
					}
				}
				f.mu.Unlock()
				status, err = b.OpStatus(context.Background(), replay)
				if err != nil {
					t.Fatal(err)
				}
			}
			if tc.replace {
				if !status.Failed || !strings.Contains(status.Message, "incumbent") {
					t.Fatalf("replacement accepted: %+v", status)
				}
				return
			}
			if !status.Done || status.Failed {
				t.Fatalf("not completed: %+v", status)
			}
			// Re-observing a completed receipt after a lost CP checkpoint is safe too.
			status, err = b.OpStatus(context.Background(), replay)
			if err != nil || !status.Done || status.Failed {
				t.Fatalf("completion replay: %+v %v", status, err)
			}
			after, err := b.Containers(context.Background(), "proxy")
			if err != nil {
				t.Fatal(err)
			}
			if len(after) != tc.to {
				t.Fatalf("count=%d want %d", len(after), tc.to)
			}
			retained := min(len(before), len(after))
			for i := range retained {
				if before[i].ID != after[i].ID {
					t.Fatal("incumbent replaced")
				}
			}
		})
	}
}

func TestServingResourceRequestExclusions(t *testing.T) {
	b := &Backend{}
	current := infra.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 1}
	target := current
	target.MaxCPU = 2
	if !b.ResourceUpdatePreservesServingFor(current, target) {
		t.Fatal("same-mode resize excluded")
	}
	target.CPUMode = "DEDICATED"
	if b.ResourceUpdatePreservesServingFor(current, target) {
		t.Fatal("CPU-mode switch admitted")
	}
	target = current
	target.MaxDisk = 10
	if b.ResourceUpdatePreservesServingFor(current, target) {
		t.Fatal("disk change admitted")
	}
}
