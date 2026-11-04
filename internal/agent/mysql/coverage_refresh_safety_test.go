// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestCoverageStatusLeaseAndFreshAdmissionDeletion(t *testing.T) {
	fixture := delayedCoverageFixture(t, 20)
	fixture.delay.Store(int64(time.Millisecond))
	agent := fixture.agent(t, t.TempDir())
	waitRefresh(t, agent, 20, 10*time.Second)
	fixture.count.Store(19)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	before := time.Now()
	fresh, err := agent.poll(ctx, true)
	cancel()
	if err != nil || !fresh.Fresh || !fresh.VerificationStartedAt.After(before) || len(fresh.Windows) != 1 || fresh.Windows[0].End.UnixMicro() != 1793577600000000+19*50 {
		t.Fatalf("fresh admission reused deleted history: %+v %v", fresh, err)
	}
	service := agent.runner.coverageRefresh
	service.mu.Lock()
	entry := service.entries[coverageRequestKey(agent.request)]
	entry.verified.VerifiedAt = time.Now().Add(-coverageLease - time.Second)
	entry.lastAttempt = time.Now()
	service.mu.Unlock()
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	out, err := agent.poll(ctx, false)
	cancel()
	if err != nil || !out.Pending || len(out.Windows) != 0 || out.Progress == nil {
		t.Fatalf("expired verification returned success: %+v %v", out, err)
	}
}

func TestCoverageSingleWorkerMakesProgressForMultipleRoots(t *testing.T) {
	fixture := delayedCoverageFixture(t, 500)
	fixture.delay.Store(int64(3 * time.Millisecond))
	oldBase := fixture.request.ObjectPaths[0]
	newBase := fixture.repo.Path + "/base/second"
	var manifest myspec.BackupObjectManifest
	if err := json.Unmarshal(fixture.objects[metadataObject(oldBase, physicalManifestName)], &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.ObjectPrefix = newBase
	for i := range manifest.Objects {
		oldKey := manifest.Objects[i].Key
		newKey := strings.Replace(oldKey, oldBase+"/", newBase+"/", 1)
		manifest.Objects[i].Key = newKey
		fixture.objects[newKey] = fixture.objects[oldKey]
	}
	raw, digest, err := manifest.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	fixture.objects[metadataObject(newBase, physicalManifestName)] = raw
	first := fixture.agent(t, t.TempDir())
	second := *first
	second.request = myspec.PITRCoverageRequest{Source: fixture.repo, ObjectPaths: []string{newBase}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: newBase, Digest: digest, Size: int64(len(raw))}}}
	deadline := time.Now().Add(20 * time.Second)
	readyA, readyB := false, false
	for time.Now().Before(deadline) && (!readyA || !readyB) {
		for index, agent := range []*refreshTestAgent{first, &second} {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			out, err := agent.poll(ctx, false)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if !out.Pending && len(out.Windows) == 1 {
				if index == 0 {
					readyA = true
				} else {
					readyB = true
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !readyA || !readyB {
		t.Fatalf("one root starved another: first=%t second=%t", readyA, readyB)
	}
	if fixture.peak.Load() > coverageReadConcurrency {
		t.Fatalf("multiple roots spawned concurrent workers: maxS3=%d", fixture.peak.Load())
	}
}
