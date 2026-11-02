// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestCoverageFortyFullRootsConvergeAcrossOneHundredSweeps(t *testing.T) {
	fixture := delayedCoverageFixture(t, 2)
	original := fixture.request.ObjectPaths[0]
	var template myspec.BackupObjectManifest
	if err := json.Unmarshal(fixture.objects[metadataObject(original, physicalManifestName)], &template); err != nil {
		t.Fatal(err)
	}
	requests := make([]myspec.PITRCoverageRequest, 40)
	for i := range requests {
		base := fmt.Sprintf("%s/base/full-%02d", fixture.repo.Path, i)
		manifest := template
		manifest.ObjectPrefix = base
		manifest.Objects = append([]myspec.BackupObject(nil), template.Objects...)
		for j := range manifest.Objects {
			old := manifest.Objects[j].Key
			key := base + strings.TrimPrefix(old, original)
			manifest.Objects[j].Key = key
			fixture.objects[key] = fixture.objects[old]
		}
		data, digest, err := manifest.Canonical()
		if err != nil {
			t.Fatal(err)
		}
		fixture.objects[metadataObject(base, physicalManifestName)] = data
		requests[i] = myspec.PITRCoverageRequest{Source: fixture.repo, ObjectPaths: []string{base}, ObjectManifests: []myspec.BackupManifestRef{{ObjectPrefix: base, Digest: digest, Size: int64(len(data))}}, AcceptPending: true}
	}
	agent := fixture.agent(t, t.TempDir())
	sweep := func() int {
		t.Helper()
		ready := 0
		for _, request := range requests {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			response, err := agent.pollRequest(ctx, request)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if !response.Pending {
				if len(response.Windows) == 0 {
					t.Fatal("completed root omitted windows")
				}
				ready++
			}
		}
		return ready
	}
	converge := func() {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for sweep() != len(requests) {
			if time.Now().After(deadline) {
				t.Fatal("40 roots did not converge")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	converge()
	service, err := agent.runner.coverageService()
	if err != nil {
		t.Fatal(err)
	}
	// Exercise an entire subsequent refresh cycle, not just the initial lease.
	service.mu.Lock()
	for _, request := range requests {
		key := coverageRequestKey(request)
		prefix := coverageStatePrefix(service.root, key)
		verified, err := loadCoverageVerified(prefix)
		if err != nil || verified == nil {
			service.mu.Unlock()
			t.Fatalf("resident eviction lost durable root: %v", err)
		}
		verified.VerifiedAt = time.Now().Add(-2 * coverageLease)
		verified.StartedAt = verified.VerifiedAt
		if err := durableJSON(prefix+".verified.json", verified); err != nil {
			service.mu.Unlock()
			t.Fatal(err)
		}
		if entry := service.entries[key]; entry != nil {
			entry.verified = verified
			entry.lastAttempt = verified.StartedAt
		}
	}
	service.mu.Unlock()
	converge()
	gets, lists := fixture.gets.Load(), fixture.lists.Load()
	started := time.Now()
	for i := 0; i < 100; i++ {
		if ready := sweep(); ready != len(requests) {
			t.Fatalf("sweep%d lost convergence: ready=%d", i, ready)
		}
	}
	if fixture.gets.Load() != gets || fixture.lists.Load() != lists {
		t.Fatalf("steady sweeps performed S3 work: GET=%d LIST=%d", fixture.gets.Load()-gets, fixture.lists.Load()-lists)
	}
	service.mu.Lock()
	resident := len(service.entries)
	service.mu.Unlock()
	if resident > coverageResidentRoots {
		t.Fatalf("resident entries=%d", resident)
	}
	t.Logf("40 valid full roots, expired-lease reconvergence, 100 sweeps: elapsed=%s sidecarGETdelta=0 LISTdelta=0 resident=%d", time.Since(started), resident)
}
