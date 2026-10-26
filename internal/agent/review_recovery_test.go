// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestReviewPartialNativeDataNeverConfirmsVolumeLoss(t *testing.T) {
	for _, fixture := range []struct{ engine, path, data string }{
		{"", "db/collection-1.wt", "retained relations"},
		{"postgres", "postgres/base/1/1234", "retained relations"},
		{"postgres", "postgres/PG_VERSION", "corrupt"},
		{"postgres", "postgres-retired/base/1/1234", "old-retired upgrade data"},
		{"postgres", "karakoram/upgrade-state.json", `{"phase":"old-retired"}`},
		{"elasticsearch", "karakoram/data/indices/uuid/0/index/segments_1", "retained index"},
		{"rabbitmq", "rabbitmq/rabbit@member/msg_stores/persistent/1.rdq", "retained queue"},
	} {
		for _, initialized := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%s/%t", fixture.engine, fixture.path, initialized), func(t *testing.T) {
				a, c := volumeFixture(t, fixture.engine, initialized)
				volumeFile(t, a, fixture.path, []byte(fixture.data))
				err := a.volumeSafety(t.Context(), c)
				if err == nil || !strings.Contains(err.Error(), "VolumeIdentityUnavailable") {
					t.Fatal("partial layout was adopted or promoted to loss", err)
				}
				if status := a.volumeObservation(); status.Lost || status.Verified || status.Message == "" {
					t.Fatal("unsafe replacement evidence", status)
				}
				if _, err := os.Stat(filepath.Join(a.o.DataDir, volumeSentinel)); !os.IsNotExist(err) {
					t.Fatal("unknown data acquired a sentinel", err)
				}
			})
		}
	}
}

func TestReviewLegacyControlsMigrateAndResume(t *testing.T) {
	a, c, _ := fakeNode(t, buildFake(t))
	if err := cacheConfig(c, a.o.DataDir); err != nil {
		t.Fatal(err)
	}
	// The base-format ordinary apply had a cache but no controls provenance.
	a.controls, a.controlsErr = loadControls(a.o.DataDir)
	if a.controlsErr != nil {
		t.Fatal(a.controlsErr)
	}
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	defer a.sup.shutdown(context.Background())
	if workerState(a).PID == 0 {
		t.Fatal("legacy member did not resume")
	}
	if _, err := os.Stat(filepath.Join(a.o.DataDir, "karakoram", "controls.initialized")); err != nil {
		t.Fatal("migration did not establish provenance", err)
	}
}

func TestReviewLegacyMaintenanceAndDesiredProcessesPreserved(t *testing.T) {
	a, c, _ := fakeNode(t, buildFake(t))
	c.Maintenance = true
	c.DesiredProcesses = []string{}
	if err := cacheConfig(c, a.o.DataDir); err != nil {
		t.Fatal(err)
	}
	a.controls, a.controlsErr = loadControls(a.o.DataDir)
	if a.controlsErr != nil || !a.controls.Maintenance {
		t.Fatal("legacy hold lost", a.controls, a.controlsErr)
	}
	c.Maintenance = false
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	defer a.sup.shutdown(context.Background())
	if workerState(a).PID != 0 || !a.config().Maintenance {
		t.Fatal("migration released a legacy hold")
	}
}

func TestReviewInterruptedInitialControlsPublicationRecoversExactIntent(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprint(held), func(t *testing.T) {
			a, _ := recoveryAgent(t)
			path := filepath.Join(a.o.DataDir, "karakoram", "controls.json")
			if err := os.MkdirAll(path, 0700); err != nil {
				t.Fatal(err)
			}
			controls := localControls{Maintenance: held, Stopped: map[string]bool{"mongod": held}}
			if err := a.saveControls(controls); err == nil {
				t.Fatal("fault did not interrupt initial publication")
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			recovered, err := loadControls(a.o.DataDir)
			if err != nil || recovered.Maintenance != held || recovered.Stopped["mongod"] != held {
				t.Fatal("initial intent was not recoverable", recovered, err)
			}
			if err := a.saveControls(recovered); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(path); err != nil {
				t.Fatal(err)
			}
			if _, err := loadControls(a.o.DataDir); err == nil {
				t.Fatal("established controls disappearance did not fence")
			}
		})
	}
}

func TestReviewRestartPreparationRefusalIsRetryable(t *testing.T) {
	a, c, base := fakeNode(t, buildFake(t))
	e := &preparingEngine{fakeEngine: base}
	a.engine = e
	if _, err := a.apply(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	defer a.sup.shutdown(context.Background())
	old := workerState(a).PID
	request := agentapi.ProcessAction{Process: "worker", Action: "restart", RequestID: "retry-preparation"}
	e.prepareErr = errors.New("temporary native gate failure")
	if err := a.processActionRequest(t.Context(), request); !errors.Is(err, e.prepareErr) {
		t.Fatal(err)
	}
	if workerState(a).PID != old {
		t.Fatal("refusal mutated process")
	}
	e.prepareErr = nil
	if err := a.processActionRequest(t.Context(), request); err != nil {
		t.Fatal("same identity remained fenced", err)
	}
	next := workerState(a).PID
	if next == 0 || next == old {
		t.Fatal("retry did not restart")
	}
	if err := a.processActionRequest(t.Context(), request); err != nil || workerState(a).PID != next {
		t.Fatal("successful replay repeated restart", err)
	}
}

func TestReviewRestartControlsRefusalIsRetryable(t *testing.T) {
	a, _ := recoveryAgent(t)
	a.sup.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := a.sup.action(t.Context(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	defer a.sup.shutdown(context.Background())
	if err := a.saveControls(localControls{}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(a.o.DataDir, "karakoram", "controls.json")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	request := agentapi.ProcessAction{Process: "mongod", Action: "restart", RequestID: "retry-storage"}
	states, _ := a.sup.snapshot()
	old := states["mongod"].PID
	if err := a.processActionRequest(t.Context(), request); err == nil {
		t.Fatal("controls publication failure ignored")
	}
	states, _ = a.sup.snapshot()
	if states["mongod"].PID != old {
		t.Fatal("storage refusal mutated child")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := a.processActionRequest(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	states, _ = a.sup.snapshot()
	next := states["mongod"].PID
	if next == old || next == 0 {
		t.Fatal("retry did not restart")
	}
	if err := a.processActionRequest(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	states, _ = a.sup.snapshot()
	if states["mongod"].PID != next {
		t.Fatal("replay restarted twice")
	}
}

func TestReviewIdentifiedRestartTimeoutAcknowledgesAutonomousReplacement(t *testing.T) {
	a, _ := recoveryAgent(t)
	a.sup.backoff = 10 * time.Millisecond
	a.sup.configure("mongod", processSpec{binary: "/bin/sleep", args: []string{"30"}})
	if err := a.sup.action(t.Context(), "mongod", "start"); err != nil {
		t.Fatal(err)
	}
	states, _ := a.sup.snapshot()
	old := states["mongod"].PID
	defer func() { syscall.Kill(old, syscall.SIGCONT); a.sup.shutdown(context.Background()) }()
	if err := syscall.Kill(old, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	awaitState(t, a.sup, func(p agentapi.ProcessStatus) bool { return p.State == "paused" })
	request := agentapi.ProcessAction{Process: "mongod", Action: "restart", RequestID: "timeout-restart"}
	a.sup.mu.Lock()
	generation := a.sup.children["mongod"].generation
	a.sup.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- a.processActionRequest(ctx, request) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		a.sup.mu.Lock()
		p := a.sup.children["mongod"]
		admitted := p.generation > generation && p.desired && p.cmd != nil && p.cmd.Process.Pid == old
		a.sup.mu.Unlock()
		if admitted {
			break
		}
		select {
		case err := <-result:
			t.Fatal("restart returned before stop admission", err)
		case <-deadline.C:
			t.Fatal("restart stop admission timed out")
		case <-tick.C:
		}
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("expected cancellation of admitted stop", err)
		}
	case <-deadline.C:
		t.Fatal("admitted restart did not return after cancellation")
	}
	if err := syscall.Kill(old, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	awaitState(t, a.sup, func(p agentapi.ProcessStatus) bool { return p.PID != 0 && p.PID != old })
	states, _ = a.sup.snapshot()
	next := states["mongod"].PID
	if err := a.processActionRequest(t.Context(), request); err != nil {
		t.Fatal("converged generation not acknowledged", err)
	}
	states, _ = a.sup.snapshot()
	if states["mongod"].PID != next {
		t.Fatal("converged restart was repeated")
	}
}

func TestReviewRestartRetentionBoundsAndRetiredReplay(t *testing.T) {
	a, _ := recoveryAgent(t)
	dir := filepath.Join(a.o.DataDir, "karakoram", "restarts")
	var first, last agentapi.ProcessAction
	for i := range restartActiveLimit * 3 {
		request := agentapi.ProcessAction{Process: "mongod", Action: "restart", RequestID: fmt.Sprintf("restart/%06d", i)}
		if i == 0 {
			first = request
		}
		last = request
		digest := sha256.Sum256([]byte(request.RequestID))
		if err := reserveRestartReceipt(dir, digest); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, hex.EncodeToString(digest[:])+".json")
		if err := writeRestartReceipt(path, restartReceipt{Request: request, Complete: true}); err != nil {
			t.Fatal(err)
		}
		stamp := time.Unix(int64(i+1), 0)
		if err := os.Chtimes(path, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != restartActiveLimit {
		t.Fatal("active receipt storage not bounded", len(entries), err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Size() > 8192 {
			t.Fatal("receipt byte bound exceeded", info, err)
		}
	}
	filter, err := os.Stat(dir + ".retired")
	if err != nil || filter.Size() != 4+restartRetiredBytes {
		t.Fatal("retirement metadata not fixed-size", filter, err)
	}
	// A newly constructed supervisor still refuses retired IDs and replays an
	// active completion, without needing a configured/running process.
	a.sup = newSupervisor(nil)
	if err := a.processActionRequest(t.Context(), first); err == nil || !strings.Contains(err.Error(), "retired") {
		t.Fatal("retired identity re-enabled", err)
	}
	if err := a.processActionRequest(t.Context(), last); err != nil {
		t.Fatal("active completion did not replay", err)
	}
}

func TestReviewInvalidRestartDoesNotConsumeStorage(t *testing.T) {
	a, _ := recoveryAgent(t)
	for i := range 200 {
		err := a.processActionRequest(t.Context(), agentapi.ProcessAction{Process: strings.Repeat("x", 10000), Action: "restart", RequestID: fmt.Sprint(i)})
		if err == nil {
			t.Fatal("invalid process accepted")
		}
	}
	if _, err := os.Stat(filepath.Join(a.o.DataDir, "karakoram")); !os.IsNotExist(err) {
		t.Fatal("invalid requests consumed durable storage", err)
	}
}

func TestReviewUnresolvedReceiptNotRetired(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "restarts")
	for i := range restartActiveLimit {
		request := agentapi.ProcessAction{Process: "mongod", Action: "restart", RequestID: fmt.Sprint(i)}
		digest := sha256.Sum256([]byte(request.RequestID))
		if err := reserveRestartReceipt(dir, digest); err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(restartReceipt{Request: request})
		if err := atomicWrite(filepath.Join(dir, hex.EncodeToString(digest[:])+".json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := reserveRestartReceipt(dir, sha256.Sum256([]byte("new"))); err == nil || !strings.Contains(err.Error(), "unresolved") {
		t.Fatal("unresolved receipt evicted", err)
	}
}
