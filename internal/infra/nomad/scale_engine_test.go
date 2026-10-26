// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package nomad

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestEngineScaleBackDeletesUnplacedNomadJobWithoutReplay(t *testing.T) {
	f, b, deploy := blockedDeployment(t, true)
	f.mu.Lock()
	for id, n := range f.nodes {
		n.SchedulingEligibility = "eligible"
		f.nodes[id] = n
	}
	replayed := 0
	f.requestHook = func(w http.ResponseWriter, request *http.Request) (bool, func()) {
		if request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" {
			raw, err := io.ReadAll(request.Body)
			var body struct{ Job struct{ Stop bool } }
			if err != nil || json.Unmarshal(raw, &body) != nil {
				t.Error("invalid job write in retirement fixture")
			}
			request.Body = io.NopCloser(bytes.NewReader(raw))
			// The required Stop=true CAS update is not a deploy replay.
			if !body.Job.Stop {
				replayed++
			}
		}
		return false, nil
	}
	f.mu.Unlock()
	r, err := b.load(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	v, err := b.load(t.Context(), "volume")
	if err != nil {
		t.Fatal(err)
	}
	internal, err := json.Marshal(map[string]any{"uid": "existing", "members": []map[string]any{
		{"name": "demom0", "index": 0, "role": "mongod", "set": "rs0", "stage": "active", "version": "existing"},
		{"name": "runtime", "volume": "volume", "index": 1, "role": "mongod", "set": "rs0", "stage": "imported", "serviceId": r.ID, "volumeId": v.ID},
	}, "pending": map[string]any{"op": deploy, "action": "deploy", "member": "runtime"}})
	if err != nil {
		t.Fatal(err)
	}
	sealer, err := secrets.New(strings.Repeat("x", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	c := &domain.Cluster{Name: "demo", Internal: internal}
	p := psmdb.New(psmdb.Options{Store: st})
	p.ApplyDefaults(&c.Spec)
	c.Spec.Components.Engine.Replicas = 1
	rc := &engine.Context{Cluster: c, Store: st, Secrets: st, Backend: b}
	var elapsed time.Duration
	for range 40 {
		result, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		var state struct {
			Members []struct {
				Name string `json:"name"`
			} `json:"members"`
		}
		if err := json.Unmarshal(c.Internal, &state); err != nil {
			t.Fatal(err)
		}
		if len(state.Members) == 1 {
			f.mu.Lock()
			defer f.mu.Unlock()
			if len(f.jobs) != 0 || len(f.volumes) != 0 || replayed != 0 {
				t.Fatalf("retirement leaked/re-registered resources: jobs=%d volumes=%d replayed=%d", len(f.jobs), len(f.volumes), replayed)
			}
			if elapsed > 8*time.Second {
				t.Fatalf("unplaced retirement spent %s in serial requeues; want <=8s", elapsed)
			}
			return
		}
		// Backend reconstruction must not lose the retirement handle or replay deploy.
		rc.Backend = fakeBackend(t, f, nil)
		elapsed += result.RequeueAfter
	}
	t.Fatal("scale reversal remained behind the obsolete deployment")
}
