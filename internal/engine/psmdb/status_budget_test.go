// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"go.mongodb.org/mongo-driver/v2/bson"
	"net/http"
	"strings"
	"testing"
	"time"
)

func assertStatusBudget(t *testing.T, start time.Time, err error, member string) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > engine.ObservationTimeout+time.Second {
		t.Errorf("status read took %s", elapsed)
	}
	if err == nil {
		t.Fatal("expected status observation diagnostic")
	}
	for _, text := range []string{"generic status read exceeded 4s budget", "inspect member " + member + " responsiveness/connectivity"} {
		if !strings.Contains(err.Error(), text) {
			t.Errorf("missing %q in %v", text, err)
		}
	}
}

func TestRestoreIdentityStatusBudget(t *testing.T) {
	for _, reload := range []bool{false, true} {
		t.Run(map[bool]string{false: "prepare", true: "reload"}[reload], func(t *testing.T) {
			p, rc, _, _ := setup(t)
			s := &internalState{Members: []*member{{Name: "member0"}}}
			rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/reload" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if r.URL.Path != "/v1/status" {
					t.Errorf("unexpected request %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				<-r.Context().Done()
			})
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			h := hooks{p: p, rc: rc, state: s}
			start := time.Now()
			var err error
			var done bool
			if reload {
				done, err = h.reloadPBMIdentity(ctx, s, 1)
			} else {
				done, err = h.PreparePITRIdentity(ctx, "restore")
			}
			if done {
				t.Fatal("identity accepted without status")
			}
			assertStatusBudget(t, start, err, "member0")
		})
	}
}
func TestRestoreWorkerStatusBudget(t *testing.T) {
	p, rc, _, _ := backupCleanupSetup(t)
	s := retryState(t, rc)
	rc.Cluster.Spec.TLS.Mode, s.TLSMode = "disabled", "disabled"
	s.Sets["rs0"].Stage = "ready"
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	native := nativeHealingFixture(t, rc, s)
	id := bson.NewObjectID()
	native.hello = bson.M{"ok": 1, "localTime": time.Now()}
	native.command = func(command bson.Raw) bson.M {
		collection, ok := command.Lookup("find").StringValueOK()
		if !ok {
			return bson.M{"ok": 1}
		}
		var document bson.M
		switch collection {
		case "pbmCmd":
			document = bson.M{"_id": id}
		case "pbmRestores":
			document = bson.M{"status": "running"}
		case "pbmLock":
			document = bson.M{"node": s.Members[0].Name + ":27017"}
		}
		return bson.M{"ok": 1, "cursor": bson.M{"id": int64(0), "ns": "admin." + collection, "firstBatch": bson.A{document}}}
	}
	rc.Agents = responder(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/status" {
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		<-r.Context().Done()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	start := time.Now()
	_, _, err := (hooks{p: p, rc: rc}).ObserveRestoreWorkerFailure(ctx, "native", pbm.Env{RC: rc, Nodes: []pbm.Node{{Member: s.Members[0].Name}}}, map[string]pbm.RestoreWorkerIdentity{})
	assertStatusBudget(t, start, err, s.Members[0].Name)
}
