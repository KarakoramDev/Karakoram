// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestLegacyTemporaryPolicyJournalAndHandleCompleteAfterReconstruction(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.MinContainers, spec.MaxContainers = 3, 3
	id := importFake(t, b, spec).Services["runtime"]
	deployFake(t, b, "runtime", "")
	original := fakeAppSnapshot(t, f, "runtime")["updateConfigSwarm"]
	if _, err := b.SetContainerCount(t.Context(), "runtime", 2, 2); err != nil {
		t.Fatal(err)
	}
	m := fakeMeta(t, f, "runtime")
	pending := m.Pending
	if pending == nil {
		t.Fatal("missing temporary scale-back operation")
	}
	previous, err := json.Marshal(pending.PreviousTasks)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately construct the pre-change wire format, not encodeOp or
	// operation.MarshalJSON: both persisted metadata and handles used this flag.
	legacy := fmt.Sprintf(`{"action":"scale","appId":%q,"appName":%q,"hostname":"runtime","tasks":%s,"issued":%q,"restoreStartFirst":true,"specification":%q}`,
		pending.AppID, pending.AppName, previous, pending.Issued.Format(time.RFC3339Nano), pending.Specification)
	var wire map[string]json.RawMessage
	if err := json.Unmarshal([]byte(strings.TrimPrefix(m.description(), metadataPrefix)), &wire); err != nil {
		t.Fatal(err)
	}
	wire["pending"] = json.RawMessage(legacy)
	metadataJSON, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.apps[id].row["description"] = metadataPrefix + string(metadataJSON)
	f.mu.Unlock()
	legacyHandle := infra.Op{ID: "dokploy:v1:" + base64.RawURLEncoding.EncodeToString([]byte(legacy)), Kind: "scale", ServiceID: pending.AppID, Hostname: "runtime"}
	b = fakeBackend(t, f, nil)
	migrated := fakeMeta(t, f, "runtime").Pending
	decoded, err := decodeOp(legacyHandle)
	if err != nil || migrated == nil || len(migrated.RestoreUpdateConfig) == 0 || !reflect.DeepEqual(decoded.RestoreUpdateConfig, migrated.RestoreUpdateConfig) {
		t.Fatalf("legacy metadata/handle migration disagrees: metadata=%+v handle=%+v err=%v", migrated, decoded, err)
	}
	awaitFake(t, b, legacyHandle)
	if got := fakeAppSnapshot(t, f, "runtime")["updateConfigSwarm"]; !reflect.DeepEqual(got, original) {
		t.Fatalf("legacy completion failed to restore start-first: got=%#v want=%#v", got, original)
	}
	if fakeMeta(t, f, "runtime").Pending != nil {
		t.Fatal("legacy operation did not complete after backend reconstruction")
	}
	encoded, err := json.Marshal(decoded)
	if err != nil || strings.Contains(string(encoded), "restoreStartFirst") {
		t.Fatalf("migrated operation rewrites obsolete flag: %s err=%v", encoded, err)
	}
}

func TestLegacyPolicyMigrationDoesNotOverrideExplicitOriginalPolicy(t *testing.T) {
	var op operation
	if err := json.Unmarshal([]byte(`{"restoreStartFirst":true,"restoreUpdateConfig":{"Parallelism":2,"Order":"stop-first","FailureAction":"rollback"}}`), &op); err != nil {
		t.Fatal(err)
	}
	var policy map[string]any
	if err := json.Unmarshal(op.RestoreUpdateConfig, &policy); err != nil || policy["Order"] != "stop-first" || policy["Parallelism"] != float64(2) {
		t.Fatalf("legacy migration overrode explicit policy: %#v err=%v", policy, err)
	}
}
