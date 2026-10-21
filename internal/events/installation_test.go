// SPDX-License-Identifier: Apache-2.0
package events

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallationEventPreservesEmptyScope(t *testing.T) {
	key, _ := secrets.New(strings.Repeat("k", 32))
	st, err := store.Open(filepath.Join(t.TempDir(), "state.db"), key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	hub := NewHub(2)
	all, cancel := hub.Subscribe("", "")
	defer cancel()
	scoped, stop := hub.Subscribe("default", "")
	defer stop()
	recorder := NewRecorder(st, hub)
	if err = recorder.Record(context.Background(), domain.Event{Type: "Warning", Reason: "ControlPlaneBackupFailed", Message: "backup failed"}); err != nil {
		t.Fatal(err)
	}
	event := <-all
	if event.Namespace != "" || event.Cluster != "" {
		t.Fatalf("event scope=%q/%q", event.Namespace, event.Cluster)
	}
	select {
	case event := <-scoped:
		t.Fatalf("installation event delivered to namespace subscription: %+v", event)
	default:
	}
	events, err := st.ListEvents(context.Background(), store.EventFilter{})
	if err != nil || len(events) != 1 || events[0].Namespace != "" {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}
