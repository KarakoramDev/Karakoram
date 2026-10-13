// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package events

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type memoryEvents struct {
	fail bool
	next int64
}

func (s *memoryEvents) AppendEvent(_ context.Context, e *domain.Event) error {
	if s.fail {
		return errors.New("write failure")
	}
	s.next++
	e.ID = s.next
	return nil
}
func (*memoryEvents) ListEvents(context.Context, store.EventFilter) ([]domain.Event, error) {
	return nil, nil
}
func TestDurablePublishAndBoundedHub(t *testing.T) {
	hub := NewHub(1)
	all, stop := hub.Subscribe("")
	defer stop()
	filtered, unsub := hub.Subscribe("one")
	defer unsub()
	db := &memoryEvents{}
	r := NewRecorder(db, hub)
	if err := r.Record(context.Background(), domain.Event{Cluster: "two"}); err != nil {
		t.Fatal(err)
	}
	if e := <-all; e.ID != 1 {
		t.Fatal(e)
	}
	select {
	case <-filtered:
		t.Fatal("filter ignored")
	default:
	}
	if err := r.Record(context.Background(), domain.Event{Cluster: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Record(context.Background(), domain.Event{Cluster: "one"}); err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 1 {
		t.Fatal("unbounded buffer")
	}
	<-all
	<-filtered
	db.fail = true
	if err := r.Record(context.Background(), domain.Event{Cluster: "one"}); err == nil {
		t.Fatal("suppressed failure")
	}
	select {
	case <-all:
		t.Fatal("published failed write")
	default:
	}
	stop()
	stop()
	if _, ok := <-all; ok {
		t.Fatal("subscription not closed")
	}
}
func TestConcurrentPublishUnsubscribe(t *testing.T) {
	h := NewHub(2)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, cancel := h.Subscribe("one")
			h.Publish(domain.Event{Cluster: "one"})
			cancel()
		}()
	}
	wg.Wait()
}
