// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package events

import (
	"context"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type subscription struct {
	namespace string
	cluster   string
	ch        chan domain.Event
}

// Hub fans out live events without blocking producers.
type Hub struct {
	mu          sync.RWMutex
	next        uint64
	subscribers map[uint64]subscription
	buffer      int
}

// NewHub constructs a hub with bounded per-subscriber buffers.
func NewHub(buffer int) *Hub {
	if buffer <= 0 {
		buffer = 64
	}
	return &Hub{subscribers: make(map[uint64]subscription), buffer: buffer}
}

// Subscribe returns a bounded stream and an idempotent cancellation function.
// Slow consumers may lose live events and recover them from the durable log.
func (h *Hub) Subscribe(namespace, cluster string) (<-chan domain.Event, func()) {
	h.mu.Lock()
	h.next++
	id := h.next
	ch := make(chan domain.Event, h.buffer)
	h.subscribers[id] = subscription{namespace: namespace, cluster: cluster, ch: ch}
	h.mu.Unlock()
	var once sync.Once
	cancel := func() { once.Do(func() { h.mu.Lock(); delete(h.subscribers, id); close(ch); h.mu.Unlock() }) }
	return ch, cancel
}

// Publish offers an event to matching subscribers without blocking.
func (h *Hub) Publish(event domain.Event) {
	event.Namespace = domain.NormalizedNamespace(event.Namespace)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, s := range h.subscribers {
		if s.namespace != "" && s.namespace != event.Namespace {
			continue
		}
		if s.cluster != "" && s.cluster != event.Cluster {
			continue
		}
		select {
		case s.ch <- event:
		default:
		}
	}
}

// Recorder persists an event before publishing it to live subscribers.
type Recorder struct {
	store store.EventRepository
	hub   *Hub
}

// NewRecorder connects durable event persistence to optional live delivery.
func NewRecorder(repository store.EventRepository, hub *Hub) *Recorder {
	return &Recorder{store: repository, hub: hub}
}

// Record durably appends an event before broadcasting its assigned ID.
func (r *Recorder) Record(ctx context.Context, event domain.Event) error {
	event.Namespace = domain.NormalizedNamespace(event.Namespace)
	if err := r.store.AppendEvent(ctx, &event); err != nil {
		return err
	}
	if r.hub != nil {
		r.hub.Publish(event)
	}
	return nil
}
