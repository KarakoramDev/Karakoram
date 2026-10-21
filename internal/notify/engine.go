// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package notify

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type DeliverySender interface {
	Send(context.Context, domain.NotificationChannel, domain.Event, domain.NotificationCategory) error
}
type Options struct {
	Store  *store.Store
	Hub    *events.Hub
	Sender DeliverySender
	Now    func() time.Time
	// Ticks allows deterministic periodic-drain tests. Nil uses a 15 second ticker.
	Ticks <-chan time.Time
}
type Engine struct{ options Options }

func New(options Options) *Engine {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Sender == nil {
		options.Sender = NewSender()
	}
	return &Engine{options: options}
}
func (e *Engine) category(ctx context.Context, event domain.Event) (domain.NotificationCategory, error) {
	var op *domain.Operation
	if event.Reason == "OperationFailed" && event.Operation != "" {
		var err error
		op, err = e.options.Store.GetOperation(ctx, event.Operation)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return "", err
		}
	}
	return Classify(event, op), nil
}

// Drain fills the outbox before advancing the durable cursor, page by page.
func (e *Engine) Drain(ctx context.Context) error {
	for {
		cursor, err := e.options.Store.NotificationCursor(ctx)
		if err != nil {
			return err
		}
		page, err := e.options.Store.ListEvents(ctx, store.EventFilter{SinceID: cursor, Limit: 1000})
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		channels, err := e.options.Store.ListNotificationChannels(ctx)
		if err != nil {
			return err
		}
		deliveries := make([]store.NotificationDelivery, 0)
		for _, event := range page {
			category, err := e.category(ctx, event)
			if err != nil {
				return err
			}
			for _, channel := range channels {
				if Matches(channel, event, category) {
					deliveries = append(deliveries, store.NotificationDelivery{Channel: channel.Name, EventID: event.ID})
				}
			}
		}
		err = e.options.Store.EnqueueNotifications(ctx, cursor, page[len(page)-1].ID, deliveries, e.options.Now().UTC())
		if errors.Is(err, store.ErrConflict) {
			continue
		}
		if err != nil {
			return err
		}
		if len(page) < 1000 {
			return nil
		}
	}
}
func retryDelay(attempts int) time.Duration {
	delay := 30 * time.Second
	for i := 1; i < attempts && delay < time.Hour; i++ {
		delay *= 2
	}
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}

// DeliverDue processes a bounded batch sequentially for one channel. Run ensures
// only one worker owns each channel, while independent destinations run in parallel.
func (e *Engine) DeliverDue(ctx context.Context, name string) error {
	channel, err := e.options.Store.NotificationChannelForDelivery(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if !channel.Enabled {
		return nil
	}
	due, err := e.options.Store.PendingNotificationDeliveries(ctx, name, e.options.Now())
	if err != nil {
		return err
	}
	for _, delivery := range due {
		now := e.options.Now().UTC()
		if now.Sub(delivery.Created) >= 24*time.Hour {
			delivery.State = "failed"
			if delivery.LastError == "" {
				delivery.LastError = "notification delivery expired"
			}
			if err = e.options.Store.CompleteNotificationAttempt(ctx, delivery, now); err != nil {
				return err
			}
			continue
		}
		page, err := e.options.Store.ListEvents(ctx, store.EventFilter{SinceID: delivery.EventID - 1, Limit: 1})
		if err != nil {
			return err
		}
		if len(page) == 0 || page[0].ID != delivery.EventID {
			return errors.New("notification event unavailable")
		}
		category, err := e.category(ctx, page[0])
		if err != nil {
			return err
		}
		sendErr := e.options.Sender.Send(ctx, *channel, page[0], category)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delivery.Attempts++
		now = e.options.Now().UTC()
		if sendErr == nil {
			delivery.State = "delivered"
			delivery.LastError = ""
		} else {
			delivery.LastError = "notification delivery failed"
			delivery.NextAttempt = now.Add(retryDelay(delivery.Attempts))
			if deadline := delivery.Created.Add(24 * time.Hour); delivery.NextAttempt.After(deadline) {
				delivery.NextAttempt = deadline
			}
			if now.Sub(delivery.Created) >= 24*time.Hour {
				delivery.State = "failed"
			}
		}
		if err = e.options.Store.CompleteNotificationAttempt(ctx, delivery, now); err != nil {
			return err
		}
	}
	return nil
}
func (e *Engine) Run(ctx context.Context) error {
	child, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	var live <-chan domain.Event
	if e.options.Hub != nil {
		var unsubscribe func()
		live, unsubscribe = e.options.Hub.Subscribe("", "")
		defer unsubscribe()
	}
	ticks := e.options.Ticks
	if ticks == nil {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		ticks = ticker.C
	}
	type worker struct {
		wake   chan struct{}
		cancel context.CancelFunc
		done   chan struct{}
	}
	wakes := map[string]worker{}
	failures := make(chan error, 1)
	drain := func() error {
		if err := e.Drain(child); err != nil {
			return err
		}
		channels, err := e.options.Store.ListNotificationChannels(child)
		if err != nil {
			return err
		}
		present := make(map[string]bool, len(channels))
		for _, channel := range channels {
			present[channel.Name] = true
		}
		for name, worker := range wakes {
			if !present[name] {
				worker.cancel()
				<-worker.done
				delete(wakes, name)
			}
		}
		for _, channel := range channels {
			current, ok := wakes[channel.Name]
			if !ok {
				workerCtx, stop := context.WithCancel(child)
				current = worker{wake: make(chan struct{}, 1), cancel: stop, done: make(chan struct{})}
				wakes[channel.Name] = current
				name := channel.Name
				workers.Add(1)
				go func() {
					defer workers.Done()
					defer close(current.done)
					for {
						select {
						case <-workerCtx.Done():
							return
						case <-current.wake:
							if err := e.DeliverDue(workerCtx, name); err != nil {
								if workerCtx.Err() != nil {
									return
								}
								select {
								case failures <- err:
								case <-child.Done():
								}
								return
							}
						}
					}
				}()
			}
			select {
			case current.wake <- struct{}{}:
			default:
			}
		}
		return nil
	}
	if err := drain(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-failures:
			return err
		case _, ok := <-live:
			if !ok {
				live = nil
				continue
			}
			if err := drain(); err != nil {
				return err
			}
		case <-ticks:
			if err := drain(); err != nil {
				return err
			}
		}
	}
}
