// SPDX-License-Identifier: Apache-2.0
package notify

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/events"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type recordingSender struct {
	calls int
	fail  bool
}

func (s *recordingSender) Send(context.Context, domain.NotificationChannel, domain.Event, domain.NotificationCategory) error {
	s.calls++
	if s.fail {
		return errors.New("secret unsafe upstream error")
	}
	return nil
}
func notificationStore(t *testing.T) (*store.Store, string, *secrets.Sealer) {
	t.Helper()
	key, err := secrets.New(strings.Repeat("n", 32))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "state.db")
	st, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, path, key
}
func TestNotificationCrashRestartOutboxAndBackoff(t *testing.T) {
	st, path, key := notificationStore(t)
	ctx := t.Context()
	now := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	channel := domain.NotificationChannel{Name: "alerts", Type: "webhook", Enabled: true, Webhook: &domain.NotificationWebhook{}, Secrets: domain.NotificationChannelSecrets{WebhookURL: "https://example.com/secret"}}
	if err := st.SaveNotificationChannel(ctx, &channel, true); err != nil {
		t.Fatal(err)
	}
	event := domain.Event{Type: "Warning", Reason: "BackupFailed", Message: "failed", CreatedAt: now}
	if err := st.AppendEvent(ctx, &event); err != nil {
		t.Fatal(err)
	}
	sender := &recordingSender{fail: true}
	engine := New(Options{Store: st, Sender: sender, Now: func() time.Time { return now }})
	if err := engine.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := engine.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	cursor, err := st.NotificationCursor(ctx)
	if err != nil || cursor != event.ID {
		t.Fatal(cursor, err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	engine = New(Options{Store: reopened, Sender: sender, Now: func() time.Time { return now }})
	if err = engine.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if err = engine.DeliverDue(ctx, "alerts"); err != nil {
		t.Fatal(err)
	}
	pending, err := reopened.PendingNotificationDeliveries(ctx, "alerts", now.Add(30*time.Second))
	if err != nil || len(pending) != 1 || pending[0].Attempts != 1 || !pending[0].NextAttempt.Equal(now.Add(30*time.Second)) {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if err = engine.DeliverDue(ctx, "alerts"); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 {
		t.Fatal("early retry", sender.calls)
	}
	now = now.Add(30 * time.Second)
	sender.fail = false
	if err = engine.DeliverDue(ctx, "alerts"); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 2 {
		t.Fatal(sender.calls)
	}
	status, err := reopened.NotificationChannelStatus(ctx, "alerts")
	if err != nil || status.Pending != 0 || status.LastDeliveryAt == nil || status.LastErrorAt == nil || strings.Contains(status.LastError, "secret") {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if err = engine.DeliverDue(ctx, "alerts"); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 2 {
		t.Fatal("delivered event repeated")
	}
}
func TestNotificationGiveUpAndDelete(t *testing.T) {
	st, _, _ := notificationStore(t)
	now := time.Now().UTC()
	sender := &recordingSender{fail: true}
	engine := New(Options{Store: st, Sender: sender, Now: func() time.Time { return now }})
	c := domain.NotificationChannel{Name: "alerts", Enabled: true}
	if err := st.SaveNotificationChannel(t.Context(), &c, true); err != nil {
		t.Fatal(err)
	}
	event := domain.Event{Type: "Warning", Reason: "AgentsOutdated"}
	if err := st.AppendEvent(t.Context(), &event); err != nil {
		t.Fatal(err)
	}
	if err := engine.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(24 * time.Hour)
	if err := engine.DeliverDue(t.Context(), c.Name); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 0 {
		t.Fatal("expired delivery sent")
	}
	counts, err := st.NotificationFailedCounts(t.Context())
	if err != nil || counts[c.Name] != 1 {
		t.Fatal(counts, err)
	}
	event.ID = 0
	if err := st.AppendEvent(t.Context(), &event); err != nil {
		t.Fatal(err)
	}
	if err := engine.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteNotificationChannel(t.Context(), c.Name); err != nil {
		t.Fatal(err)
	}
	due, err := st.PendingNotificationDeliveries(t.Context(), c.Name, now)
	if err != nil || len(due) != 0 {
		t.Fatal(due, err)
	}
}
func TestNotificationRetryDelay(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{{1, 30 * time.Second}, {2, time.Minute}, {3, 2 * time.Minute}, {8, time.Hour}, {100, time.Hour}} {
		if got := retryDelay(tc.attempt); got != tc.want {
			t.Fatal(tc, got)
		}
	}
}

type wakeSender chan int64

func (s wakeSender) Send(ctx context.Context, _ domain.NotificationChannel, event domain.Event, _ domain.NotificationCategory) error {
	select {
	case s <- event.ID:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestNotificationPeriodicDrainRecoversMissingHubSignals(t *testing.T) {
	st, _, _ := notificationStore(t)
	c := domain.NotificationChannel{Name: "alerts", Enabled: true}
	if err := st.SaveNotificationChannel(t.Context(), &c, true); err != nil {
		t.Fatal(err)
	}
	first := domain.Event{Type: "Warning", Reason: "AgentsOutdated"}
	if err := st.AppendEvent(t.Context(), &first); err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time)
	sender := make(wakeSender)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engine := New(Options{Store: st, Hub: events.NewHub(1), Sender: sender, Ticks: ticks})
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	receive := func(want int64) {
		t.Helper()
		select {
		case id := <-sender:
			if id != want {
				t.Fatalf("event %d, want %d", id, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("delivery did not wake")
		}
	}
	receive(first.ID)
	second := domain.Event{Type: "Warning", Reason: "BackupFailed"}
	if err := st.AppendEvent(t.Context(), &second); err != nil {
		t.Fatal(err)
	}
	// Deliberately do not publish: the durable periodic drain is authoritative.
	select {
	case ticks <- time.Now():
	case <-time.After(5 * time.Second):
		t.Fatal("periodic drain blocked")
	}
	receive(second.ID)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}

type channelSenderFunc func(context.Context, domain.NotificationChannel, domain.Event, domain.NotificationCategory) error

func (f channelSenderFunc) Send(ctx context.Context, channel domain.NotificationChannel, event domain.Event, category domain.NotificationCategory) error {
	return f(ctx, channel, event, category)
}

func TestNotificationDeliveryReplacementKeepsSMTPIdentityTogether(t *testing.T) {
	st, _, _ := notificationStore(t)
	ctx := t.Context()
	channel := domain.NotificationChannel{Name: "mail", Type: "email", Enabled: true,
		Email:   &domain.NotificationEmail{Host: "smtp-a.example", Username: "user-a"},
		Secrets: domain.NotificationChannelSecrets{EmailPassword: "password-a"}}
	if err := st.SaveNotificationChannel(ctx, &channel, true); err != nil {
		t.Fatal(err)
	}
	calls := 0
	sender := channelSenderFunc(func(_ context.Context, got domain.NotificationChannel, _ domain.Event, _ domain.NotificationCategory) error {
		calls++
		identity := "a"
		if calls > 1 {
			identity = "b"
		}
		if got.Email.Host != "smtp-"+identity+".example" || got.Email.Username != "user-"+identity || got.Secrets.EmailPassword != "password-"+identity {
			t.Fatalf("SMTP destination and credentials differ: host=%s user=%s", got.Email.Host, got.Email.Username)
		}
		if calls == 1 {
			// Mutate while the transport owns its snapshot. No database lock may
			// span the network send, and the next batch must use the replacement.
			channel.Email.Host = "smtp-b.example"
			channel.Email.Username = "user-b"
			channel.Secrets.EmailPassword = "password-b"
			if err := st.SaveNotificationChannel(ctx, &channel, false); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	})
	engine := New(Options{Store: st, Sender: sender})
	for range 2 {
		event := domain.Event{Type: "Warning", Reason: "BackupFailed"}
		if err := st.AppendEvent(ctx, &event); err != nil {
			t.Fatal(err)
		}
		if err := engine.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		if err := engine.DeliverDue(ctx, channel.Name); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatal("missing delivery", calls)
	}
}

func TestNotificationConcurrentDeleteDoesNotStopEngine(t *testing.T) {
	st, _, _ := notificationStore(t)
	channel := domain.NotificationChannel{Name: "deleted", Enabled: true}
	if err := st.SaveNotificationChannel(t.Context(), &channel, true); err != nil {
		t.Fatal(err)
	}
	first := domain.Event{Type: "Warning", Reason: "BackupFailed"}
	if err := st.AppendEvent(t.Context(), &first); err != nil {
		t.Fatal(err)
	}
	entered := make(chan string, 2)
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	sender := channelSenderFunc(func(ctx context.Context, got domain.NotificationChannel, _ domain.Event, _ domain.NotificationCategory) error {
		entered <- got.Name
		if got.Name == "deleted" {
			select {
			case <-release:
				return errors.New("transport failed after deletion")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ticks := make(chan time.Time)
	engine := New(Options{Store: st, Sender: sender, Ticks: ticks})
	done := make(chan error, 1)
	go func() { done <- engine.Run(ctx) }()
	receive := func(want string) {
		t.Helper()
		select {
		case got := <-entered:
			if got != want {
				t.Fatalf("channel %s, want %s", got, want)
			}
		case err := <-done:
			t.Fatal("engine stopped on deletion", err)
		case <-time.After(5 * time.Second):
			t.Fatal("delivery did not start")
		}
	}
	receive("deleted")
	if err := st.DeleteNotificationChannel(t.Context(), "deleted"); err != nil {
		t.Fatal(err)
	}
	close(release)
	// A stale worker wake after DELETE is an ordinary no-op, not ErrNotFound.
	if err := engine.DeliverDue(t.Context(), "deleted"); err != nil {
		t.Fatal(err)
	}
	channel.Name = "survivor"
	if err := st.SaveNotificationChannel(t.Context(), &channel, true); err != nil {
		t.Fatal(err)
	}
	second := domain.Event{Type: "Warning", Reason: "BackupFailed"}
	if err := st.AppendEvent(t.Context(), &second); err != nil {
		t.Fatal(err)
	}
	select {
	case ticks <- time.Now():
	case err := <-done:
		t.Fatal("engine stopped on deletion", err)
	case <-time.After(5 * time.Second):
		t.Fatal("drain blocked")
	}
	receive("survivor")
	pending, err := st.PendingNotificationDeliveries(t.Context(), "deleted", time.Now().Add(24*time.Hour))
	if err != nil || len(pending) != 0 {
		t.Fatal("deleted pending deliveries resurrected", pending, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not stop")
	}
}
