// SPDX-License-Identifier: Apache-2.0
package store

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestNotificationSecretsSealedAndBound(t *testing.T) {
	s, _ := testStore(t)
	c := domain.NotificationChannel{Name: "alerts", Type: "webhook", Secrets: domain.NotificationChannelSecrets{WebhookURL: "https://example.com/private-token", WebhookSigningSecret: "signing-secret"}}
	if err := s.SaveNotificationChannel(t.Context(), &c, true); err != nil {
		t.Fatal(err)
	}
	var raw, secret string
	if err := s.reader.QueryRowContext(t.Context(), `SELECT data,secret FROM records WHERE kind='notification-channel' AND key='alerts'`).Scan(&raw, &secret); err != nil {
		t.Fatal(err)
	}
	for _, plain := range []string{"private-token", "signing-secret"} {
		if strings.Contains(raw, plain) || strings.Contains(secret, plain) {
			t.Fatal("plaintext persisted")
		}
	}
	got, err := s.GetNotificationChannel(t.Context(), c.Name)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(got)
	if strings.Contains(string(encoded), "private-token") || got.Secrets.WebhookURL != "" {
		t.Fatal("secret read-back")
	}
	loaded, err := s.NotificationChannelForDelivery(t.Context(), c.Name)
	if err != nil || loaded.Secrets != c.Secrets {
		t.Fatal("roundtrip failed", err)
	}
	var sealed domain.NotificationChannelSecrets
	if err = json.Unmarshal([]byte(secret), &sealed); err != nil {
		t.Fatal(err)
	}
	if _, err = s.sealer.OpenFor("notification/another/webhook-url", sealed.WebhookURL); err == nil {
		t.Fatal("AAD substitution accepted")
	}
}
func TestNotificationCursorAtomicAndDeletedChannel(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	now := time.Now()
	c := domain.NotificationChannel{Name: "alerts"}
	if err := s.SaveNotificationChannel(ctx, &c, true); err != nil {
		t.Fatal(err)
	}
	deliveries := []NotificationDelivery{{Channel: c.Name, EventID: 1}}
	if err := s.EnqueueNotifications(ctx, 0, 1, deliveries, now); err != nil {
		t.Fatal(err)
	}
	if err := s.EnqueueNotifications(ctx, 0, 2, []NotificationDelivery{{Channel: c.Name, EventID: 2}}, now); !errors.Is(err, ErrConflict) {
		t.Fatal("stale cursor accepted", err)
	}
	cursor, err := s.NotificationCursor(ctx)
	if err != nil || cursor != 1 {
		t.Fatal(cursor, err)
	}
	if err = s.DeleteNotificationChannel(ctx, c.Name); err != nil {
		t.Fatal(err)
	}
	if err = s.EnqueueNotifications(ctx, 1, 2, deliveries, now); err != nil {
		t.Fatal(err)
	}
	pending, err := s.PendingNotificationDeliveries(ctx, c.Name, now)
	if err != nil || len(pending) != 0 {
		t.Fatal("deleted channel resurrected", pending, err)
	}
}
