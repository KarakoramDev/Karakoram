// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// SaveNotificationChannel persists public configuration and individually AAD-bound secrets.
func (s *Store) SaveNotificationChannel(ctx context.Context, c *domain.NotificationChannel, create bool) error {
	secret := c.Secrets
	fields := []*string{&secret.WebhookURL, &secret.WebhookSigningSecret, &secret.SlackWebhookURL, &secret.EmailPassword}
	names := []string{"webhook-url", "webhook-signing-secret", "slack-webhook-url", "email-password"}
	for i, p := range fields {
		if *p != "" {
			sealed, err := s.sealer.SealFor("notification/"+c.Name+"/"+names[i], []byte(*p))
			if err != nil {
				return err
			}
			*p = sealed
		}
	}
	raw, err := json.Marshal(secret)
	if err != nil {
		return err
	}
	return put(ctx, s.writer, "notification-channel", "", c.Name, c, string(raw), "", "", create)
}

// GetNotificationChannel returns public configuration and delivery status, never secrets.
func (s *Store) GetNotificationChannel(ctx context.Context, name string) (*domain.NotificationChannel, error) {
	var c domain.NotificationChannel
	_, _, err := s.get(ctx, "notification-channel", "", name, &c)
	if err != nil {
		return nil, err
	}
	c.Status, err = s.NotificationChannelStatus(ctx, name)
	return &c, err
}

// NotificationChannelForDelivery reads public configuration and sealed credentials
// from the same record snapshot. The read is released before decrypting or sending;
// a concurrent replacement can never pair one destination with another's password.
func (s *Store) NotificationChannelForDelivery(ctx context.Context, name string) (*domain.NotificationChannel, error) {
	var c domain.NotificationChannel
	raw, _, err := s.get(ctx, "notification-channel", "", name, &c)
	if err != nil {
		return nil, err
	}
	secret := &c.Secrets
	if err = json.Unmarshal([]byte(raw), secret); err != nil {
		return nil, err
	}
	fields := []*string{&secret.WebhookURL, &secret.WebhookSigningSecret, &secret.SlackWebhookURL, &secret.EmailPassword}
	names := []string{"webhook-url", "webhook-signing-secret", "slack-webhook-url", "email-password"}
	for i, p := range fields {
		if *p != "" {
			value, e := s.sealer.OpenFor("notification/"+name+"/"+names[i], *p)
			if e != nil {
				return nil, e
			}
			*p = string(value)
		}
	}
	return &c, nil
}
func (s *Store) ListNotificationChannels(ctx context.Context) ([]domain.NotificationChannel, error) {
	items, err := list[domain.NotificationChannel](ctx, s, "notification-channel", "")
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].Status, err = s.NotificationChannelStatus(ctx, items[i].Name)
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}
func (s *Store) DeleteNotificationChannel(ctx context.Context, name string) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "DELETE FROM records WHERE kind='notification-channel' AND scope='' AND key=?", name)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM notification_deliveries WHERE channel=? AND state='pending'", name); err != nil {
		return err
	}
	return tx.Commit()
}

// NotificationDelivery is durable retry state. Times use the injected engine clock.
type NotificationDelivery struct {
	Channel     string
	EventID     int64
	State       string
	Attempts    int
	NextAttempt time.Time
	LastError   string
	Created     time.Time
}

func (s *Store) NotificationCursor(ctx context.Context) (int64, error) {
	raw, err := s.GetSetting(ctx, "notify/cursor")
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var cursor int64
	err = json.Unmarshal(raw, &cursor)
	return cursor, err
}

// EnqueueNotifications atomically advances the cursor with all matching outbox rows.
// A stale scanner cannot move the cursor backwards or resurrect deleted channels.
func (s *Store) EnqueueNotifications(ctx context.Context, before, after int64, deliveries []NotificationDelivery, now time.Time) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var raw []byte
	err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='setting' AND scope='' AND key='notify/cursor'").Scan(&raw)
	var current int64
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &current); err != nil {
			return err
		}
	}
	if current != before {
		return ErrConflict
	}
	for _, d := range deliveries {
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO notification_deliveries(channel,event_id,state,attempts,next_attempt,last_error,created,updated)
SELECT ?,?,'pending',0,?,'',?,? WHERE EXISTS(SELECT 1 FROM records WHERE kind='notification-channel' AND scope='' AND key=?)`, d.Channel, d.EventID, now.UnixNano(), now.UnixNano(), now.UnixNano(), d.Channel)
		if err != nil {
			return err
		}
	}
	if err = put(ctx, tx, "setting", "", "notify/cursor", json.RawMessage(strconv.FormatInt(after, 10)), "", "", "", false); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) PendingNotificationDeliveries(ctx context.Context, channel string, now time.Time) ([]NotificationDelivery, error) {
	rows, err := s.reader.QueryContext(ctx, `SELECT event_id,attempts,next_attempt,last_error,created FROM notification_deliveries WHERE channel=? AND state='pending' AND next_attempt<=? ORDER BY event_id LIMIT 100`, channel, now.UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]NotificationDelivery, 0)
	for rows.Next() {
		d := NotificationDelivery{Channel: channel, State: "pending"}
		var next, created int64
		if err = rows.Scan(&d.EventID, &d.Attempts, &next, &d.LastError, &created); err != nil {
			return nil, err
		}
		d.NextAttempt = time.Unix(0, next).UTC()
		d.Created = time.Unix(0, created).UTC()
		out = append(out, d)
	}
	return out, rows.Err()
}
func (s *Store) CompleteNotificationAttempt(ctx context.Context, d NotificationDelivery, now time.Time) error {
	var delivered, failed any
	if d.State == "delivered" {
		delivered = now.UnixNano()
	}
	if d.LastError != "" {
		failed = now.UnixNano()
	}
	_, err := s.writer.ExecContext(ctx, `UPDATE notification_deliveries SET state=?,attempts=?,next_attempt=?,last_error=CASE WHEN ?='' THEN last_error ELSE ? END,updated=?,delivered_at=COALESCE(?,delivered_at),error_at=COALESCE(?,error_at) WHERE channel=? AND event_id=? AND state='pending'`, d.State, d.Attempts, d.NextAttempt.UnixNano(), d.LastError, d.LastError, now.UnixNano(), delivered, failed, d.Channel, d.EventID)
	return err
}
func (s *Store) NotificationChannelStatus(ctx context.Context, name string) (domain.NotificationChannelStatus, error) {
	var status domain.NotificationChannelStatus
	var delivered, failed sql.NullInt64
	err := s.reader.QueryRowContext(ctx, `SELECT COALESCE(SUM(state='pending'),0),COALESCE(SUM(state='failed'),0),MAX(delivered_at),MAX(error_at) FROM notification_deliveries WHERE channel=?`, name).Scan(&status.Pending, &status.Failed, &delivered, &failed)
	if err != nil {
		return status, err
	}
	if delivered.Valid {
		t := time.Unix(0, delivered.Int64).UTC()
		status.LastDeliveryAt = &t
	}
	if failed.Valid {
		t := time.Unix(0, failed.Int64).UTC()
		status.LastErrorAt = &t
		err = s.reader.QueryRowContext(ctx, `SELECT last_error FROM notification_deliveries WHERE channel=? AND error_at=? ORDER BY event_id DESC LIMIT 1`, name, failed.Int64).Scan(&status.LastError)
	}
	return status, err
}

// NotificationFailedCounts supplies karakoram_notification_deliveries_failed_total.
func (s *Store) NotificationFailedCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.reader.QueryContext(ctx, `SELECT channel,COUNT(*) FROM notification_deliveries WHERE state='failed' GROUP BY channel`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var channel string
		var n int64
		if err = rows.Scan(&channel, &n); err != nil {
			return nil, err
		}
		out[channel] = n
	}
	return out, rows.Err()
}
