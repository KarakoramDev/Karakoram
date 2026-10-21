// SPDX-License-Identifier: Apache-2.0
package notify

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"testing"
)

func TestNotificationKeepOnOmitIdentity(t *testing.T) {
	previous := domain.NotificationChannel{Name: "mail", Type: "email", Email: &domain.NotificationEmail{Host: "smtp.example.com", Port: 587, Security: "starttls", Username: "user", From: "ops@example.com", To: []string{"oncall@example.com"}}, Secrets: domain.NotificationChannelSecrets{EmailPassword: "password"}}
	for _, tc := range []struct {
		name   string
		change func(*domain.NotificationEmail)
		valid  bool
	}{
		{"same", func(*domain.NotificationEmail) {}, true},
		{"host", func(m *domain.NotificationEmail) { m.Host = "other.example.com" }, false},
		{"port", func(m *domain.NotificationEmail) { m.Port = 465 }, false},
		{"username", func(m *domain.NotificationEmail) { m.Username = "other" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := previous
			m := *previous.Email
			c.Email = &m
			c.Secrets = domain.NotificationChannelSecrets{}
			tc.change(c.Email)
			err := Prepare(&c, &previous)
			if (err == nil) != tc.valid {
				t.Fatal("unexpected result", err)
			}
			if tc.valid && c.Secrets.EmailPassword != "password" {
				t.Fatal("password not retained")
			}
			if !tc.valid {
				c.Secrets.EmailPassword = "replacement"
				if err = Prepare(&c, &previous); err != nil {
					t.Fatal("re-entry rejected", err)
				}
			}
		})
	}
}
func TestNotificationWebhookSameHostRetainsSigningSecret(t *testing.T) {
	previous := domain.NotificationChannel{Name: "web", Type: "webhook", Webhook: &domain.NotificationWebhook{}, Secrets: domain.NotificationChannelSecrets{WebhookURL: "https://example.com/old", WebhookSigningSecret: "secret"}}
	c := domain.NotificationChannel{Name: "web", Type: "webhook", Webhook: &domain.NotificationWebhook{}, Secrets: domain.NotificationChannelSecrets{WebhookURL: "https://example.com/new"}}
	if err := Prepare(&c, &previous); err != nil || c.Secrets.WebhookSigningSecret != "secret" {
		t.Fatal("same host rejected", err)
	}
	c.Secrets = domain.NotificationChannelSecrets{WebhookURL: "https://different.example/new"}
	if err := Prepare(&c, &previous); err == nil {
		t.Fatal("host change reused signing secret")
	}
}
func TestNotificationEmailRejectsHeaderInjection(t *testing.T) {
	c := domain.NotificationChannel{Name: "mail", Type: "email", Email: &domain.NotificationEmail{Host: "smtp.example.com", Port: 587, Security: "starttls", From: "ops@example.com\r\nBcc: victim@example.com", To: []string{"oncall@example.com"}}}
	if err := Prepare(&c, nil); err == nil {
		t.Fatal("header injection accepted")
	}
}
