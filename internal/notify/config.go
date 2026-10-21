// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package notify

import (
	"errors"
	"net/mail"
	"net/url"
	"regexp"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

var channelName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,39}$`)
var categories = map[domain.NotificationCategory]bool{
	domain.NotificationBackupFailed: true, domain.NotificationClusterDegraded: true, domain.NotificationClusterRecovered: true,
	domain.NotificationVolumeDeletionRefused: true, domain.NotificationCertificateExpiring: true, domain.NotificationDeploymentRetriesExhausted: true,
	domain.NotificationOperationFailed: true, domain.NotificationMembersCoLocated: true, domain.NotificationBackupDrillFailed: true,
	domain.NotificationControlPlaneBackupFailed: true, domain.NotificationAgentsOutdated: true,
}

func urlIdentity(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}

// Prepare validates configuration and retains omitted secrets only for unchanged destinations.
// previous, when supplied, must include explicitly loaded plaintext secrets.
func Prepare(c *domain.NotificationChannel, previous *domain.NotificationChannel) error {
	if !channelName.MatchString(c.Name) {
		return errors.New("channel name must be 1–40 lowercase letters, digits or hyphens, starting with a letter")
	}
	for _, category := range c.Categories {
		if !categories[category] {
			return errors.New("unsupported notification category")
		}
	}
	objects := 0
	if c.Webhook != nil {
		objects++
	}
	if c.Slack != nil {
		objects++
	}
	if c.Email != nil {
		objects++
	}
	if objects != 1 {
		return errors.New("exactly one matching destination configuration is required")
	}
	old := domain.NotificationChannel{}
	if previous != nil && previous.Type == c.Type {
		old = *previous
	}
	switch c.Type {
	case "webhook":
		if c.Webhook == nil {
			return errors.New("webhook configuration required")
		}
		if c.Secrets.WebhookURL == "" {
			c.Secrets.WebhookURL = old.Secrets.WebhookURL
		}
		if c.Secrets.WebhookSigningSecret == "" && old.Secrets.WebhookSigningSecret != "" {
			if urlIdentity(c.Secrets.WebhookURL) != urlIdentity(old.Secrets.WebhookURL) {
				return errors.New("destination changed; re-enter signing secret")
			}
			c.Secrets.WebhookSigningSecret = old.Secrets.WebhookSigningSecret
		}
		c.Webhook.Host = urlIdentity(c.Secrets.WebhookURL)
		c.Webhook.URLConfigured = c.Secrets.WebhookURL != ""
		c.Webhook.SigningSecretConfigured = c.Secrets.WebhookSigningSecret != ""
	case "slack":
		if c.Slack == nil {
			return errors.New("slack configuration required")
		}
		if c.Secrets.SlackWebhookURL == "" {
			c.Secrets.SlackWebhookURL = old.Secrets.SlackWebhookURL
		}
		c.Slack.WebhookURLConfigured = c.Secrets.SlackWebhookURL != ""
	case "email":
		if c.Email == nil {
			return errors.New("email configuration required")
		}
		m := c.Email
		if m.Security != "starttls" && m.Security != "tls" {
			return errors.New("email security must be starttls or tls")
		}
		if m.Port < 1 || m.Port > 65535 {
			return errors.New("email port must be 1–65535")
		}
		if strings.ContainsAny(m.Host+m.Username, "\r\n") {
			return errors.New("invalid email destination")
		}
		if len(m.To) < 1 || len(m.To) > 20 {
			return errors.New("email requires 1–20 recipients")
		}
		for _, address := range append([]string{m.From}, m.To...) {
			if strings.ContainsAny(address, "\r\n") {
				return errors.New("invalid email address")
			}
			if _, err := mail.ParseAddress(address); err != nil {
				return errors.New("invalid email address")
			}
		}
		if c.Secrets.EmailPassword == "" && old.Secrets.EmailPassword != "" {
			if old.Email == nil || !strings.EqualFold(m.Host, old.Email.Host) || m.Port != old.Email.Port || m.Username != old.Email.Username {
				return errors.New("destination changed; re-enter email password")
			}
			c.Secrets.EmailPassword = old.Secrets.EmailPassword
		}
		if m.Username != "" && c.Secrets.EmailPassword == "" {
			return errors.New("email password required for authentication")
		}
		m.PasswordConfigured = c.Secrets.EmailPassword != ""
	default:
		return errors.New("unsupported notification channel type")
	}
	return ValidateDestination(*c)
}
