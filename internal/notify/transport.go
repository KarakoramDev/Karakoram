// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package notify

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/netguard"
)

// Sender delivers notifications using the operator's outbound policy. Test-only
// trust roots and clocks remain private; production always uses system roots.
type Sender struct {
	policy  *netguard.Policy
	now     func() time.Time
	rootCAs *x509.CertPool
}

func NewSender() *Sender { return &Sender{policy: notificationPolicy(), now: time.Now} }
func notificationPolicy() *netguard.Policy {
	return &netguard.Policy{Allowlist: netguard.ParseAllowlist(os.Getenv("KARAKORAM_NOTIFY_ENDPOINT_ALLOWLIST"))}
}

func ValidateDestination(channel domain.NotificationChannel) error {
	return validateDestination(channel, notificationPolicy())
}
func validateDestination(channel domain.NotificationChannel, policy *netguard.Policy) error {
	switch channel.Type {
	case "webhook", "slack":
		value := channel.Secrets.WebhookURL
		if channel.Type == "slack" {
			value = channel.Secrets.SlackWebhookURL
		}
		u, err := url.Parse(value)
		if err != nil || policy.CheckURL(u, true) != nil {
			return errors.New("notification endpoint is not permitted")
		}
	case "email":
		e := channel.Email
		if e == nil || e.Host == "" || strings.ContainsAny(e.Host, "\r\n /@") || e.Port < 1 || e.Port > 65535 || (e.Security != "starttls" && e.Security != "tls") || len(e.To) < 1 || len(e.To) > 20 {
			return errors.New("invalid notification email configuration")
		}
		for _, value := range append([]string{e.From}, e.To...) {
			if strings.ContainsAny(value, "\r\n") {
				return errors.New("invalid notification email header")
			}
			if _, err := mail.ParseAddress(value); err != nil {
				return errors.New("invalid notification email address")
			}
		}
	default:
		return errors.New("unsupported notification channel type")
	}
	return nil
}
func severity(event domain.Event) string {
	if strings.EqualFold(event.Type, "warning") {
		return "warning"
	}
	return "info"
}
func summary(event domain.Event) string {
	return fmt.Sprintf("%s %s/%s %s: %s", severity(event), event.Namespace, event.Cluster, event.Reason, event.Message)
}

func (s *Sender) Send(ctx context.Context, channel domain.NotificationChannel, event domain.Event, category domain.NotificationCategory) error {
	if err := validateDestination(channel, s.policy); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if channel.Type == "email" {
		return s.sendEmail(ctx, channel, event)
	}
	value := channel.Secrets.WebhookURL
	var payload any = struct {
		ID        int64                       `json:"id"`
		Category  domain.NotificationCategory `json:"category"`
		Severity  string                      `json:"severity"`
		Namespace string                      `json:"namespace"`
		Cluster   string                      `json:"cluster"`
		Reason    string                      `json:"reason"`
		Message   string                      `json:"message"`
		CreatedAt time.Time                   `json:"createdAt"`
		Source    string                      `json:"source"`
	}{event.ID, category, severity(event), event.Namespace, event.Cluster, event.Reason, event.Message, event.CreatedAt, "karakoram"}
	if channel.Type == "slack" {
		value = channel.Secrets.SlackWebhookURL
		payload = struct {
			Text string `json:"text"`
		}{strings.Join(strings.Fields(summary(event)), " ")}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return errors.New("notification payload encoding failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, value, bytes.NewReader(body))
	if err != nil {
		return errors.New("notification request creation failed")
	}
	request.Header.Set("Content-Type", "application/json")
	if channel.Type == "webhook" {
		request.Header.Set("X-Karakoram-Event-Id", strconv.FormatInt(event.ID, 10))
		if secret := channel.Secrets.WebhookSigningSecret; secret != "" {
			timestamp := strconv.FormatInt(s.now().Unix(), 10)
			mac := hmac.New(sha256.New, []byte(secret))
			mac.Write([]byte(timestamp))
			mac.Write([]byte("."))
			mac.Write(body)
			request.Header.Set("X-Karakoram-Signature", "t="+timestamp+",v1="+hex.EncodeToString(mac.Sum(nil)))
		}
	}
	client := s.policy.HTTPClient(10 * time.Second)
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	if err != nil {
		return errors.New("notification HTTP delivery failed")
	}
	defer response.Body.Close()
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("notification HTTP status %d", response.StatusCode)
	}
	if err != nil {
		return errors.New("notification HTTP response failed")
	}
	return nil
}
