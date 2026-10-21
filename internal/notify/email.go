// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package notify

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/smtp"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func (s *Sender) sendEmail(ctx context.Context, channel domain.NotificationChannel, event domain.Event) error {
	e := channel.Email
	subject := fmt.Sprintf("[Karakoram] %s %s %s/%s", severity(event), event.Reason, event.Namespace, event.Cluster)
	if strings.ContainsAny(subject, "\r\n") {
		return errors.New("invalid notification email header")
	}
	from, _ := mail.ParseAddress(e.From)
	recipients := make([]string, 0, len(e.To))
	for _, value := range e.To {
		address, _ := mail.ParseAddress(value)
		recipients = append(recipients, address.Address)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errors.New("notification email message ID failed")
	}
	message := "Date: " + s.now().UTC().Format(time.RFC1123Z) + "\r\nMessage-ID: <" + hex.EncodeToString(nonce[:]) + "@karakoram>\r\nFrom: " + e.From + "\r\nTo: " + strings.Join(e.To, ", ") + "\r\nSubject: " + subject + "\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: 8bit\r\n\r\n" + strings.ReplaceAll(strings.ReplaceAll(event.Message, "\r\n", "\n"), "\n", "\r\n") + "\r\n"
	conn, err := s.policy.DialContext(ctx, "tcp", net.JoinHostPort(e.Host, strconv.Itoa(e.Port)))
	if err != nil {
		return errors.New("notification SMTP connection failed")
	}
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		return errors.New("notification SMTP deadline failed")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: e.Host, RootCAs: s.rootCAs}
	var smtpConn net.Conn = conn
	if e.Security == "tls" {
		secured := tls.Client(conn, config)
		if err := secured.HandshakeContext(ctx); err != nil {
			return errors.New("notification SMTP TLS failed")
		}
		smtpConn = secured
	}
	client, err := smtp.NewClient(smtpConn, e.Host)
	if err != nil {
		return errors.New("notification SMTP greeting failed")
	}
	defer client.Close()
	if e.Security == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return errors.New("notification SMTP STARTTLS is required")
		}
		if err := client.StartTLS(config); err != nil {
			return errors.New("notification SMTP TLS failed")
		}
	}
	if state, ok := client.TLSConnectionState(); !ok || !state.HandshakeComplete {
		return errors.New("notification SMTP TLS is required")
	}
	if e.Username != "" {
		if err := client.Auth(smtp.PlainAuth("", e.Username, channel.Secrets.EmailPassword, e.Host)); err != nil {
			return errors.New("notification SMTP authentication failed")
		}
	}
	if err := client.Mail(from.Address); err != nil {
		return errors.New("notification SMTP sender rejected")
	}
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return errors.New("notification SMTP recipient rejected")
		}
	}
	writer, err := client.Data()
	if err != nil {
		return errors.New("notification SMTP DATA failed")
	}
	if _, err := writer.Write([]byte(message)); err != nil {
		writer.Close()
		return errors.New("notification SMTP message failed")
	}
	if err := writer.Close(); err != nil {
		return errors.New("notification SMTP message failed")
	}
	if err := client.Quit(); err != nil {
		return errors.New("notification SMTP completion failed")
	}
	return nil
}
