// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package notify

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/netguard"
)

type smtpResult struct {
	message       string
	authenticated bool
	err           error
}

func fakeSMTP(t *testing.T, mode string, advertise bool) (string, *x509.CertPool, <-chan smtpResult) {
	t.Helper()
	certificateServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := certificateServer.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(certificateServer.Certificate())
	certificateServer.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	result := make(chan smtpResult, 1)
	go func() {
		var out smtpResult
		defer func() { result <- out }()
		conn, err := listener.Accept()
		if err != nil {
			out.err = err
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		secured := false
		config := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{certificate}}
		if mode == "tls" {
			conn = tls.Server(conn, config)
			secured = true
		}
		reader := bufio.NewReader(conn)
		send := func(value string) bool {
			_, err := io.WriteString(conn, value+"\r\n")
			if err != nil {
				out.err = err
				return false
			}
			return true
		}
		if !send("220 local ESMTP") {
			return
		}
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					out.err = err
				}
				return
			}
			line = strings.TrimSpace(line)
			switch {
			case strings.HasPrefix(line, "EHLO"):
				if secured {
					if !send("250-local\r\n250 AUTH PLAIN") {
						return
					}
				} else if advertise {
					if !send("250-local\r\n250 STARTTLS") {
						return
					}
				} else {
					if !send("250 local") {
						return
					}
				}
			case line == "STARTTLS":
				if !advertise {
					out.err = fmt.Errorf("STARTTLS not advertised")
					return
				}
				if !send("220 ready") {
					return
				}
				tlsConn := tls.Server(conn, config)
				if err := tlsConn.Handshake(); err != nil {
					out.err = err
					return
				}
				conn = tlsConn
				reader = bufio.NewReader(conn)
				secured = true
			case strings.HasPrefix(line, "AUTH "):
				if !secured {
					out.err = fmt.Errorf("AUTH before TLS")
					return
				}
				out.authenticated = true
				if !send("235 authenticated") {
					return
				}
			case strings.HasPrefix(line, "MAIL FROM:"), strings.HasPrefix(line, "RCPT TO:"):
				if !secured {
					out.err = fmt.Errorf("envelope before TLS")
					return
				}
				if !send("250 OK") {
					return
				}
			case line == "DATA":
				if !send("354 send message") {
					return
				}
				var message strings.Builder
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						out.err = err
						return
					}
					if line == ".\r\n" {
						break
					}
					message.WriteString(line)
				}
				out.message = message.String()
				if !send("250 queued") {
					return
				}
			case line == "QUIT":
				send("221 goodbye")
				return
			default:
				out.err = fmt.Errorf("unexpected SMTP command")
				return
			}
		}
	}()
	return listener.Addr().String(), roots, result
}
func TestTransportSMTP(t *testing.T) {
	for _, mode := range []string{"starttls", "tls"} {
		for _, auth := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/auth=%t", mode, auth), func(t *testing.T) {
				address, roots, result := fakeSMTP(t, mode, true)
				host, port, _ := net.SplitHostPort(address)
				number, _ := strconv.Atoi(port)
				sender := NewSender()
				sender.policy = &netguard.Policy{Allowlist: netguard.ParseAllowlist(address)}
				sender.rootCAs = roots
				channel := domain.NotificationChannel{Type: "email", Email: &domain.NotificationEmail{Host: host, Port: number, Security: mode, From: "Karakoram <sender@example.test>", To: []string{"recipient@example.test"}}}
				if auth {
					channel.Email.Username = "user"
					channel.Secrets.EmailPassword = "password"
				}
				event := domain.Event{Namespace: "ns", Cluster: "db", Type: "Warning", Reason: "Failed", Message: "failure details"}
				if err := sender.Send(t.Context(), channel, event, domain.NotificationBackupFailed); err != nil {
					t.Fatal(err)
				}
				got := <-result
				if got.err != nil {
					t.Fatal(got.err)
				}
				if got.authenticated != auth {
					t.Fatalf("auth=%v", got.authenticated)
				}
				for _, header := range []string{"Date: ", "Message-ID: <", "From: Karakoram <sender@example.test>", "To: recipient@example.test", "Subject: [Karakoram] warning Failed ns/db", "Content-Type: text/plain; charset=UTF-8", "\r\n\r\nfailure details\r\n"} {
					if !strings.Contains(got.message, header) {
						t.Errorf("missing %q", header)
					}
				}
			})
		}
	}
}
func TestTransportSMTPRequiresSTARTTLS(t *testing.T) {
	address, roots, result := fakeSMTP(t, "starttls", false)
	host, port, _ := net.SplitHostPort(address)
	number, _ := strconv.Atoi(port)
	sender := NewSender()
	sender.policy = &netguard.Policy{Allowlist: netguard.ParseAllowlist(address)}
	sender.rootCAs = roots
	channel := domain.NotificationChannel{Type: "email", Email: &domain.NotificationEmail{Host: host, Port: number, Security: "starttls", Username: "user", From: "from@example.test", To: []string{"to@example.test"}}, Secrets: domain.NotificationChannelSecrets{EmailPassword: "secret"}}
	if err := sender.Send(t.Context(), channel, domain.Event{}, ""); err == nil || err.Error() != "notification SMTP STARTTLS is required" {
		t.Fatalf("error %v", err)
	}
	got := <-result
	if got.authenticated || got.message != "" {
		t.Fatal("sent plaintext credentials or message")
	}
}
func TestTransportSMTPHeaderInjection(t *testing.T) {
	sender := NewSender()
	for _, field := range []string{"from", "to", "reason", "namespace", "cluster"} {
		t.Run(field, func(t *testing.T) {
			channel := domain.NotificationChannel{Type: "email", Email: &domain.NotificationEmail{Host: "127.0.0.1", Port: 1, Security: "tls", From: "from@example.test", To: []string{"to@example.test"}}}
			event := domain.Event{}
			switch field {
			case "from":
				channel.Email.From += "\r\nBcc: bad@example.test"
			case "to":
				channel.Email.To[0] += "\nBcc: bad@example.test"
			case "reason":
				event.Reason = "bad\r\nBcc: bad"
			case "namespace":
				event.Namespace = "bad\nBcc: bad"
			case "cluster":
				event.Cluster = "bad\rBcc: bad"
			}
			if err := sender.Send(t.Context(), channel, event, ""); err == nil || err.Error() != "invalid notification email header" {
				t.Fatalf("header error %v", err)
			}
		})
	}
}
func TestTransportSMTPUntrustedTLS(t *testing.T) {
	address, _, result := fakeSMTP(t, "tls", true)
	host, port, _ := net.SplitHostPort(address)
	number, _ := strconv.Atoi(port)
	sender := NewSender()
	sender.policy = &netguard.Policy{Allowlist: netguard.ParseAllowlist(address)}
	channel := domain.NotificationChannel{Type: "email", Email: &domain.NotificationEmail{Host: host, Port: number, Security: "tls", From: "from@example.test", To: []string{"to@example.test"}}}
	if err := sender.Send(t.Context(), channel, domain.Event{}, ""); err == nil || err.Error() != "notification SMTP TLS failed" {
		t.Fatalf("untrusted TLS error %v", err)
	}
	<-result
}
