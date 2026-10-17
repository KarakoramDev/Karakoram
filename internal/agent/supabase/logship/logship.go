// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package logship pushes bounded batches of supervised process logs to a private
// Vector HTTP source. It never reports destination credentials or log contents.
package logship

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

// Source is satisfied by *agent.Node.
type Source interface {
	LogTail(string, int) []agentapi.LogLine
}

// Options contains the private sink and process identity.
type Options struct {
	URL, Token, Member string
	Processes          []string
	// Secrets are redacted literally before transmission; empty values are ignored.
	Secrets []string
}

// Start runs until ctx is cancelled. The returned cancel function stops polling
// and in-flight HTTP requests. Batches are at most 100 events of 8 KiB each.
// A failed batch is dropped rather than retaining unbounded application logs.
func Start(ctx context.Context, source Source, o Options) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	if o.URL == "" {
		return cancel
	}
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		defer client.CloseIdleConnections()
		cursors := map[string]time.Time{}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				batch := make([]event, 0, 100)
				flush := func() {
					if len(batch) == 0 {
						return
					}
					data, err := json.Marshal(batch)
					if err == nil {
						req, e := http.NewRequestWithContext(ctx, "POST", o.URL, bytes.NewReader(data))
						if e == nil {
							req.Header.Set("Content-Type", "application/json")
							if o.Token != "" {
								req.Header.Set("Authorization", "Bearer "+o.Token)
							}
							if r, e := client.Do(req); e == nil {
								io.Copy(io.Discard, io.LimitReader(r.Body, 4096))
								r.Body.Close()
							}
						}
					}
					batch = batch[:0]
				}
				for _, process := range o.Processes {
					for _, line := range source.LogTail(process, 1000) {
						if !line.Timestamp.After(cursors[process]) {
							continue
						}
						message := line.Message
						for _, secret := range o.Secrets {
							if secret != "" {
								message = strings.ReplaceAll(message, secret, "[REDACTED]")
							}
						}
						if len(message) > 8192 {
							message = message[:8192]
							for !utf8.ValidString(message) {
								message = message[:len(message)-1]
							}
						}
						batch = append(batch, event{o.Member, process, line.Timestamp, message})
						cursors[process] = line.Timestamp
						if len(batch) == 100 {
							flush()
						}
					}
				}
				flush()
			}
		}
	}()
	return cancel
}

type event struct {
	Member    string    `json:"member"`
	Process   string    `json:"process"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}
