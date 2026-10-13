// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

func (a *app) waitOperation(ctx context.Context, id string, timeout time.Duration, print bool) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		op, err := a.request(ctx, "getOperation", []string{id}, nil, nil)
		if err != nil {
			return err
		}
		phase, _ := nested(op, "phase").(string)
		switch phase {
		case "Succeeded":
			if print {
				return a.print(op)
			}
			return nil
		case "Failed", "Canceled":
			return fmt.Errorf("operation %s %s: %s", id, phase, cell(nested(op, "error")))
		}
		if err = pollPause(ctx); err != nil {
			return fmt.Errorf("waiting for operation %s: %w", id, err)
		}
	}
}
func (a *app) waitMutation(ctx context.Context, op string, args []string, result any, q map[string]any, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if id, ok := nested(result, "id").(string); ok && id != "" {
		return a.waitOperation(ctx, id, timeout, true)
	}
	cluster, _ := nested(result, "name").(string)
	if cluster == "" && len(args) > 0 {
		cluster = args[0]
	}
	if cluster == "" {
		return fmt.Errorf("mutation response lacks cluster name or operation ID")
	}
	generation, _ := nested(result, "generation").(float64)
	for {
		observed, err := a.request(ctx, "getCluster", []string{cluster}, nil, nil)
		if err != nil {
			if op == "deleteCluster" && strings.HasPrefix(err.Error(), "HTTP 404:") {
				return a.print(map[string]any{"name": cluster, "status": "deleted"})
			}
			return err
		}
		phase, _ := nested(observed, "status", "phase").(string)
		if phase == "Error" || phase == "Degraded" {
			return fmt.Errorf("cluster %s is %s: %s", cluster, phase, cell(nested(observed, "status", "message")))
		}
		observedGeneration, _ := nested(observed, "status", "observedGeneration").(float64)
		targetPhase := "Ready"
		if op == "deleteCluster" {
			if keep, _ := q["keepVolumes"].(bool); keep {
				targetPhase = "Suspended"
			} else {
				targetPhase = ""
			}
		}
		if phase == targetPhase && observedGeneration >= generation {
			return a.print(observed)
		}
		if err = pollPause(ctx); err != nil {
			return fmt.Errorf("waiting for cluster %s: %w", cluster, err)
		}
	}
}
func (a *app) watch(ctx context.Context, q map[string]any) error {
	resp, err := dispatch(ctx, a.client, a.root, "streamEvents", nil, q, nil)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		_, err = decodeResponse(resp)
		return err
	}
	defer resp.Body.Close()
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		return fmt.Errorf("expected text/event-stream response")
	}
	reader := bufio.NewReader(resp.Body)
	data := []string{}
	emit := func() error {
		if len(data) == 0 {
			return nil
		}
		var event any
		raw := strings.Join(data, "\n")
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			return fmt.Errorf("invalid SSE event JSON: %w", err)
		}
		data = nil
		return a.print(event)
	}
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if err := emit(); err != nil {
					return err
				}
			} else if value, ok := strings.CutPrefix(line, "data:"); ok {
				data = append(data, strings.TrimPrefix(value, " "))
			}
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, io.EOF) {
				return emit()
			}
			return err
		}
	}
}
