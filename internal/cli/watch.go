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
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func waitFailure(ctx context.Context, err error, resume string) error {
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return fmt.Errorf("%w; work continues server-side. Resume waiting without resubmitting: %s", err, resume)
}

func transientObservationError(err error) bool {
	var response *commandError
	if errors.As(err, &response) {
		switch response.httpStatus {
		case 408, 429, 502, 503, 504:
			return true
		}
		return false
	}
	var dns *net.DNSError
	if errors.As(err, &dns) && dns.IsNotFound {
		return false
	}
	var network net.Error
	return errors.As(err, &network) && network.Timeout() ||
		errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func retryAfterDelay(value string, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(value, 10, 32); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) {
		return date.Sub(now)
	}
	return 0
}

func observationRetryDelay(ctx context.Context, err error, backoff time.Duration) time.Duration {
	var response *commandError
	if errors.As(err, &response) && response.retryAfter > backoff {
		backoff = response.retryAfter
	}
	if deadline, ok := ctx.Deadline(); ok {
		if remaining := time.Until(deadline); remaining < backoff {
			return max(remaining, 0)
		}
	}
	return backoff
}

// Observation retries never resubmit the accepted mutation.
func (a *app) observe(ctx context.Context, op, id string) (any, error) {
	delay := 100 * time.Millisecond
	for {
		result, err := a.request(ctx, op, []string{id}, nil, nil)
		if err == nil || !transientObservationError(err) || ctx.Err() != nil {
			return result, err
		}
		pause := observationRetryDelay(ctx, err, delay)
		fmt.Fprintf(a.opts.Err, "Observation temporarily unavailable; retrying in %s: %s\n", pause, err)
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%w (last observation error: %v)", ctx.Err(), err)
		case <-timer.C:
		}
		if delay < 2*time.Second {
			delay *= 2
			if delay > 2*time.Second {
				delay = 2 * time.Second
			}
		}
	}
}

func (a *app) acceptedOperationFailure(ctx context.Context, err error, id string, timeout time.Duration) error {
	return fmt.Errorf("operation %s was accepted; %w", id, waitFailure(ctx, err, a.resumeCommand("operations", id, timeout)))
}

func operationMessage(op any) string {
	for _, key := range []string{"message", "error", "progress"} {
		if value := nested(op, key); value != nil && value != "" {
			return cell(value)
		}
	}
	return "no further details reported; inspect events"
}

func (a *app) resumeCommand(kind, name string, timeout time.Duration) string {
	contextFlag := ""
	if _, exists := a.config.Contexts[a.selected]; exists {
		contextFlag = fmt.Sprintf(" --context %q", a.selected)
	}
	return fmt.Sprintf("karactl%s --server %q --namespace %q %s wait %q --timeout %s", contextFlag, a.server, a.namespace, kind, name, timeout)
}

func (a *app) waitOperation(ctx context.Context, id string, timeout time.Duration, print bool) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fmt.Fprintf(a.opts.Err, "Waiting for operation %s.\n", id)
	last := ""
	lastPrinted := time.Time{}
	for {
		op, err := a.observe(ctx, "getOperation", id)
		if err != nil {
			return a.acceptedOperationFailure(ctx, err, id, timeout)
		}
		phase, _ := nested(op, "phase").(string)
		progress := phase + ": " + operationMessage(op)
		if progress != last || time.Since(lastPrinted) >= 15*time.Second {
			fmt.Fprintf(a.opts.Err, "Operation %s: %s\n", id, progress)
			last, lastPrinted = progress, time.Now()
		}
		switch phase {
		case "Succeeded":
			if print {
				return a.print(op)
			}
			return nil
		case "Failed", "Canceled", "Cancelled":
			return fmt.Errorf("operation %s %s: %s", id, phase, operationMessage(op))
		}
		if err = pollPause(ctx); err != nil {
			return a.acceptedOperationFailure(ctx, err, id, timeout)
		}
	}
}

func (a *app) waitMutation(ctx context.Context, op string, args []string, result any, q map[string]any, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	id, _ := nested(result, "operationId").(string)
	if id == "" {
		id, _ = nested(result, "id").(string)
	}
	cluster, _ := nested(result, "name").(string)
	if cluster == "" && len(args) > 0 {
		cluster = args[0]
	}
	generation, _ := nested(result, "generation").(float64)
	if generation == 0 {
		generation, _ = nested(result, "targetGeneration").(float64)
	}
	if id != "" {
		fmt.Fprintf(a.opts.Err, "Accepted %s for cluster %s; operation %s.\n", mutationDescription(op), cluster, id)
		if err := a.waitOperation(ctx, id, timeout, cluster == ""); err != nil {
			return err
		}
		if cluster == "" {
			return nil
		}
	} else {
		fmt.Fprintf(a.opts.Err, "Accepted %s for cluster %s; generation %.0f.\n", mutationDescription(op), cluster, generation)
	}
	if cluster == "" {
		return fmt.Errorf("mutation response lacks cluster name or operation ID")
	}
	return a.waitCluster(ctx, op, cluster, generation, q, timeout)
}

func mutationDescription(op string) string {
	switch op {
	case "createCluster":
		return "creation"
	case "deleteCluster":
		return "deletion"
	case "patchCluster", "replaceCluster":
		return "update"
	case "replaceClusterMember":
		return "member replacement"
	case "restartCluster":
		return "restart"
	case "resumeCluster":
		return "resume"
	case "rotateClusterTLS":
		return "TLS rotation"
	case "raiseClusterFCV":
		return "compatibility-version change"
	default:
		return "request"
	}
}

func (a *app) waitCluster(ctx context.Context, op, cluster string, generation float64, q map[string]any, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	fmt.Fprintf(a.opts.Err, "Waiting for cluster %s.\n", cluster)
	last := ""
	lastPrinted := time.Time{}
	deleting := op == "deleteCluster"
	for {
		observed, err := a.observe(ctx, "getCluster", cluster)
		if err != nil {
			if deleting && ExitCode(err) == 3 {
				return a.print(map[string]any{"name": cluster, "status": "deleted"})
			}
			return waitFailure(ctx, err, a.resumeCommand("clusters", cluster, timeout))
		}
		phase, _ := nested(observed, "status", "phase").(string)
		if phase == "Deleting" {
			deleting = true
		}
		message := cell(nested(observed, "status", "message"))
		observedGeneration, _ := nested(observed, "status", "observedGeneration").(float64)
		currentGeneration, _ := nested(observed, "generation").(float64)
		if generation == 0 {
			generation = currentGeneration
		}
		progress := fmt.Sprintf("%s; generation %.0f/%.0f; %s", phase, observedGeneration, generation, message)
		if progress != last || time.Since(lastPrinted) >= 15*time.Second {
			fmt.Fprintf(a.opts.Err, "Cluster %s: %s\n", cluster, progress)
			last, lastPrinted = progress, time.Now()
		}
		if phase == "Error" || phase == "Degraded" {
			return fmt.Errorf("cluster %s is %s: %s", cluster, phase, message)
		}
		targetPhase := "Ready"
		if op == "deleteCluster" {
			if keep, _ := q["keepVolumes"].(bool); keep {
				targetPhase = "Suspended"
			} else {
				targetPhase = ""
			}
		}
		if (phase == targetPhase || op == "observe" && phase == "Suspended") && observedGeneration >= generation {
			return a.print(observed)
		}
		if err = pollPause(ctx); err != nil {
			return waitFailure(ctx, err, a.resumeCommand("clusters", cluster, timeout))
		}
	}
}
func (a *app) watch(ctx context.Context, q map[string]any) error {
	resp, err := dispatch(ctx, a.client, a.root, "streamEvents", nil, a.namespaceQuery("streamEvents", q), nil)
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
