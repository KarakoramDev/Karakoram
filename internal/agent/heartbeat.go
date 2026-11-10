// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func (a *nodeAgent) sessionID() string {
	a.heartbeatSessionOnce.Do(func() { a.heartbeatSession = rand.Text() })
	return a.heartbeatSession
}

func heartbeatIdentity(o Options) agentapi.InstanceIdentity {
	identity := agentapi.InstanceIdentity{TaskID: o.TaskID}
	if identity.TaskID == "" {
		identity.TaskID = os.Getenv("NOMAD_ALLOC_ID")
	}
	identity.Hostname, _ = os.Hostname()
	addresses, _ := net.InterfaceAddrs()
	for _, address := range addresses {
		ip, _, err := net.ParseCIDR(address.String())
		if err == nil && ip.IsGlobalUnicast() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() {
			identity.Addresses = append(identity.Addresses, ip.String())
		}
	}
	sort.Strings(identity.Addresses)
	return identity
}

func heartbeatInterval(millis int64) time.Duration {
	if millis <= 0 {
		return agentapi.HeartbeatInterval
	}
	// Compare before conversion so an untrusted interval cannot overflow.
	if millis < agentapi.HeartbeatMinInterval.Milliseconds() {
		return agentapi.HeartbeatMinInterval
	}
	if millis > agentapi.HeartbeatMaxInterval.Milliseconds() {
		return agentapi.HeartbeatMaxInterval
	}
	return time.Duration(millis) * time.Millisecond
}

func heartbeatWait(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// A single batch may remain in flight if a native handler ignores cancellation.
// Skip subsequent sampling in that case, rather than accumulating goroutines.
func (a *nodeAgent) heartbeatLoop(ctx context.Context, handler http.Handler, wait func(context.Context, time.Duration) bool) {
	request := agentapi.HeartbeatRequest{Session: a.sessionID(), Instance: heartbeatIdentity(a.o)}
	interval := agentapi.HeartbeatInterval
	legacyLogged := false
	var pending <-chan []agentapi.HeartbeatSample
	var reloading <-chan struct{}
	defer func() {
		if reloading != nil {
			<-reloading
		}
	}()
	for ctx.Err() == nil {
		a.mu.RLock()
		request.ConfigRevision = a.changeRevision
		a.mu.RUnlock()
		if reloading != nil {
			select {
			case <-reloading:
				reloading = nil
			default:
			}
		}
		response, err := a.client.Heartbeat(ctx, request)
		request.Samples = nil
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			var remote *agentapi.HTTPError
			if errors.As(err, &remote) && remote.StatusCode == http.StatusNotFound {
				interval = agentapi.HeartbeatLegacyBackoff
				if !legacyLogged && a.o.Logger != nil {
					a.o.Logger.Info("control plane does not support heartbeats; backing off")
				}
				legacyLogged = true
			}
			if !wait(ctx, interval) {
				return
			}
			continue
		}
		if response.ReloadConfig && reloading == nil {
			done := make(chan struct{})
			reloading = done
			go func() {
				defer close(done)
				reloadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()
				if _, err := a.reload(reloadCtx); err != nil && ctx.Err() == nil && a.o.Logger != nil {
					a.o.Logger.Warn("notified configuration pull failed", "error", err)
				}
			}()
		}
		interval = heartbeatInterval(response.IntervalMillis)
		request.Token = response.Token
		if !wait(ctx, interval) {
			return
		}
		if pending != nil {
			select {
			case <-pending:
				pending = nil // Old samples belong to an older token; discard them.
			default:
				continue
			}
		}
		if request.Token == "" {
			continue
		}
		sampleCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
		pending = a.heartbeatSamples(sampleCtx, handler, response.Requests)
		select {
		case request.Samples = <-pending:
			pending = nil
		case <-sampleCtx.Done():
		}
		cancel()
		// Bound the complete wire envelope as well as each individual result.
		for len(request.Samples) > 0 {
			encoded, marshalErr := json.Marshal(request)
			if marshalErr == nil && len(encoded) <= agentapi.HeartbeatMaxBytes {
				break
			}
			request.Samples = request.Samples[:len(request.Samples)-1]
		}
	}
}

func (a *nodeAgent) heartbeatSamples(ctx context.Context, handler http.Handler, actions []agentapi.HeartbeatAction) <-chan []agentapi.HeartbeatSample {
	selected := []agentapi.HeartbeatAction{{Action: agentapi.GenericStatusAction}}
	for _, action := range actions {
		if len(selected) == agentapi.HeartbeatMaxSamples {
			break
		}
		if !agentapi.PushableAction(action.Engine, action.Action) || len(action.Request) > agentapi.HeartbeatSampleMaxBytes || (action.Engine == "" && action.Action == agentapi.GenericStatusAction) {
			continue
		}
		selected = append(selected, action)
	}
	result := make(chan []agentapi.HeartbeatSample, 1)
	go func() {
		var wg sync.WaitGroup
		samples := make([]agentapi.HeartbeatSample, len(selected))
		for index, action := range selected {
			wg.Add(1)
			go func() {
				defer wg.Done()
				method, path := "POST", "/v1/"+action.Engine+"/"+action.Action
				if action.Engine == "" {
					method, path = "GET", "/v1/status"
				}
				req, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(action.Request))
				if err != nil {
					return
				}
				req.Header.Set("Authorization", "Bearer "+a.o.Token)
				if len(action.Request) != 0 {
					req.Header.Set("Content-Type", "application/json")
				}
				writer := &heartbeatWriter{header: make(http.Header)}
				handler.ServeHTTP(writer, req)
				writer.WriteHeader(http.StatusOK)
				if writer.overflow || ctx.Err() != nil {
					return
				}
				body := writer.body.Bytes()
				if len(body) != 0 && !json.Valid(body) {
					body, _ = json.Marshal(string(body))
				}
				if len(body) > agentapi.HeartbeatSampleMaxBytes {
					return
				}
				samples[index] = agentapi.HeartbeatSample{Engine: action.Engine, Action: action.Action, Request: action.Request, Code: writer.code, Response: body}
			}()
		}
		wg.Wait()
		kept := samples[:0]
		for _, sample := range samples {
			if sample.Code != 0 {
				kept = append(kept, sample)
			}
		}
		result <- kept
	}()
	return result
}

// Unlike ResponseRecorder, this writer never retains an oversized native reply.
type heartbeatWriter struct {
	header   http.Header
	body     bytes.Buffer
	code     int
	overflow bool
}

func (w *heartbeatWriter) Header() http.Header { return w.header }
func (w *heartbeatWriter) WriteHeader(code int) {
	if w.code == 0 {
		w.code = code
	}
}
func (w *heartbeatWriter) Write(body []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	if w.overflow || len(body) > agentapi.HeartbeatSampleMaxBytes-w.body.Len() {
		w.overflow = true
		return len(body), nil
	}
	return w.body.Write(body)
}
