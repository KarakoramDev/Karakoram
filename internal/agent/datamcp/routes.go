package datamcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
)

// A dedicated transport avoids environment proxies and whole-response deadlines:
// MCP responses may remain open for SSE notifications.
var upstreamTransport = &http.Transport{Proxy: nil, ResponseHeaderTimeout: 0, IdleConnTimeout: 90 * time.Second}

func proxy(e endpoint, prefix string) http.Handler {
	target := &url.URL{Scheme: "http", Host: fmt.Sprintf("127.0.0.1:%d", e.Port)}
	return &httputil.ReverseProxy{
		Transport: upstreamTransport, FlushInterval: -1,
		Rewrite: func(p *httputil.ProxyRequest) {
			suffix := strings.TrimPrefix(p.In.URL.Path, prefix)
			p.SetURL(target)
			p.Out.URL.Path = e.Path + suffix
			p.Out.URL.RawPath = ""
			p.Out.Host = target.Host
			p.Out.Header.Del("Cookie")
			p.Out.Header.Set("Authorization", "Bearer "+e.Token)
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
		},
	}
}

func (r *Runner) Routes(mux *http.ServeMux, n *agent.Node) {
	for _, route := range []struct{ segment, name string }{{"ro", "mcp-ro"}, {"rw", "mcp-rw"}} {
		prefix := "/v1/datamcp/" + route.segment
		handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			r.mu.RLock()
			e, ok := r.state.Processes[route.name]
			r.mu.RUnlock()
			if !ok {
				http.NotFound(w, req)
				return
			}
			e.handler.ServeHTTP(w, req)
		})
		mux.Handle(prefix, handler)
		mux.Handle(prefix+"/", handler)
	}
	mux.HandleFunc("/v1/datamcp/readiness", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != "POST" && req.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		r.mu.RLock()
		s := r.state
		r.mu.RUnlock()
		type processHealth struct {
			Healthy bool `json:"healthy"`
		}
		response := struct {
			ConfigVersion int64                    `json:"configVersion"`
			Version       string                   `json:"version"`
			Processes     map[string]processHealth `json:"processes"`
		}{s.ConfigVersion, s.Version, map[string]processHealth{}}
		ok := len(s.Processes) > 0
		for name, e := range s.Processes {
			h := healthy(req.Context(), e)
			response.Processes[name] = processHealth{h}
			ok = ok && h
		}
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(response)
	})
}

func healthy(ctx context.Context, e endpoint) bool {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	// An authenticated MCP initialize probes the actual application, not merely
	// an open listener or an unauthenticated liveness endpoint.
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("http://127.0.0.1:%d%s", e.Port, e.Path), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"karakoram-readiness","version":"1"}}}`))
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+e.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := upstreamTransport.RoundTrip(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	if session := resp.Header.Get("Mcp-Session-Id"); session != "" {
		defer func() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer cancel()
			request, _ := http.NewRequestWithContext(cleanup, "DELETE", req.URL.String(), nil)
			request.Header.Set("Authorization", "Bearer "+e.Token)
			request.Header.Set("Mcp-Session-Id", session)
			if response, err := upstreamTransport.RoundTrip(request); err == nil {
				response.Body.Close()
			}
		}()
	}
	reader := io.LimitReader(resp.Body, 65536)
	var result struct {
		Result json.RawMessage `json:"result"`
	}
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		scanner := bufio.NewScanner(reader)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data:") && json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &result) == nil && len(result.Result) > 0 {
				return true
			}
		}
		return false
	}
	return json.NewDecoder(reader).Decode(&result) == nil && len(result.Result) > 0
}
