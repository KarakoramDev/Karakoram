// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agentapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Client performs bearer-authenticated requests over pinned-CA HTTPS.
type Client struct {
	base         *url.URL
	token        string
	http         *http.Client
	engine       *http.Client
	dialInstance func(context.Context, string, string, string) (net.Conn, error)
}

// HTTPError exposes the response status for protocol capability negotiation.
// Message contains only the client's bounded, redacted remote diagnostic.
type HTTPError struct {
	StatusCode              int
	Method, Action, Message string
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("agent request %s %s: HTTP %d: %s", e.Method, e.Action, e.StatusCode, e.Message)
	}
	return fmt.Sprintf("agent request %s %s: HTTP %d", e.Method, e.Action, e.StatusCode)
}

// ClientOptions supplies the HTTPS endpoint, token, trust roots, and request deadlines.
// EngineTimeout bounds engine-owned actions such as broker drains, which may
// legitimately outlast ordinary requests; it defaults to three minutes.
type ClientOptions struct {
	BaseURL       string
	Token         string
	CAPEM         []byte
	Timeout       time.Duration
	EngineTimeout time.Duration
	DialContext   func(context.Context, string, string) (net.Conn, error)
	// DialInstanceContext authorizes and pins a runtime within the logical
	// member's namespace. Arguments are network, logical host:port and instance host.
	DialInstanceContext func(context.Context, string, string, string) (net.Conn, error)
}

// NewClient trusts only the supplied private CA and verifies the hostname.
func NewClient(opts ClientOptions) (*Client, error) {
	base, err := url.Parse(opts.BaseURL)
	if err != nil {
		return nil, err
	}
	if base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("agent URL must be HTTPS without credentials, query or fragment")
	}
	if opts.Token == "" {
		return nil, errors.New("agent bearer token is required")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(opts.CAPEM) {
		return nil, errors.New("invalid pinned agent CA")
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: timeout, IdleConnTimeout: 90 * time.Second}
	if opts.DialContext != nil {
		transport.DialContext = opts.DialContext
	}
	redirect := func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client := &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: redirect}
	engineTimeout := opts.EngineTimeout
	if engineTimeout <= 0 {
		engineTimeout = 3 * time.Minute
	}
	engineTransport := transport.Clone()
	engineTransport.ResponseHeaderTimeout = engineTimeout
	engineClient := &http.Client{Transport: engineTransport, Timeout: engineTimeout, CheckRedirect: redirect}
	return &Client{base: base, token: opts.Token, http: client, engine: engineClient, dialInstance: opts.DialInstanceContext}, nil
}

// ForInstance pins connections to one runtime address while retaining the logical
// member's HTTPS hostname, CA, bearer token and namespace transport. The caller
// owns the returned client and must Close it; the original client is unchanged.
func (c *Client) ForInstance(host string) (*Client, error) {
	if net.ParseIP(host) == nil && !instanceHostname.MatchString(host) {
		return nil, errors.New("invalid agent instance address")
	}
	pin := func(original *http.Client) *http.Client {
		client := *original
		transport := original.Transport.(*http.Transport).Clone()
		dial := transport.DialContext
		if dial == nil {
			dial = (&net.Dialer{}).DialContext
		}
		transport.DialContext = func(ctx context.Context, network, logical string) (net.Conn, error) {
			if c.dialInstance != nil {
				return c.dialInstance(ctx, network, logical, host)
			}
			_, port, err := net.SplitHostPort(logical)
			if err != nil {
				return nil, err
			}
			return dial(ctx, network, net.JoinHostPort(host, port))
		}
		client.Transport = transport
		return &client
	}
	return &Client{base: c.base, token: c.token, http: pin(c.http), engine: pin(c.engine)}, nil
}

// DialContext uses this client's namespace transport and instance pin for a
// native application probe. The caller retains the logical host for TLS.
func (c *Client) DialContext(ctx context.Context, network, logical string) (net.Conn, error) {
	dial := c.http.Transport.(*http.Transport).DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	return dial(ctx, network, logical)
}

var instanceHostname = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,252}$`)
var diagnosticCredential = regexp.MustCompile(`(?i)((?:password|token|secret|key)=|://)[^\s]+`)

// Close releases idle HTTPS connections.
func (c *Client) Close() { c.http.CloseIdleConnections(); c.engine.CloseIdleConnections() }
func (c *Client) request(ctx context.Context, method, path string, input, output any) error {
	return c.requestWith(ctx, c.http, method, path, input, output)
}

func (c *Client) requestWith(ctx context.Context, hc *http.Client, method, path string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	u := *c.base
	parts := strings.SplitN(path, "?", 2)
	u.Path = strings.TrimRight(u.Path, "/") + parts[0]
	if len(parts) == 2 {
		u.RawQuery = parts[1]
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if action, ok := input.(ProcessAction); ok && action.RequestID != "" {
		req.Header.Set("Idempotency-Key", action.RequestID)
	}
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var diagnostic ErrorResponse
		data, readErr := io.ReadAll(io.LimitReader(res.Body, 8<<10))
		if readErr == nil && json.Unmarshal(data, &diagnostic) == nil && diagnostic.Error != "" {
			message := strings.ReplaceAll(diagnostic.Error, c.token, "[redacted]")
			if strings.Contains(message, "PRIVATE KEY") {
				message = "remote action failed (sensitive diagnostic omitted)"
			}
			message = diagnosticCredential.ReplaceAllString(message, "$1[redacted]")
			message = strings.Map(func(r rune) rune {
				if r < ' ' || r == 127 {
					return ' '
				}
				return r
			}, message)
			if len(message) > 1024 {
				message = message[:1024]
			}
			return &HTTPError{StatusCode: res.StatusCode, Method: method, Action: parts[0], Message: message}
		}
		return &HTTPError{StatusCode: res.StatusCode, Method: method, Action: parts[0]}
	}
	if output == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(res.Body, 1<<20))
		return err
	}
	limit := int64(16 << 20)
	if parts[0] == "/internal/agent/v1/config" {
		// Supabase permits 100 functions with 1 MiB of source each. JSON escaping
		// and source-file names can expand the admitted 7 MiB function envelope.
		// Keep this larger allowance confined to the pinned control-plane pull.
		limit = 768 << 20
	}
	return json.NewDecoder(io.LimitReader(res.Body, limit)).Decode(output)
}

// Config pulls the member configuration from the control plane.
func (c *Client) Config(ctx context.Context) (MemberConfig, error) {
	var v MemberConfig
	err := c.request(ctx, "GET", "/internal/agent/v1/config?capability="+ConfigCapabilityGaleraListenHost+"&capability="+ConfigCapabilityMySQLListenHost, nil, &v)
	return v, err
}

// Status reads observed node-agent state.
func (c *Client) Status(ctx context.Context) (Status, error) {
	var v Status
	err := c.request(ctx, "GET", "/v1/status", nil, &v)
	return v, err
}

// Reload requests an immediate configuration pull.
func (c *Client) Reload(ctx context.Context) error {
	return c.request(ctx, "POST", "/v1/reload", struct{}{}, nil)
}

// Process executes a whitelisted supervisor process action.
func (c *Client) Process(ctx context.Context, a ProcessAction) error {
	if !processName.MatchString(a.Process) {
		return errors.New("invalid process")
	}
	if a.Action != "start" && a.Action != "stop" && a.Action != "restart" {
		return errors.New("invalid process action")
	}
	if a.RequestID != "" {
		return c.request(ctx, "POST", "/v1/processes/"+a.Process+"/"+a.Action, a, nil)
	}
	return c.request(ctx, "POST", "/v1/processes/"+a.Process+"/"+a.Action, nil, nil)
}

// FencePBMRestore actively replaces PBM only on an agent advertising the
// stop/wait/start protocol. Legacy success-only responses are never proof.
func (c *Client) FencePBMRestore(ctx context.Context, requestID string, commandTS int64) (PBMRestartFence, error) {
	status, err := c.Status(ctx)
	if err != nil {
		return PBMRestartFence{}, err
	}
	if !status.PBMRestoreFence {
		return PBMRestartFence{}, errors.New("member lacks supervised PBM restore fencing capability")
	}
	request := ProcessAction{Process: "pbm-agent", Action: "restart", RequestID: requestID, RestoreCommandTS: commandTS}
	var out ActionResponse
	if err := c.request(ctx, "POST", "/v1/processes", request, &out); err != nil {
		return PBMRestartFence{}, err
	}
	minimum := time.Unix(commandTS, 0).Add(PBMRestoreFenceClockMargin)
	if !out.Success || out.PBMFence == nil || !out.PBMFence.OldExited || out.PBMFence.PID <= 0 ||
		out.PBMFence.StartedAfter.Before(minimum) || out.PBMFence.MemberTime.Before(minimum) {
		return PBMRestartFence{}, errors.New("PBM replacement exit/start or member clock confirmation unavailable")
	}
	return *out.PBMFence, nil
}

var processName = regexp.MustCompile(`^[a-z][a-z0-9-]{1,31}$`)

// Engine invokes an engine-owned authenticated JSON action using the longer
// engine deadline. Engine and action names share the process-name syntax.
func (c *Client) Engine(ctx context.Context, engine, action string, req, resp any) error {
	if !processName.MatchString(engine) || !processName.MatchString(action) {
		return errors.New("invalid engine action")
	}
	return c.requestWith(ctx, c.engine, "POST", "/v1/"+engine+"/"+action, req, resp)
}

// Maintenance sets database auto-restart suppression.
func (c *Client) Maintenance(ctx context.Context, v Maintenance) error {
	return c.request(ctx, "POST", "/v1/maintenance", v, nil)
}

// Bootstrap initializes the replica set and first administrator idempotently.
func (c *Client) Bootstrap(ctx context.Context, v BootstrapRequest) (BootstrapResponse, error) {
	var out BootstrapResponse
	err := c.request(ctx, "POST", "/v1/bootstrap", v, &out)
	return out, err
}

// PBM executes a whitelisted backup-agent command.
func (c *Client) PBM(ctx context.Context, v PBMRequest) (PBMResponse, error) {
	var out PBMResponse
	err := c.request(ctx, "POST", "/v1/pbm", v, &out)
	return out, err
}

// Logs reads a bounded process log tail.
func (c *Client) Logs(ctx context.Context, process string, lines int) (LogsResponse, error) {
	var out LogsResponse
	q := url.Values{"process": {process}, "lines": {strconv.Itoa(lines)}}
	err := c.request(ctx, "GET", "/v1/logs?"+q.Encode(), nil, &out)
	return out, err
}

// Metrics reads the agent Prometheus exposition.
func (c *Client) Metrics(ctx context.Context) ([]byte, error) {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/metrics"
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	res, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("agent metrics: HTTP %d", res.StatusCode)
	}
	return io.ReadAll(io.LimitReader(res.Body, 16<<20))
}

// AuthenticateBearer compares fixed-length hashes regardless of token length.
func AuthenticateBearer(r *http.Request, expectedToken string) bool {
	header := r.Header.Get("Authorization")
	provided, ok := strings.CutPrefix(header, "Bearer ")
	a := sha256.Sum256([]byte(provided))
	b := sha256.Sum256([]byte(expectedToken))
	equal := subtle.ConstantTimeCompare(a[:], b[:])
	return ok && expectedToken != "" && equal == 1
}

// AuthenticateBearerHash authenticates a token against its stored SHA-256 hash.
func AuthenticateBearerHash(r *http.Request, expectedHash string) bool {
	provided, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	actual := sha256.Sum256([]byte(provided))
	expected, err := hex.DecodeString(expectedHash)
	if err != nil || len(expected) != sha256.Size {
		return false
	}
	return subtle.ConstantTimeCompare(actual[:], expected) == 1 && ok
}
