// Package nativermq exposes scoped RabbitMQ data tools through the Management API.
package nativermq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/mgmt"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const payloadLimit = 64 << 10

// Manifest pins the reviewed native tool surface.
func Manifest() datamcp.Manifest {
	tools := map[string]datamcp.ToolRule{}
	for _, name := range []string{"overview", "queues_list", "queue_get", "exchanges_list", "bindings_list", "consumers_list"} {
		tools[name] = datamcp.ToolRule{Class: datamcp.ClassRead}
	}
	count, max := float64(5), float64(20)
	tools["messages_peek"] = datamcp.ToolRule{Class: datamcp.ClassWrite, Args: []datamcp.ArgRule{{Name: "count", Default: &count, Max: &max}, {Name: "ackmode", Allow: []string{"ack_requeue_true"}}, {Name: "encoding", Allow: []string{"auto"}}}}
	tools["message_publish"] = datamcp.ToolRule{Class: datamcp.ClassWrite}
	tools["queue_purge"] = datamcp.ToolRule{Class: datamcp.ClassDestructive}
	return datamcp.Manifest{Upstream: "native-rabbitmq", Version: "1", Tools: tools}
}

type upstream struct {
	client *mgmt.Client
	scope  map[string]bool
	write  bool
}

// Open connects with the data identity, a namespace dialer and verified TLS.
func Open(_ context.Context, in datamcp.NativeInput) (datamcp.Upstream, error) {
	if !in.Level.Valid() {
		return nil, errors.New("invalid RabbitMQ access level")
	}
	c := in.Credentials
	endpoint := c.URI
	if endpoint == "" && len(c.Hosts) > 0 {
		endpoint = c.Hosts[0]
	}
	if endpoint == "" {
		return nil, errors.New("RabbitMQ management endpoint is missing")
	}
	if !strings.Contains(endpoint, "://") {
		scheme := "http"
		if c.TLS {
			scheme = "https"
		}
		endpoint = scheme + "://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (c.TLS && u.Scheme != "https") {
		return nil, errors.New("invalid RabbitMQ management endpoint")
	}
	if u.User != nil {
		if c.Username == "" {
			c.Username = u.User.Username()
		}
		if c.Password == "" {
			c.Password, _ = u.User.Password()
		}
		u.User = nil
	}
	if u.Port() == "" {
		port := "15672"
		if u.Scheme == "https" {
			port = "15671"
		}
		u.Host = net.JoinHostPort(u.Hostname(), port)
	}
	tr := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, ResponseHeaderTimeout: 15 * time.Second, MaxIdleConnsPerHost: 2}
	if in.Dialer != nil {
		tr.DialContext = in.Dialer.DialContext
	} else {
		tr.DialContext = (&net.Dialer{Timeout: 15 * time.Second}).DialContext
	}
	if len(c.CAPEM) > 0 {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(c.CAPEM) {
			return nil, errors.New("invalid RabbitMQ CA certificate")
		}
		tr.TLSClientConfig.RootCAs = roots
	}
	client, err := mgmt.New(u.String(), c.Username, c.Password, &http.Client{Transport: tr, Timeout: 20 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }})
	if err != nil {
		return nil, errors.New("invalid RabbitMQ management configuration")
	}
	scope := make(map[string]bool, len(in.Scope.Vhosts))
	for _, v := range in.Scope.Vhosts {
		scope[v] = true
	}
	return &upstream{client: client, scope: scope, write: in.Level == engine.DataAccessReadWrite}, nil
}
func (u *upstream) Close() error { u.client.Close(); return nil }

func schemaProperty(kind string, extra map[string]any) map[string]any {
	p := map[string]any{"type": kind}
	for k, v := range extra {
		p[k] = v
	}
	return p
}
func tools() []*mcp.Tool {
	var result []*mcp.Tool
	add := func(name, description string, props map[string]any, required ...string) {
		props["vhost"] = schemaProperty("string", map[string]any{"minLength": 1})
		required = append([]string{"vhost"}, required...)
		result = append(result, &mcp.Tool{Name: name, Description: description, InputSchema: map[string]any{"type": "object", "properties": props, "required": required, "additionalProperties": false}})
	}
	add("overview", "Get overview of one permitted vhost, including message statistics.", map[string]any{})
	page := func() map[string]any {
		return map[string]any{"page": schemaProperty("integer", map[string]any{"minimum": 1, "default": 1}), "page_size": schemaProperty("integer", map[string]any{"minimum": 1, "maximum": 100, "default": 100})}
	}
	add("queues_list", "List a bounded page of queues with depth, rates and consumer counts.", page())
	add("queue_get", "Get one queue's depth, rates and consumers.", map[string]any{"queue": schemaProperty("string", map[string]any{"minLength": 1})}, "queue")
	for _, name := range []string{"exchanges_list", "bindings_list", "consumers_list"} {
		add(name, "List a bounded page in one permitted vhost.", page())
	}
	add("messages_peek", "Consume and requeue messages; payloads are truncated to 64 KiB.", map[string]any{"queue": schemaProperty("string", map[string]any{"minLength": 1}), "count": schemaProperty("integer", map[string]any{"minimum": 1, "maximum": 20, "default": 5}), "ackmode": schemaProperty("string", map[string]any{"enum": []string{"ack_requeue_true"}, "default": "ack_requeue_true"}), "encoding": schemaProperty("string", map[string]any{"enum": []string{"auto"}, "default": "auto"})}, "queue")
	add("message_publish", "Publish a UTF-8 payload of at most 64 KiB to an exchange in this vhost.", map[string]any{"exchange": schemaProperty("string", nil), "routing_key": schemaProperty("string", nil), "payload": schemaProperty("string", map[string]any{"maxLength": payloadLimit})}, "exchange", "routing_key", "payload")
	add("queue_purge", "Permanently remove all ready messages from a queue.", map[string]any{"queue": schemaProperty("string", map[string]any{"minLength": 1})}, "queue")
	return result
}

var toolDefinitions = tools()
var toolManifest = Manifest()

func (u *upstream) allowed(name string) bool {
	rule, ok := toolManifest.Tools[name]
	return ok && (u.write || rule.Class == datamcp.ClassRead)
}

func (u *upstream) ListTools(context.Context) ([]*mcp.Tool, error) {
	result := make([]*mcp.Tool, 0, len(toolDefinitions))
	for _, t := range toolDefinitions {
		if u.allowed(t.Name) {
			result = append(result, t)
		}
	}
	return result, nil
}

func integer(args map[string]any, key string, def, max int) (int, error) {
	v, ok := args[key]
	if !ok {
		return def, nil
	}
	var n float64
	switch v := v.(type) {
	case int:
		n = float64(v)
	case int64:
		n = float64(v)
	case float64:
		n = v
	case json.Number:
		var err error
		n, err = v.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid %s", key)
		}
	default:
		return 0, fmt.Errorf("invalid %s", key)
	}
	if math.IsNaN(n) || n < 1 || n > float64(max) || math.Trunc(n) != n {
		return 0, fmt.Errorf("%s must be between 1 and %d", key, max)
	}
	return int(n), nil
}

// overview uses the monitoring-accessible collection: the single-vhost resource
// requires administrator even for GET. RabbitMQ 4.3.6 fails on paginated vhost
// collections, so request summary columns and select the exact name locally.
// That broker version ignores columns too; DataRequest still bounds the list.
func (u *upstream) overview(ctx context.Context, vhost string) (map[string]any, error) {
	query := url.Values{"columns": {"name,messages,messages_details,messages_ready,messages_ready_details,messages_unacknowledged,messages_unacknowledged_details,message_stats,recv_oct,recv_oct_details,send_oct,send_oct_details,tracing,default_queue_type"}}
	var items []map[string]any
	if err := u.client.DataRequest(ctx, http.MethodGet, []string{"vhosts"}, query, nil, &items); err != nil {
		return nil, errors.New("RabbitMQ management request failed")
	}
	for _, item := range items {
		if item["name"] == vhost {
			return item, nil
		}
	}
	return nil, errors.New("RabbitMQ vhost is not available")
}

func (u *upstream) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	var tool *mcp.Tool
	if u.allowed(name) {
		for _, t := range toolDefinitions {
			if t.Name == name {
				tool = t
				break
			}
		}
	}
	if tool == nil {
		return nil, errors.New("RabbitMQ tool is not available")
	}
	schema := tool.InputSchema.(map[string]any)
	props := schema["properties"].(map[string]any)
	for key := range args {
		if _, ok := props[key]; !ok {
			return nil, errors.New("unknown RabbitMQ tool argument")
		}
	}
	for _, key := range schema["required"].([]string) {
		if _, ok := args[key].(string); !ok {
			return nil, fmt.Errorf("%s must be a string", key)
		}
	}
	for _, key := range []string{"vhost", "queue", "exchange"} {
		if value, ok := args[key].(string); ok && (value == "." || value == "..") {
			return nil, errors.New("invalid RabbitMQ resource name")
		}
	}
	vhost, _ := args["vhost"].(string)
	if vhost == "" || (len(u.scope) > 0 && !u.scope[vhost]) {
		return nil, errors.New("vhost is outside the permitted scope")
	}
	queue, _ := args["queue"].(string)
	if _, ok := props["queue"]; ok && queue == "" {
		return nil, errors.New("queue is required")
	}
	method := http.MethodGet
	var parts []string
	var body any
	query := url.Values{}
	page, size := 1, 100
	if _, ok := props["page"]; ok {
		var err error
		page, err = integer(args, "page", 1, 1000000)
		if err != nil {
			return nil, err
		}
		size, err = integer(args, "page_size", 100, 100)
		if err != nil {
			return nil, err
		}
	}
	switch name {
	case "queues_list":
		parts = []string{"queues", vhost}
		query.Set("page", strconv.Itoa(page))
		query.Set("page_size", strconv.Itoa(size))
		query.Set("pagination", "true")
	case "queue_get":
		parts = []string{"queues", vhost, queue}
	case "exchanges_list":
		parts = []string{"exchanges", vhost}
	case "bindings_list":
		parts = []string{"bindings", vhost}
	case "consumers_list":
		parts = []string{"consumers", vhost}
	case "messages_peek":
		count, err := integer(args, "count", 5, 20)
		if err != nil {
			return nil, err
		}
		for key, want := range map[string]string{"ackmode": "ack_requeue_true", "encoding": "auto"} {
			if v, ok := args[key]; ok && v != want {
				return nil, fmt.Errorf("invalid %s", key)
			}
		}
		method = http.MethodPost
		parts = []string{"queues", vhost, queue, "get"}
		body = map[string]any{"count": count, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": payloadLimit}
	case "message_publish":
		payload := args["payload"].(string)
		if len(payload) > payloadLimit {
			return nil, errors.New("payload exceeds 64 KiB")
		}
		method = http.MethodPost
		parts = []string{"exchanges", vhost, args["exchange"].(string), "publish"}
		body = map[string]any{"payload": payload, "payload_encoding": "string", "routing_key": args["routing_key"], "properties": map[string]any{}}
	case "queue_purge":
		method = http.MethodDelete
		parts = []string{"queues", vhost, queue, "contents"}
	}
	var result any
	var out any = &result
	if name == "queue_purge" {
		out = nil
		result = map[string]any{"purged": true}
	}
	if name == "overview" {
		var err error
		result, err = u.overview(ctx, vhost)
		if err != nil {
			return nil, err
		}
	} else if err := u.client.DataRequest(ctx, method, parts, query, body, out); err != nil {
		return nil, errors.New("RabbitMQ management request failed")
	}
	if name == "exchanges_list" || name == "bindings_list" || name == "consumers_list" {
		items, ok := result.([]any)
		if !ok {
			return nil, errors.New("invalid RabbitMQ list response")
		}
		start := (page - 1) * size
		if start > len(items) {
			start = len(items)
		}
		end := start + size
		if end > len(items) {
			end = len(items)
		}
		result = map[string]any{"items": items[start:end], "page": page, "page_size": size, "total_count": len(items)}
	}
	if name == "messages_peek" {
		items, ok := result.([]any)
		if !ok {
			return nil, errors.New("invalid RabbitMQ message response")
		}
		count, _ := integer(args, "count", 5, 20)
		if len(items) > count {
			items = items[:count]
		}
		for _, item := range items {
			if m, ok := item.(map[string]any); ok {
				if p, ok := m["payload"].(string); ok && len(p) > payloadLimit {
					p = p[:payloadLimit]
					for !utf8.ValidString(p) {
						p = p[:len(p)-1]
					}
					m["payload"] = p
					m["payload_truncated"] = true
				}
			}
		}
		result = items
	}
	data, err := json.Marshal(result)
	if err != nil {
		return nil, errors.New("invalid RabbitMQ response")
	}
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}}, nil
}
