// Package nativees implements the versioned, scope-restricted Elasticsearch data tools.
//
// Searches default to 10 documents, cap at 100, use a five-second server timeout
// and bounded total-hit tracking. ES|QL accepts a conservative FROM-and-pipeline
// subset and appends LIMIT 500. Responses and requests are capped at four MiB.
// Hidden backing indices are excluded even when reached through a data stream.
// Alias filters and routing remain in effect after backing-index validation.
// Query DSL validation distinguishes field names from executable query, sort,
// and aggregation positions; scripts and cross-index lookups are not supported.
package nativees

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/datamcp"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const maxResponse = 4 << 20

var errRequest = errors.New("elasticsearch request failed")
var errScope = errors.New("index is outside the permitted non-hidden scope")

// Manifest identifies the reviewed Karakoram tool set, independently of the server version.
func Manifest() datamcp.Manifest {
	m := datamcp.Manifest{Upstream: "native-elasticsearch", Version: "1", Tools: map[string]datamcp.ToolRule{}}
	for _, n := range []string{"indices_list", "mapping_get", "search", "esql_query", "count", "shards_get", "index_stats"} {
		m.Tools[n] = datamcp.ToolRule{Class: datamcp.ClassRead}
	}
	for _, n := range []string{"doc_index", "doc_update"} {
		m.Tools[n] = datamcp.ToolRule{Class: datamcp.ClassWrite}
	}
	m.Tools["delete_by_query"] = datamcp.ToolRule{Class: datamcp.ClassDestructive}
	return m
}

type upstream struct {
	client             *http.Client
	transport          *http.Transport
	endpoint           *url.URL
	username, password string
	patterns           []string
	write              bool
}

// Open constructs a TLS-only client. All network requests, including scope resolution,
// use the namespace dialer and the pinned cluster CA.
func Open(ctx context.Context, in datamcp.NativeInput) (datamcp.Upstream, error) {
	if err := ctx.Err(); err != nil {
		return nil, errors.New("elasticsearch open cancelled")
	}
	if !in.Level.Valid() {
		return nil, errors.New("invalid Elasticsearch access level")
	}
	c := in.Credentials
	endpoint := c.URI
	if len(c.Hosts) > 0 {
		endpoint = c.Hosts[0]
		if !strings.Contains(endpoint, "://") {
			endpoint = "https://" + endpoint
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("invalid Elasticsearch HTTPS endpoint")
	}
	if u.User != nil {
		if c.Username == "" {
			c.Username = u.User.Username()
			c.Password, _ = u.User.Password()
		}
		u.User = nil
	}
	roots := x509.NewCertPool()
	if len(c.CAPEM) == 0 || !roots.AppendCertsFromPEM(c.CAPEM) {
		return nil, errors.New("invalid Elasticsearch CA")
	}
	patterns := append([]string(nil), in.Scope.IndexPatterns...)
	for _, p := range patterns {
		if !validPattern(p) {
			return nil, errScope
		}
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, MaxIdleConnsPerHost: 2, ResponseHeaderTimeout: 15 * time.Second}
	if in.Dialer != nil {
		tr.DialContext = in.Dialer.DialContext
	}
	return &upstream{client: &http.Client{Transport: tr, Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errRequest }}, transport: tr, endpoint: u, username: c.Username, password: c.Password, patterns: patterns, write: in.Level == engine.DataAccessReadWrite}, nil
}

func (u *upstream) Close() error { u.transport.CloseIdleConnections(); return nil }

func (u *upstream) request(ctx context.Context, method, endpoint string, query url.Values, body any) ([]byte, error) {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil || len(data) > maxResponse {
			return nil, errors.New("invalid Elasticsearch request body")
		}
	}
	target := *u.endpoint
	target.Path = endpoint
	target.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, target.String(), bytes.NewReader(data))
	if err != nil {
		return nil, errRequest
	}
	req.SetBasicAuth(u.username, u.password)
	req.Header.Set("Content-Type", "application/json")
	resp, err := u.client.Do(req)
	if err != nil {
		return nil, errRequest
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse+1))
	if err != nil || len(b) > maxResponse {
		return nil, errors.New("elasticsearch response exceeds limits or could not be read")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("elasticsearch request rejected (status %d)", resp.StatusCode)
	}
	if !json.Valid(b) {
		return nil, errors.New("invalid Elasticsearch JSON response")
	}
	return b, nil
}

func validPattern(s string) bool {
	return engine.ValidElasticsearchDataIndexPattern(s)
}
func (u *upstream) allowed(s string) bool {
	if !validPattern(s) || strings.ContainsAny(s, "*?") {
		return false
	}
	if len(u.patterns) == 0 {
		return true
	}
	for _, p := range u.patterns {
		if ok, _ := path.Match(p, s); ok {
			return true
		}
	}
	return false
}

// resolve never forwards a caller's wildcard to a data operation. Every returned
// concrete index, alias and data-stream backing index must pass scope checks.
func (u *upstream) resolve(ctx context.Context, patterns string) ([]string, error) {
	if patterns == "" {
		patterns = strings.Join(u.patterns, ",")
		if patterns == "" {
			patterns = "*"
		}
	}
	for _, p := range strings.Split(patterns, ",") {
		if !validPattern(p) {
			return nil, errScope
		}
	}
	b, err := u.request(ctx, http.MethodGet, "/_resolve/index/"+patterns, url.Values{"expand_wildcards": {"open"}}, nil)
	if err != nil {
		return nil, err
	}
	var r struct {
		Indices []struct {
			Name       string   `json:"name"`
			Attributes []string `json:"attributes"`
		} `json:"indices"`
		Aliases []struct {
			Name    string   `json:"name"`
			Indices []string `json:"indices"`
		} `json:"aliases"`
		Streams []struct {
			Name    string   `json:"name"`
			Backing []string `json:"backing_indices"`
		} `json:"data_streams"`
	}
	if json.Unmarshal(b, &r) != nil {
		return nil, errRequest
	}
	names := map[string]bool{}
	targets := map[string]bool{}
	for _, i := range r.Indices {
		if !u.allowed(i.Name) {
			return nil, errScope
		}
		for _, a := range i.Attributes {
			if a == "hidden" || a == "system" {
				return nil, errScope
			}
		}
		names[i.Name] = true
		targets[i.Name] = true
	}
	for _, a := range r.Aliases {
		if !u.allowed(a.Name) || len(a.Indices) == 0 {
			return nil, errScope
		}
		targets[a.Name] = true
		for _, n := range a.Indices {
			if !u.allowed(n) {
				return nil, errScope
			}
			names[n] = true
		}
	}
	for _, s := range r.Streams {
		if !u.allowed(s.Name) || len(s.Backing) == 0 {
			return nil, errScope
		}
		targets[s.Name] = true
		for _, n := range s.Backing {
			if !u.allowed(n) {
				return nil, errScope
			}
			names[n] = true
		}
	}
	out := make([]string, 0, len(names))
	for n := range names {
		out = append(out, n)
	}
	sort.Strings(out)
	// Alias and stream resolutions do not necessarily include index attributes.
	// Resolve their concrete targets once more rather than trusting names alone.
	if len(r.Aliases) > 0 || len(r.Streams) > 0 {
		concrete, err := u.request(ctx, http.MethodGet, "/_resolve/index/"+strings.Join(out, ","), url.Values{"expand_wildcards": {"open"}}, nil)
		if err != nil {
			return nil, err
		}
		var checked struct {
			Indices []struct {
				Name       string   `json:"name"`
				Attributes []string `json:"attributes"`
			} `json:"indices"`
		}
		if json.Unmarshal(concrete, &checked) != nil {
			return nil, errRequest
		}
		seen := map[string]bool{}
		for _, i := range checked.Indices {
			if !names[i.Name] || !u.allowed(i.Name) {
				return nil, errScope
			}
			for _, a := range i.Attributes {
				if a == "hidden" || a == "system" {
					return nil, errScope
				}
			}
			seen[i.Name] = true
		}
		if len(seen) != len(names) {
			return nil, errScope
		}
	}
	// Preserve alias filters and routing; concrete backing names are used only
	// for validation, never as replacements for the requested alias.
	out = out[:0]
	for name := range targets {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func schema(kind string) map[string]any { return map[string]any{"type": kind} }
func tool(name string) *mcp.Tool {
	props := map[string]any{"index": schema("string")}
	required := []string{}
	switch name {
	case "search":
		props["body"] = schema("object")
		props["size"] = map[string]any{"type": "integer", "default": 10, "maximum": 100, "minimum": 0}
		props["source"] = map[string]any{"type": "array", "items": schema("string")}
	case "count", "delete_by_query":
		props["query"] = schema("object")
	case "esql_query":
		props = map[string]any{"query": schema("string")}
		required = []string{"query"}
	case "doc_index", "doc_update":
		props["id"] = schema("string")
		props["document"] = schema("object")
		required = []string{"index", "id", "document"}
	}
	return &mcp.Tool{Name: name, Description: "Scoped Elasticsearch " + strings.ReplaceAll(name, "_", " "), InputSchema: map[string]any{"type": "object", "properties": props, "additionalProperties": false, "required": required}}
}
func (u *upstream) ListTools(context.Context) ([]*mcp.Tool, error) {
	names := []string{}
	for n, rule := range Manifest().Tools {
		if rule.Class == datamcp.ClassRead || u.write {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	out := make([]*mcp.Tool, 0, len(names))
	for _, n := range names {
		out = append(out, tool(n))
	}
	return out, nil
}

func textResult(b []byte) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(b)}}}
}

func (u *upstream) CallTool(ctx context.Context, name string, args map[string]any) (*mcp.CallToolResult, error) {
	rule, ok := Manifest().Tools[name]
	if !ok || (rule.Class != datamcp.ClassRead && !u.write) {
		return nil, errors.New("elasticsearch tool is not available")
	}
	// Validate top-level arguments even when called outside the gateway.
	spec := tool(name).InputSchema.(map[string]any)
	props := spec["properties"].(map[string]any)
	for k, v := range args {
		p, ok := props[k].(map[string]any)
		if !ok {
			return nil, errors.New("unknown Elasticsearch argument")
		}
		switch p["type"] {
		case "string":
			if _, ok := v.(string); !ok {
				return nil, errors.New("invalid string argument")
			}
		case "object":
			if _, ok := v.(map[string]any); !ok {
				return nil, errors.New("invalid object argument")
			}
		}
	}
	for _, k := range spec["required"].([]string) {
		if _, ok := args[k]; !ok {
			return nil, errors.New("missing Elasticsearch argument")
		}
	}
	if name == "esql_query" {
		return u.esql(ctx, args["query"].(string))
	}
	index, _ := args["index"].(string)
	names, err := u.resolve(ctx, index)
	if err != nil {
		return nil, err
	}
	if name == "indices_list" {
		b, _ := json.Marshal(map[string]any{"indices": names})
		return textResult(b), nil
	}
	if len(names) == 0 {
		return nil, errors.New("no permitted indices matched")
	}
	endpoint := "/" + strings.Join(names, ",")
	method := http.MethodGet
	params := url.Values{"expand_wildcards": {"open"}, "allow_no_indices": {"false"}, "ignore_unavailable": {"false"}}
	if name == "doc_index" || name == "doc_update" {
		params = nil
	}
	var body any
	switch name {
	case "mapping_get":
		endpoint += "/_mapping"
	case "shards_get":
		endpoint += "/_search_shards"
	case "index_stats":
		endpoint += "/_stats"
	case "search":
		endpoint += "/_search"
		method = http.MethodPost
		b := map[string]any{}
		if input, ok := args["body"].(map[string]any); ok {
			for k, v := range input {
				switch k {
				case "query", "sort", "_source", "aggs", "aggregations", "from", "size", "track_total_hits", "timeout":
					b[k] = v
				default:
					return nil, errors.New("unsupported search body field")
				}
			}
		}
		if unsafeSearch(b) {
			return nil, errors.New("cross-index query features are not permitted")
		}
		size := any(10)
		if v, ok := b["size"]; ok {
			size = v
		}
		if v, ok := args["size"]; ok {
			size = v
		}
		n, err := boundedSize(size)
		if err != nil {
			return nil, err
		}
		b["size"] = n
		b["timeout"] = "5s"
		b["track_total_hits"] = 10000
		if source, ok := args["source"]; ok {
			b["_source"] = source
		}
		if !validSource(b["_source"]) {
			return nil, errors.New("invalid source filter")
		}
		body = b
	case "count", "delete_by_query":
		endpoint += "/_" + name
		method = http.MethodPost
		q, _ := args["query"].(map[string]any)
		if q == nil {
			q = map[string]any{"match_all": map[string]any{}}
		}
		if unsafeQuery(q) {
			return nil, errors.New("cross-index query features are not permitted")
		}
		body = map[string]any{"query": q}
		if name == "delete_by_query" {
			params.Set("timeout", "5s")
			params.Set("scroll_size", "100")
			params.Set("max_docs", "500")
			params.Set("wait_for_completion", "true")
		}
	case "doc_index", "doc_update":
		if len(names) != 1 || index != names[0] {
			return nil, errors.New("document writes require one concrete index")
		}
		id := args["id"].(string)
		if id == "" || strings.ContainsAny(id, "/\\?#%") || id == "." || id == ".." {
			return nil, errors.New("invalid document ID")
		}
		endpoint += "/_doc/" + id
		method = http.MethodPut
		body = args["document"]
		if name == "doc_update" {
			endpoint = "/" + names[0] + "/_update/" + id
			method = http.MethodPost
			body = map[string]any{"doc": args["document"]}
		}
	}
	b, err := u.request(ctx, method, endpoint, params, body)
	if err != nil {
		return nil, err
	}
	return textResult(b), nil
}

func boundedSize(v any) (int, error) {
	var f float64
	switch n := v.(type) {
	case int:
		f = float64(n)
	case float64:
		f = n
	case json.Number:
		var err error
		f, err = n.Float64()
		if err != nil {
			return 0, errors.New("invalid search size")
		}
	default:
		return 0, errors.New("invalid search size")
	}
	if f < 0 || f != f || f != float64(int64(f)) {
		return 0, errors.New("invalid search size")
	}
	if f > 100 {
		return 100, nil
	}
	return int(f), nil
}
func validSource(v any) bool {
	if v == nil {
		return true
	}
	switch s := v.(type) {
	case bool, string:
		return true
	case []string:
		return true
	case []any:
		for _, v := range s {
			if _, ok := v.(string); !ok {
				return false
			}
		}
		return true
	case map[string]any:
		for k, v := range s {
			if k != "includes" && k != "excludes" {
				return false
			}
			if !validSource(v) {
				return false
			}
		}
		return true
	}
	return false
}

// Validators follow DSL positions, not arbitrary JSON keys: application field
// names and aggregation names may legitimately be "index", "script" or "indices".
func unsafeSearch(body map[string]any) bool {
	for key, value := range body {
		switch key {
		case "query", "post_filter":
			if unsafeQuery(value) {
				return true
			}
		case "aggs", "aggregations":
			if unsafeAggregations(value) {
				return true
			}
		case "sort":
			if unsafeSort(value) {
				return true
			}
		case "script_fields", "runtime_mappings", "pit", "scroll":
			return true
		}
	}
	return false
}

func object(v any) map[string]any           { m, _ := v.(map[string]any); return m }
func has(m map[string]any, key string) bool { _, ok := m[key]; return ok }

func unsafeQuery(v any) bool {
	if clauses, ok := v.([]any); ok {
		for _, clause := range clauses {
			if unsafeQuery(clause) {
				return true
			}
		}
		return false
	}
	q := object(v)
	if len(q) == 0 {
		return v != nil
	}
	for kind, value := range q {
		params := object(value)
		switch kind {
		case "match_all", "match_none", "term", "match", "match_phrase", "match_phrase_prefix",
			"multi_match", "combined_fields", "range", "exists", "prefix", "wildcard", "regexp",
			"fuzzy", "ids", "query_string", "simple_query_string", "distance_feature",
			"rank_feature", "rank_features", "geo_distance", "geo_bounding_box", "geo_polygon":
			// These parameters contain values and field names, not nested queries.
		case "terms":
			for field, values := range params {
				if field != "boost" && object(values) != nil {
					return true
				}
			}
		case "terms_set":
			for _, field := range params {
				if has(object(field), "minimum_should_match_script") {
					return true
				}
			}
		case "bool":
			for _, key := range []string{"must", "filter", "should", "must_not"} {
				if unsafeQuery(params[key]) {
					return true
				}
			}
		case "constant_score", "nested", "has_child", "has_parent":
			key := "query"
			if kind == "constant_score" {
				key = "filter"
			}
			if unsafeQuery(params[key]) {
				return true
			}
			if inner := object(params["inner_hits"]); unsafeSearch(inner) {
				return true
			}
		case "dis_max":
			if unsafeQuery(params["queries"]) {
				return true
			}
		case "boosting":
			if unsafeQuery(params["positive"]) || unsafeQuery(params["negative"]) {
				return true
			}
		case "function_score":
			if has(params, "script_score") || unsafeQuery(params["query"]) {
				return true
			}
			if functions, ok := params["functions"].([]any); ok {
				for _, function := range functions {
					f := object(function)
					if has(f, "script_score") || unsafeQuery(f["filter"]) {
						return true
					}
				}
			}
		case "more_like_this":
			for _, key := range []string{"like", "unlike"} {
				items, ok := params[key].([]any)
				if !ok {
					items = []any{params[key]}
				}
				for _, item := range items {
					doc := object(item)
					if has(doc, "_index") || has(doc, "_id") {
						return true
					}
				}
			}
		case "percolate":
			if has(params, "index") || has(params, "id") {
				return true
			}
		case "geo_shape", "shape":
			for _, field := range params {
				if has(object(field), "indexed_shape") {
					return true
				}
			}
		case "pinned":
			if has(params, "docs") || unsafeQuery(params["organic"]) {
				return true
			}
		default:
			// Includes scripts, encoded wrapper queries and unknown extensions.
			return true
		}
	}
	return false
}

func unsafeSort(value any) bool {
	if sorts, ok := value.([]any); ok {
		for _, sort := range sorts {
			if unsafeSort(sort) {
				return true
			}
		}
		return false
	}
	for field, options := range object(value) {
		if field == "_script" {
			return true
		}
		nested := object(object(options)["nested"])
		for nested != nil {
			if unsafeQuery(nested["filter"]) {
				return true
			}
			nested = object(nested["nested"])
		}
	}
	return false
}

func unsafeAggregations(value any) bool {
	for _, definition := range object(value) {
		for kind, options := range object(definition) {
			p := object(options)
			switch kind {
			case "aggs", "aggregations":
				if unsafeAggregations(options) {
					return true
				}
			case "meta":
				continue
			case "filter":
				if unsafeQuery(options) {
					return true
				}
			case "filters", "adjacency_matrix":
				filters := p["filters"]
				if list, ok := filters.([]any); ok {
					for _, q := range list {
						if unsafeQuery(q) {
							return true
						}
					}
				} else {
					for _, q := range object(filters) {
						if unsafeQuery(q) {
							return true
						}
					}
				}
			case "top_hits":
				if unsafeSearch(p) {
					return true
				}
			case "scripted_metric", "bucket_script", "bucket_selector":
				return true
			default:
				if has(p, "script") || has(p, "init_script") || has(p, "map_script") ||
					has(p, "combine_script") || has(p, "reduce_script") ||
					unsafeQuery(p["background_filter"]) {
					return true
				}
				// multi_terms and composite contain aggregation definitions.
				if terms, ok := p["terms"].([]any); ok {
					for _, term := range terms {
						if has(object(term), "script") {
							return true
						}
					}
				}
				if sources, ok := p["sources"].([]any); ok {
					for _, source := range sources {
						if unsafeAggregations(source) {
							return true
						}
					}
				}
			}
		}
	}
	return false
}

var fromClause = regexp.MustCompile(`(?i)^FROM\s+([a-z0-9_*?.,-]+)\s*(\|.*)?$`)
var pipelineCommand = regexp.MustCompile(`(?i)^\s*(WHERE|KEEP|DROP|SORT|LIMIT|STATS|EVAL|RENAME|DISSECT|GROK|MV_EXPAND)\b`)

func (u *upstream) esql(ctx context.Context, query string) (*mcp.CallToolResult, error) {
	// No quoted sources, comments, metadata, parameters, subqueries, joins or
	// enrichment: those cannot be verified by this intentionally small parser.
	if strings.ContainsAny(query, ";\n\r`\\") || strings.Contains(query, "/*") || strings.Contains(query, "//") {
		return nil, errors.New("unsupported ES|QL syntax")
	}
	m := fromClause.FindStringSubmatch(strings.TrimSpace(query))
	if m == nil {
		return nil, errors.New("ES|QL requires a verifiable FROM clause")
	}
	tail := m[2]
	if tail != "" {
		for _, stage := range strings.Split(tail[1:], "|") {
			if !pipelineCommand.MatchString(stage) {
				return nil, errors.New("unsupported ES|QL pipeline")
			}
		}
	}
	names, err := u.resolve(ctx, m[1])
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, errors.New("no permitted indices matched")
	}
	body := map[string]any{"query": "FROM " + strings.Join(names, ",") + " " + tail + " | LIMIT 500", "columnar": false}
	b, err := u.request(ctx, http.MethodPost, "/_query", nil, body)
	if err != nil {
		return nil, err
	}
	return textResult(b), nil
}
