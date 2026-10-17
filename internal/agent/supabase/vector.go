// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"net/url"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

func renderVector(s sbspec.Spec, dir string) ([]byte, error) {
	type object = map[string]any
	address := "0.0.0.0:9002"
	if s.DatabaseIPv6 {
		address = "[::]:9002"
	}
	source := object{"type": "http_server", "address": address, "decoding": object{"codec": "json"}, "framing": object{"method": "bytes"}}
	if s.VectorSinkToken != "" {
		source["auth"] = object{"strategy": "custom", "source": ".headers.authorization == " + quote("Bearer "+s.VectorSinkToken)}
	}
	transforms := object{"project_logs": object{"type": "remap", "inputs": []string{"member_logs"}, "source": `.project = "default"
.event_message = del(.message)
.appname = .process
.metadata.member = .member
`}}
	// Keep upstream Logflare source identities and service-specific parsing while
	// replacing container labels with supervised process names.
	routes := object{}
	for _, name := range []string{"gateway", "auth", "rest", "realtime", "storage", "functions", "db"} {
		expression := ".process == " + quote(name)
		if name == "db" {
			expression = `.process == "postgres" || .process == "patroni"`
		}
		routes[name] = expression
	}
	transforms["router"] = object{"type": "route", "inputs": []string{"project_logs"}, "route": routes}
	parsers := map[string]string{
		"gateway": `.metadata.level = "info"`,
		"auth": `parsed, err = parse_json(.event_message)
if err == null { .metadata = merge!(.metadata, parsed) }`,
		"rest": `parsed, err = parse_regex(.event_message, r'^(?P<time>.*): (?P<msg>.*)$')
if err == null {
 .event_message = parsed.msg
 parsed_time, time_err = parse_timestamp(parsed.time, format: "%d/%b/%Y:%H:%M:%S %z")
 if time_err == null { .timestamp = parsed_time }
 .metadata.host = .project
}`,
		"realtime": `.metadata.project = .project
.metadata.external_id = .project
parsed, err = parse_regex(.event_message, r'^(?P<time>\d+:\d+:\d+\.\d+) \[(?P<level>\w+)\] (?P<msg>.*)$')
if err == null { .event_message = parsed.msg; .metadata.level = parsed.level }`,
		"functions": `.metadata.project_ref = .project`,
		"storage": `.metadata.project = .project
.metadata.tenantId = .project
parsed, err = parse_json(.event_message)
if err == null {
 .event_message = parsed.msg
 .metadata.level = parsed.level
 .metadata.timestamp = parsed.time
 .metadata.context = [{"host": parsed.hostname, "pid": parsed.pid}]
}`,
		"db": `.metadata.host = "db-default"
.metadata.parsed.timestamp = .timestamp
parsed, err = parse_regex(.event_message, r'.*(?P<level>INFO|NOTICE|WARNING|ERROR|LOG|FATAL|PANIC):.*')
.metadata.parsed.error_severity = "LOG"
if err == null { .metadata.parsed.error_severity = parsed.level }`,
	}
	sinks := object{}
	names := map[string]string{"gateway": "kong.logs.prod", "auth": "gotrue.logs.prod", "rest": "postgREST.logs.prod", "realtime": "realtime.logs.prod", "storage": "storage.logs.prod.2", "functions": "deno-relay-logs", "db": "postgres.logs"}
	for name, parser := range parsers {
		input := "router." + name
		if name == "realtime" {
			transforms["realtime_filtered"] = object{"type": "filter", "inputs": []string{input}, "condition": `!contains(string!(.event_message), "/health")`}
			input = "realtime_filtered"
		}
		transforms[name+"_logs"] = object{"type": "remap", "inputs": []string{input}, "source": parser}
		sinks["logflare_"+name] = object{"type": "http", "inputs": []string{name + "_logs"}, "encoding": object{"codec": "json"}, "method": "post", "request": object{"retry_max_duration_secs": 30, "retry_initial_backoff_secs": 1, "headers": object{"x-api-key": s.LogflarePublicToken}}, "uri": "http://" + s.Upstreams["analytics"] + "/api/logs?source_name=" + url.QueryEscape(names[name]), "batch": object{"max_events": 100, "timeout_secs": 1}}
	}
	return json.MarshalIndent(object{"data_dir": dir + "/vector-data", "api": object{"enabled": true, "address": "127.0.0.1:9001"}, "sources": object{"member_logs": source}, "transforms": transforms, "sinks": sinks}, "", "  ")
}
func quote(s string) string { v, _ := json.Marshal(s); return string(v) }
