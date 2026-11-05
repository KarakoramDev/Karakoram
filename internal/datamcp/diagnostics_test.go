// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package datamcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"
)

const diagnosticCanary = "password=synthetic-canary SELECT private_value FROM private_table https://user:credential@example.invalid/?token=synthetic"

func TestClassifyListToolsFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		category string
		typeName string
	}{
		{"none", nil, "unknown", ""},
		{"unknown", errors.New(diagnosticCanary), "unknown", "*errors.errorString"},
		{"wrapped rpc", fmt.Errorf("%s: %w", diagnosticCanary, &jsonrpc.Error{Code: -32603, Message: diagnosticCanary}), "jsonrpc", "*jsonrpc2.WireError"},
		{"http client", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 403}, Body: []byte(diagnosticCanary)}, "http_4xx", "*oauth2.RetrieveError"},
		{"http server", &oauth2.RetrieveError{Response: &http.Response{StatusCode: 503}}, "http_5xx", "*oauth2.RetrieveError"},
		{"decode", &json.UnmarshalTypeError{Value: diagnosticCanary}, "schema_decoding", "*json.UnmarshalTypeError"},
		{"syntax", &json.SyntaxError{}, "schema_decoding", "*json.SyntaxError"},
		{"transport", &url.Error{Op: diagnosticCanary, URL: diagnosticCanary, Err: net.ErrClosed}, "transport", "*url.Error"},
		{"untyped status", errors.New("HTTP 503 " + diagnosticCanary), "unknown", "*errors.errorString"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := classifyListToolsFailure(tc.err)
			if d.category != tc.category || (tc.typeName != "" && !slices.Contains(d.types, tc.typeName)) {
				t.Fatalf("unexpected classification: category=%s types=%v", d.category, d.types)
			}
			var b bytes.Buffer
			slog.New(slog.NewJSONHandler(&b, nil)).LogAttrs(context.Background(), slog.LevelError, "failure", d.attrs()...)
			for _, forbidden := range []string{"synthetic-canary", "SELECT", "credential", "example.invalid", "private_table"} {
				if strings.Contains(b.String(), forbidden) {
					t.Fatalf("unsafe diagnostic contains %q", forbidden)
				}
			}
		})
	}
}

func TestListToolsFailureSentinelsAndBounds(t *testing.T) {
	err := errors.Join(
		fmt.Errorf("%s: %w", diagnosticCanary, context.Canceled),
		context.DeadlineExceeded, io.EOF, net.ErrClosed,
		mcp.ErrSessionMissing, mcp.ErrConnectionClosed,
	)
	d := classifyListToolsFailure(err)
	if !d.contextCanceled || !d.deadlineExceeded || !d.eof || !d.netClosed || !d.sessionMissing || !d.connectionClosed || d.truncated {
		t.Fatal("joined/wrapped sentinel flags missing or unexpectedly truncated")
	}
	deep := error(io.EOF)
	for range maxDiagnosticErrors + 2 {
		deep = fmt.Errorf("%s: %w", diagnosticCanary, deep)
	}
	wide := make([]error, maxDiagnosticErrors+2)
	for i := range wide {
		wide[i] = errors.New(diagnosticCanary)
	}
	for _, err := range []error{deep, errors.Join(wide...), &diagnosticCycle{}} {
		d := classifyListToolsFailure(err)
		if !d.truncated || len(d.types) > maxDiagnosticErrors {
			t.Fatal("error traversal is not bounded")
		}
	}
	if d := classifyListToolsFailure(nil); len(d.types) != 0 || d.truncated {
		t.Fatal("nil error has a chain")
	}
}

type diagnosticCycle struct{}

func (*diagnosticCycle) Error() string   { panic("diagnostics must not format errors") }
func (e *diagnosticCycle) Unwrap() error { return e }

type diagnosticUpstream struct {
	Upstream
	err error
}

func (u diagnosticUpstream) ListTools(context.Context) ([]*mcp.Tool, error) {
	return nil, u.err
}

func TestListToolsFailureLogging(t *testing.T) {
	for _, failLevel := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		t.Run(string(failLevel), func(t *testing.T) {
			x := setup(t)
			var b bytes.Buffer
			x.g.o.Logger = slog.New(slog.NewJSONHandler(&b, nil))
			ref := domain.ClusterRef{Namespace: "team", Name: "db"}
			for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
				u, release, err := x.g.upstream(context.Background(), ref, x.c, level)
				if err != nil {
					t.Fatal(err)
				}
				release()
				key := ref.Namespace + "/" + ref.Name + ":" + string(level)
				if level == failLevel {
					x.g.upstreams[key].upstream = diagnosticUpstream{Upstream: u.upstream, err: fmt.Errorf("%s: %w", diagnosticCanary, net.ErrClosed)}
				}
			}
			_, err := x.g.tools(context.Background(), ref, x.c, AccessDecision{Write: true}, x.g.o.Manifests["test"])
			if err == nil || err.Error() != "tool listing unavailable" {
				t.Fatal("client error changed")
			}
			var record map[string]any
			if err := json.Unmarshal(b.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["accessLevel"] != string(failLevel) || record["netClosed"] != true || record["upstreamCategory"] != "transport" {
				t.Fatal("missing safe gateway classification")
			}
			if strings.Contains(b.String(), "synthetic") || strings.Contains(b.String(), "SELECT") || strings.Contains(b.String(), "credential") {
				t.Fatal("raw upstream error leaked")
			}
		})
	}
}
