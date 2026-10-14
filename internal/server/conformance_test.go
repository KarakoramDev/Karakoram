// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/apispec"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
)

func init() {
	// SSE is text; the YAML download is an OpenAPI document object.
	openapi3filter.RegisterBodyDecoder("text/event-stream", openapi3filter.PlainBodyDecoder)
	openapi3filter.RegisterBodyDecoder("application/yaml", openapi3filter.YamlBodyDecoder)
}

// Every public handler used by this package's fixtures goes through the same
// response validator, including cookie requests and real streaming HTTP servers.
var conformanceTests sync.Map // *Server -> *testing.T

func registerConformance(t *testing.T, s *Server) {
	conformanceTests.Store(s, t)
	t.Cleanup(func() { conformanceTests.Delete(s) })
}

func testHandler(s *Server) http.Handler {
	return conformingHandler(s, s.Handler())
}

func testStream(s *Server, ctx context.Context, w http.ResponseWriter) error {
	var streamErr error
	conformingHandler(s, http.HandlerFunc(func(out http.ResponseWriter, _ *http.Request) {
		streamErr = s.stream(ctx).serve(out)
	})).ServeHTTP(w, request(ctx))
	return streamErr
}

func conformingHandler(s *Server, handler http.Handler) http.Handler {
	owner, ok := conformanceTests.Load(s)
	if !ok {
		panic("server fixture has no conformance test owner")
	}
	t := owner.(*testing.T)
	doc, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorded := &conformanceWriter{ResponseWriter: w}
		handler.ServeHTTP(recorded, r)
		route, params, err := router.FindRoute(r)
		if err != nil {
			// SPA/relay routes and intentionally nonexistent API paths are not OpenAPI
			// operations. Their behavior is asserted by the calling regression tests.
			return
		}
		status := recorded.status
		if status == 0 {
			status = http.StatusOK
		}
		input := &openapi3filter.ResponseValidationInput{
			RequestValidationInput: &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route},
			Status:                 status, Header: recorded.Header(), Body: http.NoBody,
			Options: &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
		}
		input.SetBodyBytes(recorded.body.Bytes())
		if err := openapi3filter.ValidateResponse(context.Background(), input); err != nil {
			t.Errorf("OpenAPI response %s (%s %s), status %d: %v", route.Operation.OperationID, r.Method, r.URL.Path, status, err)
		}
		if route.Operation.OperationID == "streamEvents" && status == http.StatusOK {
			for _, line := range bytes.Split(recorded.body.Bytes(), []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("data:")) {
					continue
				}
				var event any
				if err := json.Unmarshal(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:"))), &event); err != nil {
					t.Errorf("OpenAPI response streamEvents /data: invalid JSON: %v", err)
					continue
				}
				if err := doc.Components.Schemas["Event"].Value.VisitJSON(event, openapi3.MultiErrors(), openapi3.VisitAsResponse()); err != nil {
					t.Errorf("OpenAPI response streamEvents /data: %v", err)
				}
			}
		}
	})
}

type conformanceWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *conformanceWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *conformanceWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.body.Write(p[:n])
	return n, err
}
func (w *conformanceWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *conformanceWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
