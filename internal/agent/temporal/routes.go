// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package temporal

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"golang.org/x/net/http2"
	"google.golang.org/protobuf/encoding/protowire"
)

func frontendTLS(s tspec.Spec) (*tls.Config, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(s.TLS.CAPEM)) {
		return nil, errors.New("invalid Temporal CA")
	}
	cert, err := tls.X509KeyPair([]byte(s.TLS.InternodeCertPEM), []byte(s.TLS.InternodeKeyPEM))
	if err != nil {
		return nil, errors.New("invalid Temporal client identity")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}, ServerName: s.TLS.FrontendServerName}, nil
}

type frontendError struct {
	code         int
	grpcCode     int
	method, path string
}

// Error describes the failed request without exposing the upstream response body.
func (e frontendError) Error() string {
	return fmt.Sprintf("Temporal frontend %s %s failed (HTTP %d)", e.method, e.path, e.code)
}

func frontendRequest(ctx context.Context, s tspec.Spec, method, path string, body any) ([]byte, error) {
	config, err := frontendTLS(s)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{TLSClientConfig: config, Proxy: nil}
	defer transport.CloseIdleConnections()
	var data []byte
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return nil, errors.New("encode Temporal API request failed")
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, "https://127.0.0.1:7243"+path, bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("create Temporal API request failed")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Transport: transport, Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return nil, errors.New("temporal frontend is unavailable")
	}
	defer response.Body.Close()
	return frontendResponse(response, method, path)
}

func frontendResponse(response *http.Response, method, path string) ([]byte, error) {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		failure := frontendError{code: response.StatusCode, method: method, path: strings.SplitN(path, "?", 2)[0]}
		// The gateway maps FailedPrecondition to HTTP 400. Retain its numeric
		// gRPC code to distinguish conflicts without forwarding diagnostic text.
		var envelope struct {
			Code int `json:"code"`
		}
		if json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&envelope) == nil && envelope.Code > 0 && envelope.Code <= 16 {
			failure.grpcCode = envelope.Code
		}
		return nil, failure
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil || len(data) > 4<<20 {
		return nil, errors.New("temporal API response exceeds limit or is unreadable")
	}
	return data, nil
}

type namespaceResponse struct {
	Info   struct{ Name, Description, State string } `json:"namespaceInfo"`
	Config struct {
		Retention string `json:"workflowExecutionRetentionTtl"`
	} `json:"config"`
}

func (n namespaceResponse) status() (tspec.NamespaceStatus, error) {
	d, err := time.ParseDuration(n.Config.Retention)
	if err != nil {
		return tspec.NamespaceStatus{}, errors.New("invalid Temporal namespace retention response")
	}
	state := strings.TrimPrefix(n.Info.State, "NAMESPACE_STATE_")
	switch state {
	case "REGISTERED":
		state = "Registered"
	case "DEPRECATED":
		state = "Deprecated"
	case "DELETED":
		state = "Deleted"
	}
	return tspec.NamespaceStatus{Namespace: tspec.Namespace{Name: n.Info.Name, Description: n.Info.Description, Retention: d.String()}, State: state}, nil
}

func listNamespaces(ctx context.Context, s tspec.Spec) ([]tspec.NamespaceStatus, error) {
	result := []tspec.NamespaceStatus{}
	token := ""
	for {
		path := "/api/v1/namespaces?pageSize=100"
		if token != "" {
			path += "&nextPageToken=" + url.QueryEscape(token)
		}
		data, err := frontendRequest(ctx, s, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		var page struct {
			Namespaces    []namespaceResponse `json:"namespaces"`
			NextPageToken string              `json:"nextPageToken"`
		}
		if json.Unmarshal(data, &page) != nil {
			return nil, errors.New("decode namespace list failed")
		}
		for _, entry := range page.Namespaces {
			status, err := entry.status()
			if err != nil {
				return nil, err
			}
			result = append(result, status)
		}
		if page.NextPageToken == "" {
			return result, nil
		}
		if page.NextPageToken == token || len(result) > 10000 {
			return nil, errors.New("temporal namespace pagination limit exceeded")
		}
		token = page.NextPageToken
	}
}

func ensureNamespaces(ctx context.Context, s tspec.Spec, desired []tspec.Namespace) error {
	for _, namespace := range desired {
		if err := namespace.Validate(); err != nil {
			return err
		}
	}
	for _, namespace := range desired {
		path := "/api/v1/namespaces/" + url.PathEscape(namespace.Name)
		data, err := frontendRequest(ctx, s, http.MethodGet, path, nil)
		duration, _ := time.ParseDuration(namespace.Retention)
		retention := strconv.FormatInt(int64(duration/time.Second), 10) + "s"
		if err != nil {
			var apiError frontendError
			if !errors.As(err, &apiError) || apiError.code != http.StatusNotFound {
				return err
			}
			_, err = frontendRequest(ctx, s, http.MethodPost, "/api/v1/namespaces", configMap{"namespace": namespace.Name, "description": namespace.Description, "workflowExecutionRetentionPeriod": retention})
			// A concurrent ensure may have won the registration race. Re-read before
			// deciding whether to update; never hide other registration failures.
			if err != nil {
				if !errors.As(err, &apiError) || apiError.code != http.StatusConflict {
					return err
				}
			} else {
				continue
			}
			data, err = frontendRequest(ctx, s, http.MethodGet, path, nil)
			if err != nil {
				return err
			}
		}
		var current namespaceResponse
		if json.Unmarshal(data, &current) != nil {
			return errors.New("decode namespace failed")
		}
		observed, err := time.ParseDuration(current.Config.Retention)
		if err != nil {
			return errors.New("invalid namespace retention response")
		}
		// Temporal treats an empty update description as "leave unchanged".
		if observed == duration && (namespace.Description == "" || current.Info.Description == namespace.Description) {
			continue
		}
		if _, err = frontendRequest(ctx, s, http.MethodPost, path+"/update", configMap{"namespace": namespace.Name, "updateInfo": configMap{"description": namespace.Description}, "config": configMap{"workflowExecutionRetentionTtl": retention}}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteNamespace has no HTTP gateway binding upstream. Its unary protobuf
// request contains only namespace (field 1); use the same pinned mTLS identity.
func deleteNamespace(ctx context.Context, s tspec.Spec, name string) error {
	config, err := frontendTLS(s)
	if err != nil {
		return err
	}
	transport := &http2.Transport{TLSClientConfig: config}
	defer transport.CloseIdleConnections()
	message := protowire.AppendTag(nil, 1, protowire.BytesType)
	message = protowire.AppendString(message, name)
	frame := make([]byte, 5, len(message)+5)
	binary.BigEndian.PutUint32(frame[1:], uint32(len(message)))
	frame = append(frame, message...)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://127.0.0.1:7233/temporal.api.operatorservice.v1.OperatorService/DeleteNamespace", bytes.NewReader(frame))
	if err != nil {
		return errors.New("create namespace deletion request failed")
	}
	request.Header.Set("Content-Type", "application/grpc")
	request.Header.Set("TE", "trailers")
	response, err := (&http.Client{Transport: transport, Timeout: 30 * time.Second}).Do(request)
	if err != nil {
		return errors.New("temporal namespace deletion unavailable")
	}
	defer response.Body.Close()
	if _, err = io.Copy(io.Discard, io.LimitReader(response.Body, 4<<20)); err != nil {
		return errors.New("read namespace deletion response failed")
	}
	return deletionResponse(response)
}

// grpcStatusError retains only the wire code, never upstream diagnostic text.
type grpcStatusError struct{ code int }

// Error reports the gRPC code without exposing the upstream message.
func (e grpcStatusError) Error() string {
	return fmt.Sprintf("Temporal namespace deletion failed (gRPC status %d; server details withheld)", e.code)
}

func deletionResponse(response *http.Response) error {
	status := response.Trailer.Get("Grpc-Status")
	if status == "" {
		status = response.Header.Get("Grpc-Status")
	}
	code, err := strconv.Atoi(status)
	if err != nil || code < 0 || code > 16 || response.StatusCode != http.StatusOK {
		return grpcStatusError{code: 2}
	}
	if code != 0 {
		return grpcStatusError{code: code}
	}
	return nil
}

func namespaceHTTPStatus(err error) int {
	var rpcError grpcStatusError
	if errors.As(err, &rpcError) {
		switch rpcError.code {
		case 3: // InvalidArgument
			return http.StatusBadRequest
		case 5: // NotFound
			return http.StatusNotFound
		case 6, 9: // AlreadyExists, FailedPrecondition
			return http.StatusConflict
		}
	}
	var apiError frontendError
	if errors.As(err, &apiError) {
		if apiError.grpcCode != 0 {
			return namespaceHTTPStatus(grpcStatusError{code: apiError.grpcCode})
		}
		switch apiError.code {
		case http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusNotFound, http.StatusConflict:
			return apiError.code
		}
	}
	return http.StatusServiceUnavailable
}

func writeNamespaceError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), namespaceHTTPStatus(err))
}

func decodeBody(w http.ResponseWriter, request *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, 64<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(target) != nil || decoder.Decode(new(any)) != io.EOF {
		http.Error(w, "invalid Temporal request body", http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(value)
}

// Routes registers authenticated Temporal status and namespace actions.
func (r *Runner) Routes(mux *http.ServeMux, n *agent.Node) {
	status := func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			var body struct{}
			if !decodeBody(w, request, &body) {
				return
			}
		}
		writeJSON(w, r.status(request.Context(), n))
	}
	mux.HandleFunc("GET /v1/temporal/"+tspec.ActionStatus, status)
	mux.HandleFunc("POST /v1/temporal/"+tspec.ActionStatus, status)
	frontend := func(w http.ResponseWriter) (tspec.Spec, bool) {
		s, ok := r.current()
		if !ok || !hasFrontend(s) {
			http.Error(w, "action requires a frontend member", http.StatusConflict)
			return s, false
		}
		return s, true
	}
	mux.HandleFunc("POST /v1/temporal/"+tspec.ActionNamespaces, func(w http.ResponseWriter, request *http.Request) {
		s, ok := frontend(w)
		if !ok {
			return
		}
		var body struct{}
		if !decodeBody(w, request, &body) {
			return
		}
		result, err := listNamespaces(request.Context(), s)
		if err != nil {
			writeNamespaceError(w, err)
			return
		}
		writeJSON(w, result)
	})
	mux.HandleFunc("POST /v1/temporal/"+tspec.ActionEnsureNamespaces, func(w http.ResponseWriter, request *http.Request) {
		s, ok := frontend(w)
		if !ok {
			return
		}
		var body []tspec.Namespace
		if !decodeBody(w, request, &body) {
			return
		}
		if len(body) == 0 || len(body) > 100 {
			http.Error(w, "expected 1–100 namespaces", http.StatusBadRequest)
			return
		}
		seen := map[string]bool{}
		for _, namespace := range body {
			if err := namespace.Validate(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if seen[namespace.Name] {
				http.Error(w, "duplicate namespace", http.StatusBadRequest)
				return
			}
			seen[namespace.Name] = true
		}
		if err := ensureNamespaces(request.Context(), s, body); err != nil {
			writeNamespaceError(w, err)
			return
		}
		result, err := listNamespaces(request.Context(), s)
		if err != nil {
			writeNamespaceError(w, err)
			return
		}
		writeJSON(w, result)
	})
	mux.HandleFunc("POST /v1/temporal/"+tspec.ActionDeleteNamespace, func(w http.ResponseWriter, request *http.Request) {
		s, ok := frontend(w)
		if !ok {
			return
		}
		var body tspec.DeleteNamespace
		if !decodeBody(w, request, &body) {
			return
		}
		if (tspec.Namespace{Name: body.Name, Retention: "24h"}).Validate() != nil {
			http.Error(w, "invalid namespace name", http.StatusBadRequest)
			return
		}
		if body.Name == "default" || body.Name == "temporal-system" {
			http.Error(w, "system namespace cannot be deleted", http.StatusConflict)
			return
		}
		if err := deleteNamespace(request.Context(), s, body.Name); err != nil {
			writeNamespaceError(w, err)
			return
		}
		writeJSON(w, struct{}{})
	})
}
