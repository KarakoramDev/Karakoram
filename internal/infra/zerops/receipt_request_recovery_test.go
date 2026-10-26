// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestVersionRequestPreSendCrashRecovery(t *testing.T) {
	for _, stage := range []string{"uploading", "deploying"} {
		t.Run(stage, func(t *testing.T) {
			server := newFakeAPI(t)
			plain := hardeningBackend(t, server, nil)
			id := importHardeningRuntime(t, plain)
			uploads, deploys := 0, 0
			crashed := false
			transport := receiptTransport(func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/upload") {
					uploads++
				}
				if strings.HasSuffix(r.URL.Path, "/deploy") {
					deploys++
				}
				var entry EnvItem
				if r.Method != http.MethodGet && strings.Contains(r.URL.Path, "user-data") {
					body, err := r.GetBody()
					if err != nil {
						return nil, err
					}
					_ = json.NewDecoder(body).Decode(&entry)
					body.Close()
				}
				response, err := http.DefaultTransport.RoundTrip(r)
				var marker operationReceipt
				if err == nil && !crashed && entry.Key == operationReceiptKey && json.Unmarshal([]byte(entry.Content), &marker) == nil && marker.Stage == stage {
					crashed = true
					_, _ = io.Copy(io.Discard, response.Body)
					response.Body.Close()
					panic("simulated pre-send control-plane crash")
				}
				return response, err
			})
			backend := func() *Backend {
				b := hardeningBackend(t, server, transport)
				b.client.http.Timeout = 100 * time.Millisecond
				return b
			}
			b := backend()
			func() {
				defer func() {
					if recover() == nil {
						t.Error("crash boundary was not reached")
					}
				}()
				_, _ = b.Deploy(t.Context(), "member", hardeningDeployment())
			}()
			marker, err := plain.receipt(t.Context(), id)
			if err != nil || marker.Version == "" || marker.Stage != stage {
				t.Fatalf("crash lost selected receipt: %+v %v", marker, err)
			}
			version := marker.Version
			if stage == "uploading" && uploads != 0 || stage == "deploying" && deploys != 0 {
				t.Fatal("request was sent before crash")
			}
			recovered := backend()
			var op infra.Op
			if stage == "uploading" {
				_, err = recovered.Deploy(t.Context(), "member", hardeningDeployment())
				if !errors.Is(err, infra.ErrConflict) || infra.RetryAt(err).IsZero() {
					t.Fatalf("missing upload recovery wait: %v", err)
				}
				time.Sleep(250 * time.Millisecond)
				op, err = backend().Deploy(t.Context(), "member", hardeningDeployment())
			} else {
				op = infra.Op{ID: "version:" + version, Kind: "deploy", ServiceID: id}
				_, err = recovered.OpStatus(t.Context(), op)
				if !errors.Is(err, infra.ErrConflict) || infra.RetryAt(err).IsZero() {
					t.Fatalf("missing deploy recovery wait: %v", err)
				}
				time.Sleep(250 * time.Millisecond)
				state, e := backend().OpStatus(t.Context(), op)
				if e != nil || !state.Done || !state.Failed || !strings.Contains(state.Message, "retry") {
					t.Fatalf("unsent attempt did not become retryable: %+v %v", state, e)
				}
				op, err = backend().Deploy(t.Context(), "member", hardeningDeployment())
			}
			if err != nil || op.ID != "version:"+version {
				t.Fatalf("recovery lost version: %+v %v", op, err)
			}
			state, err := backend().OpStatus(t.Context(), op)
			if err != nil || !state.Done || state.Failed {
				t.Fatalf("recovery did not converge: %+v %v", state, err)
			}
			versions, err := getPages[AppVersion](t.Context(), plain.client, "/service-stack/"+id+"/app-version")
			if err != nil || len(versions) != 1 || uploads != 1 || deploys != 1 {
				t.Fatalf("versions=%d uploads=%d deploys=%d err=%v", len(versions), uploads, deploys, err)
			}
		})
	}
}

func TestVersionDiagnosticPreservesCooldown(t *testing.T) {
	for _, status := range []int{429, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			server := newFakeAPI(t)
			b := hardeningBackend(t, server, nil)
			importHardeningRuntime(t, b)
			op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
			if err != nil {
				t.Fatal(err)
			}
			subsequent := 0
			limited := false
			b.client.http.Transport = receiptTransport(func(r *http.Request) (*http.Response, error) {
				if limited {
					subsequent++
				}
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/app-version") {
					limited = true
					return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{"60"}}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				}
				return http.DefaultTransport.RoundTrip(r)
			})
			before := time.Now()
			state, err := b.OpStatus(t.Context(), op)
			if !errors.Is(err, infra.ErrUnavailable) || infra.RetryAt(err).Before(before.Add(59*time.Second)) || subsequent != 0 || state.Done {
				t.Fatalf("cooldown swallowed: state=%+v err=%v subsequentRequests=%d", state, err, subsequent)
			}
		})
	}
}

func TestVersionRequestAdoptsStartedDeployWithLaggingVersion(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, plain)
	deploys, writesWhileRunning := 0, 0
	processStatus := "RUNNING"
	transport := receiptTransport(func(r *http.Request) (*http.Response, error) {
		if deploys > 0 && processStatus == "RUNNING" && r.Method != http.MethodGet && strings.Contains(r.URL.Path, "user-data") {
			writesWhileRunning++
		}
		response, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			return response, err
		}
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/deploy") {
			deploys++
		}
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/app-version/") {
			var version AppVersion
			_ = json.NewDecoder(response.Body).Decode(&version)
			response.Body.Close()
			version.Status = "WAITING_TO_DEPLOY"
			data, _ := json.Marshal(version)
			response.Body = io.NopCloser(strings.NewReader(string(data)))
		}
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/process") {
			var processes page[Process]
			_ = json.NewDecoder(response.Body).Decode(&processes)
			response.Body.Close()
			for i := range processes.List {
				if processes.List[i].AppVersion != nil {
					processes.List[i].Status = processStatus
				}
			}
			data, _ := json.Marshal(processes)
			response.Body = io.NopCloser(strings.NewReader(string(data)))
		}
		return response, nil
	})
	b := hardeningBackend(t, server, transport)
	op, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil {
		t.Fatal(err)
	}
	recovered := hardeningBackend(t, server, transport)
	retry, err := recovered.Deploy(t.Context(), "member", hardeningDeployment())
	if err != nil || retry.ID != op.ID {
		t.Fatalf("accepted deploy was not adopted: %+v %v", retry, err)
	}
	state, err := recovered.OpStatus(t.Context(), op)
	if err != nil || state.Done || state.Failed || state.Status != "RUNNING" || deploys != 1 || writesWhileRunning != 0 {
		t.Fatalf("original process was replayed or blocked by a receipt write: %+v %v deploys=%d writes=%d", state, err, deploys, writesWhileRunning)
	}
	processStatus = "FINISHED"
	state, err = recovered.OpStatus(t.Context(), op)
	if err != nil || !state.Done || state.Failed || deploys != 1 {
		t.Fatalf("original deployment completion lost: %+v %v deploys=%d", state, err, deploys)
	}
	versions, err := getPages[AppVersion](t.Context(), plain.client, "/service-stack/"+id+"/app-version")
	if err != nil || len(versions) != 1 {
		t.Fatalf("adoption allocated another version: %+v %v", versions, err)
	}
}
