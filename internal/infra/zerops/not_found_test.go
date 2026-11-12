// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// movedNotFoundCodes are the entity codes older Zerops API versions answer
// with HTTP 400 and current ones with HTTP 404.
var movedNotFoundCodes = []string{
	"twoFactorAuthSessionNotFound", "clientNotFound", "userNotFound", "userDataNotFound", "projectEnvNotFound",
	"userTokenNotFound", "userAccountNotFound", "templateNotFound", "countryNotFound", "recordNotFound",
	"clientUserConnectionNotFound", "serviceStackNotFound", "serviceStackTypeNotFound", "serviceStackTypeVersionNotFound",
	"objectStorageNotFound", "containerNotFound", "processNotFound", "projectNotFound", "coreServiceStackTypeVersionNotFound",
	"l7HttpBalancerServiceStackTypeVersionNotFound", "publicHttpRoutingNotFound", "publicHttpRoutingServiceStackAccessPointNotFound",
	"publicHttpRoutingDomainNotFound", "userNotificationNotFound", "publicPortRoutingNotFound", "driverNotFound",
	"appVersionNotFound", "objectNotFound", "vpnPublicKeyNotFound", "autoscalingTrimProfileNotFound",
	"autoscalingTrimProfileOverrideNotFound", "autoscalingProfileNotFound", "cdnActiveDomainNotFound", "cdnActiveBucketNotFound",
	"locationInstanceNotFound", "locationNotFound",
}

type recordedError struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

// notFoundResponses returns recorded error answers by code, then by API
// version: "old" answers moved codes with HTTP 400 and usually null metadata,
// "new" with HTTP 404 and the entity ID; the generic notFound is 404 in both.
func notFoundResponses(t *testing.T) map[string]map[string]recordedError {
	t.Helper()
	data, err := os.ReadFile("testdata/not-found-errors.json")
	if err != nil {
		t.Fatal(err)
	}
	var all map[string]map[string]recordedError
	if err = json.Unmarshal(data, &all); err != nil {
		t.Fatal(err)
	}
	return all
}

type classification struct{ notFound, rejected, expired, unauthorized, unavailable bool }

func classify(err error) classification {
	return classification{
		notFound:     errors.Is(err, infra.ErrNotFound),
		rejected:     errors.Is(err, infra.ErrRejected),
		expired:      processExpired(err),
		unauthorized: errors.Is(err, infra.ErrUnauthorized),
		unavailable:  errors.Is(err, infra.ErrUnavailable),
	}
}

func TestNotFoundCodesClassifyAlikeOnOldAndNewAPI(t *testing.T) {
	if len(movedNotFoundCodes) != len(notFoundCodes) {
		t.Fatalf("%d known not-found codes, %d tested", len(notFoundCodes), len(movedNotFoundCodes))
	}
	for _, code := range movedNotFoundCodes {
		absent := code == "serviceStackNotFound" || code == "appVersionNotFound"
		want := classification{notFound: absent, rejected: !absent, expired: code == "processNotFound"}
		for _, status := range []int{http.StatusBadRequest, http.StatusNotFound} {
			err := fmt.Errorf("call: %w", &APIError{StatusCode: status, Code: code, Message: "not found"})
			if got := classify(err); got != want {
				t.Errorf("HTTP %d %s: got %+v, want %+v", status, code, got, want)
			}
			if got := notFound(err); got != absent {
				t.Errorf("notFound(HTTP %d %s) = %v, want %v", status, code, got, absent)
			}
		}
	}
}

func TestNotFoundStatusFallbacks(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		want   classification
	}{
		// HTTP 404 that names no known entity proves nothing: a gateway or
		// route answer, or a code Karakoram does not know, is transient.
		{404, "", classification{unavailable: true}},
		{404, "notFound", classification{unavailable: true}},
		{404, "volumeNotFound", classification{unavailable: true}},
		{400, "notFound", classification{rejected: true}},
		{404, "accessDenied", classification{unauthorized: true}},
		{403, "serviceStackNotFound", classification{unauthorized: true}},
		{500, "serviceStackNotFound", classification{unavailable: true}},
		{409, "processNotFound", classification{}},
	} {
		err := &APIError{StatusCode: tc.status, Code: tc.code}
		if got := classify(err); got != tc.want {
			t.Errorf("HTTP %d %q: got %+v, want %+v", tc.status, tc.code, got, tc.want)
		}
	}
	if notFound(errors.New("not found")) || notFound(nil) || processExpired(nil) {
		t.Fatal("non-API errors classified as missing")
	}
}

// A 404 from a gateway, proxy or unknown route carries no Zerops error
// envelope; it must never read as the requested service being gone.
func TestUnexplainedNotFoundIsTransient(t *testing.T) {
	for name, body := range map[string]string{
		"html":         "<html><body><h1>404 Not Found</h1></body></html>",
		"empty":        "",
		"no envelope":  `{"message":"Not Found"}`,
		"generic":      `{"error":{"code":"notFound","message":"Not Found","meta":[{"error":"Not Found","code":"notFound","metadata":null}]}}`,
		"unknown code": `{"error":{"code":"volumeNotFound","message":"Volume not found."}}`,
	} {
		t.Run(name, func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/project/ExampleID0000000000077/service-stack" {
					_ = json.NewEncoder(w).Encode(page[ServiceStack]{Count: 1, Total: 1, List: []ServiceStack{{ID: "ExampleID0000000000101", ProjectID: "ExampleID0000000000077", Name: "app"}}})
					return
				}
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(body))
			})
			s, err := b.GetService(t.Context(), "app")
			if s != nil || err == nil || errors.Is(err, infra.ErrNotFound) || errors.Is(err, infra.ErrRejected) || !errors.Is(err, infra.ErrUnavailable) {
				t.Fatalf("unexplained 404 classified as %+v: %+v %v", classify(err), s, err)
			}
			if _, err = b.DeleteServiceByID(t.Context(), "app", "ExampleID0000000000101"); err == nil || errors.Is(err, infra.ErrNotFound) {
				t.Fatalf("delete after unexplained 404: %v", err)
			}
		})
	}
}

// A mutation answered by an unexplained 404 may have reached the platform, so
// its receipt must stay pending; a known entity code is a definitive answer.
func TestUnexplainedNotFoundMutationIsAmbiguous(t *testing.T) {
	for _, tc := range []struct {
		status    int
		code      string
		ambiguous bool
	}{
		{404, "", true}, {404, "notFound", true}, {404, "volumeNotFound", true}, {502, "", true},
		{404, "serviceStackNotFound", false}, {400, "serviceStackNotFound", false}, {404, "appVersionNotFound", false},
		{404, "projectNotFound", false}, {400, "invalidValue", false}, {429, "", false}, {404, "accessDenied", false},
	} {
		if got := ambiguousMutation(fmt.Errorf("mutation: %w", &APIError{StatusCode: tc.status, Code: tc.code})); got != tc.ambiguous {
			t.Errorf("HTTP %d %q: ambiguous=%v, want %v", tc.status, tc.code, got, tc.ambiguous)
		}
	}
}

func TestRecordedNotFoundResponsesOldAndNewAPI(t *testing.T) {
	for code, versions := range notFoundResponses(t) {
		t.Run(code, func(t *testing.T) {
			var errs []error
			for _, version := range []string{"old", "new"} {
				recorded, ok := versions[version]
				if !ok {
					t.Fatalf("missing %s response", version)
				}
				b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(recorded.Status)
					_, _ = w.Write(recorded.Body)
				})
				_, err := b.client.GetProcess(t.Context(), "ExampleID0000000000103")
				var api *APIError
				if !errors.As(err, &api) || api.Code != code || api.StatusCode != recorded.Status {
					t.Fatalf("%s: decoded %#v from %v", version, api, err)
				}
				var recordedBody struct {
					Error struct{ Meta json.RawMessage } `json:"error"`
				}
				var want, got any
				if json.Unmarshal(recorded.Body, &recordedBody) != nil || json.Unmarshal(recordedBody.Error.Meta, &want) != nil ||
					json.Unmarshal(api.Meta, &got) != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s: meta %s, recorded %s", version, api.Meta, recordedBody.Error.Meta)
				}
				// Entity IDs in the new metadata are not diagnostics.
				if strings.Contains(err.Error(), "ExampleID") || strings.Contains(err.Error(), "postgresql@99") {
					t.Fatalf("%s: metadata leaked into %q", version, err)
				}
				errs = append(errs, err)
			}
			if old, current := classify(errs[0]), classify(errs[1]); old != current {
				t.Fatalf("old API %+v, new API %+v", old, current)
			}
			status := func(err error) string {
				return strings.Replace(err.Error(), "zerops HTTP 404:", "zerops HTTP 400:", 1)
			}
			if status(errs[0]) != status(errs[1]) {
				t.Fatalf("diagnostics differ beyond status: %q vs %q", errs[0], errs[1])
			}
		})
	}
}

func TestAdapterNotFoundBehaviourOnOldAndNewAPI(t *testing.T) {
	responses := notFoundResponses(t)
	const (
		project   = "ExampleID0000000000077"
		serviceID = "ExampleID0000000000101"
		envID     = "ExampleID0000000000104"
	)
	service := ServiceStack{ID: serviceID, ProjectID: project, Name: "app", Status: "ACTIVE"}
	list := page[ServiceStack]{Count: 1, Total: 1, Limit: 100, List: []ServiceStack{service}}
	for _, version := range []string{"old", "new"} {
		answer := func(w http.ResponseWriter, code string) {
			recorded, ok := responses[code][version]
			if !ok {
				t.Errorf("missing %s %s response", version, code)
			}
			w.WriteHeader(recorded.Status)
			_, _ = w.Write(recorded.Body)
		}
		t.Run(version+"/deleted service resolves as absent", func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/project/"+project+"/service-stack" {
					_ = json.NewEncoder(w).Encode(list)
					return
				}
				answer(w, "serviceStackNotFound")
			})
			if s, err := b.GetService(t.Context(), "app"); err != nil || s != nil {
				t.Fatalf("deleted service: %+v %v", s, err)
			}
		})
		t.Run(version+"/missing project is not a missing service", func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) { answer(w, "projectNotFound") })
			s, err := b.GetService(t.Context(), "app")
			if s != nil || !errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrNotFound) {
				t.Fatalf("missing project: %+v %v", s, err)
			}
		})
		t.Run(version+"/missing app version is absent", func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) { answer(w, "appVersionNotFound") })
			if _, err := b.version(t.Context(), "ExampleID0000000000102"); !notFound(err) || errors.Is(err, infra.ErrRejected) {
				t.Fatalf("missing app version: %v", err)
			}
		})
		t.Run(version+"/expired process", func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) { answer(w, "processNotFound") })
			state, err := b.OpStatus(t.Context(), infra.Op{ID: "ExampleID0000000000103", Kind: "import"})
			if err != nil || !state.Done || !state.Expired || state.Failed || state.Status != "EXPIRED" {
				t.Fatalf("expired process: %+v %v", state, err)
			}
		})
		t.Run(version+"/missing environment entry is a rejection", func(t *testing.T) {
			b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/project/" + project + "/service-stack":
					_ = json.NewEncoder(w).Encode(list)
				case "/service-stack/" + serviceID:
					_ = json.NewEncoder(w).Encode(service)
				case "/service-stack/" + serviceID + "/env":
					_ = json.NewEncoder(w).Encode(map[string][]EnvItem{"items": {{ID: envID, Key: "MODE", Content: "a", Type: "USER"}}})
				case "/user-data/" + envID:
					answer(w, "userDataNotFound")
				default:
					t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusTeapot)
				}
			})
			err := b.SetEnvVars(t.Context(), "app", map[string]string{"MODE": "b"})
			if !errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrNotFound) {
				t.Fatalf("missing environment entry: %v", err)
			}
		})
		t.Run(version+"/unknown service base is a rejection", func(t *testing.T) {
			for _, code := range []string{"serviceStackTypeNotFound", "serviceStackTypeVersionNotFound"} {
				b := newTestBackend(t, func(w http.ResponseWriter, r *http.Request) { answer(w, code) })
				_, err := b.client.ImportServices(t.Context(), "services: []")
				if !errors.Is(err, infra.ErrRejected) || errors.Is(err, infra.ErrNotFound) {
					t.Fatalf("%s: %v", code, err)
				}
			}
		})
	}
}
