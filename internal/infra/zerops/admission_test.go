// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type admissionFixture struct {
	mu                sync.Mutex
	processes         []Process
	versions          []AppVersion
	listCode          int
	evidenceCode      int
	processReads      int
	mutations         int
	activeReads       int
	afterHistoryPage  func(*admissionFixture, *http.Request)
	filterCode        int
	afterFilteredPage func(*admissionFixture, *http.Request)
}

func admissionBackend(t *testing.T) (*Backend, *admissionFixture) {
	t.Helper()
	f := &admissionFixture{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.Method != "GET" {
			f.mutations++
			t.Errorf("unexpected mutation %s %s", r.Method, r.URL.Path)
			w.WriteHeader(500)
			return
		}
		switch {
		case r.URL.Path == "/project/project/process":
			t.Error("admission scanned project process history")
			w.WriteHeader(http.StatusBadRequest)
		case r.URL.Path == "/project/project/service-stack":
			fakePage(w, r, []ServiceStack{{ID: "a", Name: "membera"}, {ID: "b", Name: "memberb"}, {ID: "foreign", Name: "foreign"}, {ID: "core", Name: "core", IsSystem: true}})
		case strings.HasPrefix(r.URL.Path, "/process/"):
			f.processReads++
			id := strings.TrimPrefix(r.URL.Path, "/process/")
			for _, p := range f.processes {
				if p.ID == id {
					_ = json.NewEncoder(w).Encode(p)
					return
				}
			}
			w.WriteHeader(http.StatusNotFound)
		case strings.HasSuffix(r.URL.Path, "/env"):
			owner := "owner"
			if strings.Contains(r.URL.Path, "foreign") {
				owner = ""
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []EnvItem{{Key: "KARAKORAM_OWNER", Content: owner, Type: "USER"}}})
		case strings.HasSuffix(r.URL.Path, "/process"):
			filtered := r.URL.Query().Has("statuses")
			if filtered {
				f.activeReads++
				if f.filterCode != 0 {
					w.WriteHeader(f.filterCode)
					return
				}
			} else {
				f.processReads++
			}
			if f.listCode != 0 {
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(f.listCode)
				return
			}
			if f.evidenceCode != 0 {
				w.WriteHeader(f.evidenceCode)
				return
			}
			list := []Process{}
			for _, p := range f.processes {
				if filtered && !slices.Contains(r.URL.Query()["statuses"], p.Status) {
					continue
				}
				if "/service-stack/"+p.ServiceStackID+"/process" == r.URL.Path {
					list = append(list, p)
				}
			}
			fakePage(w, r, list)
			if !filtered && f.afterHistoryPage != nil {
				f.afterHistoryPage(f, r)
			}
			if filtered && f.afterFilteredPage != nil {
				f.afterFilteredPage(f, r)
			}
		case strings.HasSuffix(r.URL.Path, "/app-version"):
			fakePage(w, r, f.versions)
		default:
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	b, err := New(Options{BaseURL: server.URL, Token: "token", ProjectID: "project", MaxRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	return b, f
}
func admissionProcess(id, service, status string) Process {
	return Process{ID: id, ServiceStackID: service, Status: status, ActionName: "appVersion.deploy", Created: time.Now()}
}
func TestAdmissionOwnedCapacityAndTerminal(t *testing.T) {
	b, f := admissionBackend(t)
	f.processes = []Process{admissionProcess("p1", "a", "RUNNING"), admissionProcess("p2", "b", "RUNNING")}
	_, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "c", Name: "memberc"})
	if err == nil || !strings.Contains(err.Error(), "2/2 in flight") || !strings.Contains(err.Error(), "membera:RUNNING") || !strings.Contains(err.Error(), "memberb:RUNNING") {
		t.Fatalf("missing capacity diagnostic: %v", err)
	}
	if time.Until(infra.RetryAt(err)) < 14*time.Second {
		t.Fatalf("missing backpressure: %v", err)
	}
	f.processes[0].Status = "FINISHED"
	b.deploymentAdmission().observedAt = time.Now().Add(-11 * time.Second)
	release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "c", Name: "memberc"})
	if err != nil {
		t.Fatal(err)
	}
	release()
	if f.mutations != 0 {
		t.Fatal("admission mutated platform")
	}
}
func TestAdmissionConcurrentLastSlotSharedBackends(t *testing.T) {
	b, f := admissionBackend(t)
	f.processes = []Process{admissionProcess("p1", "a", "RUNNING")}
	other, err := New(Options{BaseURL: b.client.base, Token: "token", ProjectID: "project", MaxRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for index, backend := range []*Backend{b, other} {
		wg.Add(1)
		go func(b *Backend, index int) {
			defer wg.Done()
			if _, err := b.admitDeployment(t.Context(), &ServiceStack{ID: string(rune('c' + index)), Name: "member"}); err == nil {
				admitted.Add(1)
			}
		}(backend, index)
	}
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("admitted %d", admitted.Load())
	}
}
func TestAdmissionForeignProtectedIgnored(t *testing.T) {
	b, f := admissionBackend(t)
	f.processes = []Process{admissionProcess("p1", "foreign", "RUNNING"), admissionProcess("p2", "core", "PENDING")}
	release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err != nil {
		t.Fatal(err)
	}
	release()
}
func TestAdmissionReservationReleaseDedupeTTL(t *testing.T) {
	for _, mode := range []string{"release", "active", "terminal", "ttl"} {
		t.Run(mode, func(t *testing.T) {
			b, f := admissionBackend(t)
			b.maxInflightDeploys = 1
			release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"}); err == nil {
				t.Fatal("reservation not counted")
			}
			switch mode {
			case "release":
				release()
			case "active":
				f.processes = []Process{admissionProcess("p1", "a", "RUNNING")}
			case "terminal":
				b.finishDeploymentSubmission("a")
				f.processes = []Process{admissionProcess("p1", "a", "FINISHED")}
			case "ttl":
				state := b.deploymentAdmission()
				b.finishDeploymentSubmission("a")
				for reservation := range state.reservations {
					reservation.finishedAt = time.Now().Add(-deploymentReservationTTL - time.Second)
				}
			}
			b.deploymentAdmission().observedAt = time.Now().Add(-11 * time.Second)
			release2, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"})
			if mode == "active" {
				if err == nil || !strings.Contains(err.Error(), "1/1 in flight") || strings.Contains(err.Error(), "SUBMITTING") {
					t.Fatalf("reservation not deduped: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				release2()
			}
		})
	}
}
func TestAdmissionListRetryAfter(t *testing.T) {
	b, f := admissionBackend(t)
	f.listCode = 429
	_, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err == nil || time.Until(infra.RetryAt(err)) < 119*time.Second {
		t.Fatalf("retry-after lost: %v at %v", err, infra.RetryAt(err))
	}
}
func TestAdmissionConfig(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int
		bad  bool
	}{{"", 2, false}, {"3", 3, false}, {"0", 0, true}, {"-1", 0, true}, {"many", 0, true}} {
		t.Run(tc.raw, func(t *testing.T) {
			n, err := deploymentLimit(0, tc.raw)
			if (err != nil) != tc.bad || n != tc.want {
				t.Fatalf("limit=%d err=%v", n, err)
			}
			_, err = NewFactory(Options{}).TargetFromEnv(func(key string) string {
				if key == "KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS" {
					return tc.raw
				}
				return ""
			})
			if (err != nil) != tc.bad {
				t.Fatalf("factory config: %v", err)
			}
		})
	}
	t.Setenv("KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS", "invalid")
	if _, err := New(Options{Token: "token", ProjectID: "project"}); err == nil || !strings.Contains(err.Error(), "KARAKORAM_ZEROPS_MAX_INFLIGHT_DEPLOYS") {
		t.Fatalf("startup accepted invalid cap: %v", err)
	}
}
func TestPreviousDeploymentEvidenceGate(t *testing.T) {
	for _, mode := range []string{"running", "pending", "building", "unknown-process", "unknown-version", "unreadable", "terminal", "selected-upload"} {
		t.Run(mode, func(t *testing.T) {
			b, f := admissionBackend(t)
			selected := ""
			wantWait := true
			switch mode {
			case "running", "pending":
				f.processes = []Process{admissionProcess("p1", "a", strings.ToUpper(mode))}
			case "unknown-process":
				f.processes = []Process{admissionProcess("p1", "a", "UNKNOWN")}
			case "building":
				f.versions = []AppVersion{{ID: "v1", Status: "BUILDING"}}
			case "unknown-version":
				f.versions = []AppVersion{{ID: "v1", Status: "UNKNOWN"}}
			case "unreadable":
				f.evidenceCode = http.StatusForbidden
			case "terminal":
				f.versions = []AppVersion{{ID: "v1", Status: "BACKUP"}}
				wantWait = false
			case "selected-upload":
				f.versions = []AppVersion{{ID: "v1", Status: "UPLOADING"}}
				selected = "v1"
				wantWait = false
			}
			err := b.waitPreviousDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"}, selected)
			if (err != nil) != wantWait {
				t.Fatalf("gate err=%v", err)
			}
			if mode == "unreadable" {
				var wait *infra.DeploymentWaitError
				if !errors.As(err, &wait) || wait.Reason != "DeploymentEvidenceUnavailable" || !strings.Contains(wait.Detail, "403") {
					t.Fatalf("missing specific evidence failure: %v", err)
				}
				return
			}
			if err != nil && !strings.Contains(err.Error(), "waiting for previous platform deployment to finish") {
				t.Fatal(err)
			}
		})
	}
}

func TestPreviousRunningDeployBlocksAllocationAndUpload(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, plain)
	var mutations atomic.Int32
	b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && r.URL.Path == "/service-stack/"+id+"/process" {
			body, _ := json.Marshal(page[Process]{List: []Process{admissionProcess("pending-deploy", id, "RUNNING")}, Total: 1})
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/app-version") || r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/upload") {
			mutations.Add(1)
		}
		return http.DefaultTransport.RoundTrip(r)
	}))
	_, err := b.Deploy(t.Context(), "member", hardeningDeployment())
	if err == nil || !strings.Contains(err.Error(), "waiting for previous platform deployment to finish: process pending-deploy RUNNING") || mutations.Load() != 0 {
		t.Fatalf("running deploy gate: mutations=%d err=%v", mutations.Load(), err)
	}
}

func TestAdmissionResumesExistingServiceReservation(t *testing.T) {
	b, _ := admissionBackend(t)
	b.maxInflightDeploys = 1
	first, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err != nil {
		t.Fatal(err)
	}
	defer first()
	release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err != nil {
		t.Fatalf("same submission cannot recover at limit one: %v", err)
	}
	release()
	if len(b.deploymentAdmission().reservations) != 0 {
		t.Fatal("resuming submission leaked a reservation")
	}
}

func TestAdmissionMissingProcessTimestampDedupe(t *testing.T) {
	b, f := admissionBackend(t)
	b.maxInflightDeploys = 1
	if _, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"}); err != nil {
		t.Fatal(err)
	}
	p := admissionProcess("new-process", "a", "FINISHED")
	p.Created = time.Time{}
	f.processes = []Process{p}
	b.finishDeploymentSubmission("a")
	b.deploymentAdmission().observedAt = time.Now().Add(-11 * time.Second)
	release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"})
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestAdmissionSharedObservationCache(t *testing.T) {
	b, f := admissionBackend(t)
	release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	reads := f.processReads
	release2, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"})
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	if reads != 2 || f.processReads != reads {
		t.Fatalf("shared observation multiplied reads: before=%d after=%d", reads, f.processReads)
	}
}

func TestAdmissionLongRunningPagedEvidence(t *testing.T) {
	for _, total := range []int{101, 501, 807} {
		for _, status := range []string{"FINISHED", "RUNNING", "PENDING", "UNKNOWN"} {
			t.Run(fmt.Sprintf("%d/%s", total, status), func(t *testing.T) {
				b, f := admissionBackend(t)
				b.maxInflightDeploys = 1
				for i := range total {
					p := admissionProcess(fmt.Sprintf("p%d", i), "a", "FINISHED")
					p.Created = time.Now().Add(-2*time.Hour - time.Duration(i)*time.Second)
					f.processes = append(f.processes, p)
				}
				f.processes[total-1].Status = status
				release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"})
				if status == "FINISHED" {
					if err != nil {
						t.Fatal(err)
					}
					release()
				} else if err == nil || !strings.Contains(err.Error(), "membera:"+status) {
					t.Fatalf("old unfinished process not observed: %v", err)
				}
				reads := f.processReads
				b.deploymentAdmission().observedAt = time.Time{}
				release, err = b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"})
				if release != nil {
					release()
				}
				wantReads := 2 // one newest page per owned service
				if status != "FINISHED" {
					wantReads++
				}
				if f.processReads-reads != wantReads {
					t.Fatalf("warm inventory rescanned history: reads=%d want=%d", f.processReads-reads, wantReads)
				}
				if (err == nil) != (status == "FINISHED") {
					t.Fatalf("warm inventory changed admission: %v", err)
				}
			})
		}
	}
}

func TestDeployLiveReservationSurvivesVisibilityExpiry(t *testing.T) {
	server := newFakeAPI(t)
	plain := hardeningBackend(t, server, nil)
	id := importHardeningRuntime(t, plain)
	entered, resume := make(chan struct{}), make(chan struct{})
	b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/upload") {
			close(entered)
			<-resume
		}
		return http.DefaultTransport.RoundTrip(r)
	}))
	b.maxInflightDeploys = 1
	done := make(chan error, 1)
	go func() { _, err := b.Deploy(t.Context(), "member", hardeningDeployment()); done <- err }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("deploy did not reach upload: %v", err)
	}
	state := b.deploymentAdmission()
	_ = state.lock.Lock(t.Context())
	for reservation := range state.reservations {
		if reservation.service == id {
			reservation.at = time.Now().Add(-deploymentReservationTTL - time.Minute)
		}
	}
	state.lock.Unlock()
	_, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "competitor", Name: "competitor"})
	close(resume)
	deployErr := <-done
	if err == nil {
		t.Error("live submitter lost its slot across visibility expiry")
	}
	if deployErr != nil {
		t.Fatal(deployErr)
	}
}

func TestDeployOtherServiceUnknownProcessFailsClosed(t *testing.T) {
	for _, tc := range []struct{ status, action string }{
		{"UNKNOWN", "appVersion.deploy"}, {"", "appVersion.build"}, {"RUNNING", ""}, {"UNKNOWN", ""}, {"RUNNING", "unrecognized"},
	} {
		t.Run(tc.status+"/"+tc.action, func(t *testing.T) {
			server := newFakeAPI(t)
			plain := hardeningBackend(t, server, nil)
			id := importHardeningRuntime(t, plain)
			var mutations atomic.Int32
			b := hardeningBackend(t, server, receiptTransport(func(r *http.Request) (*http.Response, error) {
				var body any
				switch {
				case r.Method != http.MethodGet:
					mutations.Add(1)
				case r.URL.Path == "/project/project/service-stack":
					body = page[ServiceStack]{List: []ServiceStack{{ID: id, Name: "member"}, {ID: "other", Name: "other"}}, Total: 2}
				case r.URL.Path == "/service-stack/other/env":
					body = map[string]any{"items": []EnvItem{{Key: "KARAKORAM_OWNER", Type: "USER", Content: "owner"}}}
				case r.URL.Path == "/service-stack/other/process":
					body = page[Process]{List: []Process{{ID: "blocking", ServiceStackID: "other", Status: tc.status, ActionName: tc.action}}, Total: 1}
				}
				if body != nil {
					data, _ := json.Marshal(body)
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, nil
				}
				return http.DefaultTransport.RoundTrip(r)
			}))
			b.maxInflightDeploys = 1
			_, err := b.Deploy(t.Context(), "member", hardeningDeployment())
			if err == nil || mutations.Load() != 0 {
				t.Fatalf("unknown other-service work admitted deployment: mutations=%d err=%v", mutations.Load(), err)
			}
		})
	}
}

func TestAdmissionIncompleteProcessEvidence(t *testing.T) {
	b, _ := admissionBackend(t)
	b.client.http = &http.Client{Transport: receiptTransport(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/process") {
			data, _ := json.Marshal(page[Process]{Total: 807})
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, nil
		}
		return http.DefaultTransport.RoundTrip(r)
	})}
	_, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "b"})
	var wait *infra.DeploymentWaitError
	if !errors.As(err, &wait) || wait.Reason != "DeploymentEvidenceUnavailable" || !strings.Contains(wait.Detail, "incomplete service process inventory") {
		t.Fatalf("missing specific fail-closed evidence diagnostic: %v", err)
	}
}

func TestDeploymentWaitIdentityStableAcrossAgeAndOrder(t *testing.T) {
	b, f := admissionBackend(t)
	f.processes = []Process{admissionProcess("p2", "b", "RUNNING"), admissionProcess("p1", "a", "PENDING")}
	_, first := b.admitDeployment(t.Context(), &ServiceStack{ID: "c"})
	var before *infra.DeploymentWaitError
	if !errors.As(first, &before) || before.Reason != "DeploymentSlot" || before.Key != "a/p1:PENDING,b/p2:RUNNING" {
		t.Fatalf("missing typed stable identity: %v", first)
	}
	for i := range f.processes {
		f.processes[i].Created = f.processes[i].Created.Add(-time.Minute)
	}
	f.processes[0], f.processes[1] = f.processes[1], f.processes[0]
	b.deploymentAdmission().observedAt = time.Now().Add(-11 * time.Second)
	_, second := b.admitDeployment(t.Context(), &ServiceStack{ID: "c"})
	var after *infra.DeploymentWaitError
	if !errors.As(second, &after) || before.Key != after.Key || before.Detail == after.Detail || infra.RetryAt(second) != after.RetryAt {
		t.Fatalf("age/order changed identity or lost detail/deadline: before=%+v after=%+v", before, after)
	}
	f.processes[0].Status = "RUNNING"
	b.deploymentAdmission().observedAt = time.Now().Add(-11 * time.Second)
	_, third := b.admitDeployment(t.Context(), &ServiceStack{ID: "c"})
	var changed *infra.DeploymentWaitError
	if !errors.As(third, &changed) || changed.Key == before.Key {
		t.Fatalf("state change did not change identity: %v", third)
	}
}

func TestPreviousDeploymentReusesBoundedObservation(t *testing.T) {
	b, f := admissionBackend(t)
	release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	reads := f.processReads
	if err := b.waitPreviousDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"}, ""); err != nil {
		t.Fatal(err)
	}
	if f.processReads != reads {
		t.Fatalf("fresh bounded observation scanned again: %d -> %d", reads, f.processReads)
	}
}

func TestPreviousDeploymentWaitSortedIdentity(t *testing.T) {
	b, f := admissionBackend(t)
	f.processes = []Process{admissionProcess("p2", "a", "RUNNING"), admissionProcess("p1", "a", "UNKNOWN")}
	err := b.waitPreviousDeployment(t.Context(), &ServiceStack{ID: "a"}, "")
	var wait *infra.DeploymentWaitError
	if !errors.As(err, &wait) || wait.Reason != "PreviousDeployment" || wait.Key != "a/p1:UNKNOWN,a/p2:RUNNING" {
		t.Fatalf("missing previous-deployment identity: %v", err)
	}
}

func TestReservationVisibilityTTLStartsOnReturn(t *testing.T) {
	b, _ := admissionBackend(t)
	b.maxInflightDeploys = 1
	_, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err != nil {
		t.Fatal(err)
	}
	for pending := range b.deploymentAdmission().reservations {
		pending.at = time.Now().Add(-2 * deploymentReservationTTL)
	}
	b.finishDeploymentSubmission("a")
	_, err = b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"})
	if err == nil {
		t.Fatal("visibility window expired relative to admission rather than return")
	}
}

func TestRefusedReservationRecoveryPreservesVisibilityExpiry(t *testing.T) {
	b, f := admissionBackend(t)
	b.maxInflightDeploys = 1
	_, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"})
	if err != nil {
		t.Fatal(err)
	}
	b.finishDeploymentSubmission("a")
	state := b.deploymentAdmission()
	var pending *deploymentReservation
	for reservation := range state.reservations {
		pending = reservation
	}
	finishedAt := pending.finishedAt
	f.processes = []Process{admissionProcess("competing", "b", "RUNNING")}
	state.observedAt = time.Now().Add(-11 * time.Second)
	if _, err = b.admitDeployment(t.Context(), &ServiceStack{ID: "a", Name: "membera"}); err == nil {
		t.Fatal("recovery admitted while competing work filled capacity")
	}
	if pending.live || !pending.finishedAt.Equal(finishedAt) {
		t.Errorf("refused recovery changed lifecycle: live=%v finishedAt=%v want=%v", pending.live, pending.finishedAt, finishedAt)
	}
	f.processes = nil
	pending.finishedAt = time.Now().Add(-deploymentReservationTTL - time.Second)
	state.observedAt = time.Now().Add(-11 * time.Second)
	release, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "b", Name: "memberb"})
	if err != nil {
		t.Fatalf("refused recovery stranded reservation beyond expiry: %v", err)
	}
	release()
}

func TestAdmissionIncrementalInventoryContinuity(t *testing.T) {
	b, f := admissionBackend(t)
	state := b.deploymentAdmission()
	for i := range 807 {
		p := admissionProcess(fmt.Sprintf("old%d", i), "a", "FINISHED")
		p.Created = time.Now().Add(-time.Hour - time.Duration(i)*time.Second)
		p.ActionName = "stack.updateAutoscaling"
		f.processes = append(f.processes, p)
	}
	observe := func() []Process {
		t.Helper()
		processes, err := b.serviceProcesses(t.Context(), state, "a")
		if err != nil {
			t.Fatal(err)
		}
		return processes
	}
	if got := observe(); len(got) != 0 || f.processReads != 9 {
		t.Fatalf("cold inventory: relevant=%d reads=%d", len(got), f.processReads)
	}
	f.processes = append([]Process{admissionProcess("new", "a", "PENDING")}, f.processes...)
	if got := observe(); len(got) != 1 || got[0].Status != "PENDING" || f.processReads != 10 {
		t.Fatalf("new prefix: processes=%v reads=%d", got, f.processReads)
	}
	f.processes[0].Status = "FINISHED"
	if got := observe(); len(got) != 1 || got[0].Status != "FINISHED" || f.processReads != 11 {
		t.Fatalf("active refresh: processes=%v reads=%d", got, f.processReads)
	}
	if got := observe(); len(got) != 0 || f.processReads != 12 {
		t.Fatalf("terminal process refreshed again: processes=%v reads=%d", got, f.processReads)
	}
	// Retention shrank the inventory: cold-scan again, including old unfinished work.
	f.processes = f.processes[1:]
	f.processes[len(f.processes)-1].Status = "UNKNOWN"
	f.processes[len(f.processes)-1].ActionName = "appVersion.deploy"
	if got := observe(); len(got) != 1 || got[0].Status != "UNKNOWN" || f.processReads != 21 {
		t.Fatalf("shrunken history skipped: processes=%v reads=%d", got, f.processReads)
	}
	// A new ID without a total increase cannot prove continuity.
	f.processes[0] = admissionProcess("replacement", "a", "RUNNING")
	if got := observe(); len(got) != 2 || f.processReads != 30 {
		t.Fatalf("ID gap skipped: processes=%v reads=%d", got, f.processReads)
	}
}

func TestAdmissionEqualTimestampRetentionDoesNotHideNewWork(t *testing.T) {
	b, f := admissionBackend(t)
	at := time.Now().Add(-time.Hour)
	for i, id := range []string{"A", "B", "C"} {
		p := admissionProcess(id, "a", "FINISHED")
		p.Created = at.Add(-time.Duration(i) * time.Minute)
		f.processes = append(f.processes, p)
	}
	state := b.deploymentAdmission()
	if _, err := b.serviceProcesses(t.Context(), state, "a"); err != nil {
		t.Fatal(err)
	}
	x := admissionProcess("X", "a", "RUNNING")
	x.Created = at
	f.processes = []Process{f.processes[0], x, f.processes[1]}
	got, err := b.serviceProcesses(t.Context(), state, "a")
	if err != nil || len(got) != 1 || got[0].ID != "X" {
		t.Fatalf("equal-created insertion hidden by retention: processes=%v err=%v", got, err)
	}
	if _, retained := state.inventories["a"].seen["C"]; retained {
		t.Fatal("retention mismatch did not rebuild index")
	}
}

func TestAdmissionInventoryEvictedOnlyAfterSuccessfulObservation(t *testing.T) {
	b, f := admissionBackend(t)
	state := b.deploymentAdmission()
	stale := &serviceProcessInventory{seen: map[string]struct{}{"old": {}}}
	state.inventories = map[string]*serviceProcessInventory{"deleted": stale}
	f.evidenceCode = http.StatusForbidden
	if _, _, err := b.deploymentObservation(t.Context(), state); err == nil {
		t.Fatal("expected failed observation")
	}
	if state.inventories["deleted"] != stale {
		t.Fatal("failed observation evicted inventory")
	}
	f.evidenceCode = 0
	if _, _, err := b.deploymentObservation(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	if _, retained := state.inventories["deleted"]; retained {
		t.Fatal("successful complete observation retained deleted service")
	}
}

func TestAdmissionFilteredWorkSurvivesHistoryVisibilityGaps(t *testing.T) {
	for _, mode := range []string{"older-group", "balanced-page-shift"} {
		for _, status := range unfinishedProcessStatuses {
			t.Run(mode+"/"+status, func(t *testing.T) {
				b, f := admissionBackend(t)
				at := time.Now().Add(-time.Hour)
				for i := range 201 {
					p := admissionProcess(fmt.Sprintf("p%d", i), "a", "FINISHED")
					p.Created = at.Add(-time.Duration(i) * time.Minute)
					f.processes = append(f.processes, p)
				}
				state := b.deploymentAdmission()
				if _, err := b.serviceProcesses(t.Context(), state, "a"); err != nil {
					t.Fatal(err)
				}
				x := admissionProcess("late", "a", status)
				x.Created = f.processes[1].Created
				insert := func() {
					f.processes = append(f.processes[:2], append([]Process{x}, f.processes[2:len(f.processes)-1]...)...)
				}
				if mode == "older-group" {
					insert()
				} else {
					// Force a multi-page history rebuild, then balance an insertion
					// behind the cursor against deletion after the first page.
					state.inventories["a"].rebuiltAt = time.Now().Add(-11 * time.Minute)
					f.afterHistoryPage = func(f *admissionFixture, r *http.Request) {
						if r.URL.Query().Get("offset") == "0" {
							f.processes = append([]Process{x}, f.processes[1:]...)
							f.afterHistoryPage = nil
						}
					}
				}
				got, err := b.serviceProcesses(t.Context(), state, "a")
				if err != nil {
					t.Fatal(err)
				}
				found := false
				for _, p := range got {
					found = found || p.ID == "late" && p.Status == status
				}
				if !found || f.activeReads != 2 {
					t.Fatalf("filtered work missed: processes=%v filteredReads=%d", got, f.activeReads)
				}
			})
		}
	}
}

func TestAdmissionPeriodicRebuildFindsLateUnknownStatus(t *testing.T) {
	b, f := admissionBackend(t)
	at := time.Now().Add(-time.Hour)
	for i := range 3 {
		p := admissionProcess(fmt.Sprintf("p%d", i), "a", "FINISHED")
		p.Created = at.Add(-time.Duration(i) * time.Minute)
		f.processes = append(f.processes, p)
	}
	state := b.deploymentAdmission()
	if _, err := b.serviceProcesses(t.Context(), state, "a"); err != nil {
		t.Fatal(err)
	}
	p := admissionProcess("late-unknown", "a", "FUTURE_STATUS")
	p.Created = f.processes[1].Created
	f.processes = []Process{f.processes[0], f.processes[1], p}
	state.inventories["a"].rebuiltAt = time.Now().Add(-11 * time.Minute)
	got, err := b.serviceProcesses(t.Context(), state, "a")
	if err != nil || len(got) != 3 || got[2].ID != p.ID {
		t.Fatalf("periodic rebuild missed unknown status: processes=%v err=%v", got, err)
	}
}

func TestAdmissionFilteredEvidenceFailsClosed(t *testing.T) {
	for _, mode := range []string{"total-change", "duplicate", "filtered-only-failure"} {
		t.Run(mode, func(t *testing.T) {
			b, f := admissionBackend(t)
			for i := range 101 {
				p := admissionProcess(fmt.Sprintf("p%d", i), "a", processRunning)
				p.ActionName = "stack.updateAutoscaling"
				p.Created = time.Now().Add(-time.Duration(i) * time.Minute)
				f.processes = append(f.processes, p)
			}
			switch mode {
			case "total-change":
				f.afterFilteredPage = func(f *admissionFixture, r *http.Request) {
					if r.URL.Query().Get("offset") == "0" {
						// Unrelated first-page work finishes; a shrinking
						// second page must not certify a complete inventory.
						f.processes[0].Status = "FINISHED"
					}
				}
			case "duplicate":
				f.afterFilteredPage = func(f *admissionFixture, r *http.Request) {
					if r.URL.Query().Get("offset") == "0" {
						f.processes[100] = f.processes[0]
					}
				}
			case "filtered-only-failure":
				f.filterCode = http.StatusForbidden
			}
			_, err := b.admitDeployment(t.Context(), &ServiceStack{ID: "b"})
			var wait *infra.DeploymentWaitError
			if !errors.As(err, &wait) || wait.Reason != "DeploymentEvidenceUnavailable" {
				t.Fatalf("incomplete filtered evidence admitted: %v", err)
			}
			detail := map[string]string{
				"total-change":          "unfinished process inventory changed",
				"duplicate":             "duplicate unfinished process identity",
				"filtered-only-failure": "403",
			}[mode]
			if !strings.Contains(wait.Detail, detail) || f.processReads < 2 || f.activeReads < 1 || f.mutations != 0 {
				t.Fatalf("filtered evidence diagnosis lost: detail=%q history=%d filtered=%d mutations=%d", wait.Detail, f.processReads, f.activeReads, f.mutations)
			}
		})
	}
}
