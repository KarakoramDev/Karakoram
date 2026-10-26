// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestImportReconcilesCommittedLostResponses(t *testing.T) {
	for _, phase := range []string{"reservation", "volume", "environment", "completion"} {
		for _, obscureReadback := range []bool{false, true} {
			t.Run(phase+map[bool]string{false: "/readable", true: "/unknown"}[obscureReadback], func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				volume := volumeSpec("volume")
				volume.Env = map[string]string{"PRIVATE": "never-print-import-secret"}
				specs := []infra.ServiceSpec{volume, runtimeSpec("runtime", "volume")}
				transport := b.c.http.Transport
				injected, hidden := false, false
				recordWrites := 0
				b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
					if obscureReadback && injected && !hidden && req.Method == http.MethodGet {
						hidden = true
						return nil, io.ErrUnexpectedEOF
					}
					if req.Method == http.MethodPut && req.URL.Path == "/v1/var/"+recordPrefix+"volume" {
						recordWrites++
					}
					match := phase == "reservation" && recordWrites == 1 && req.Method == http.MethodPut && req.URL.Path == "/v1/var/"+recordPrefix+"volume" ||
						phase == "volume" && req.Method == http.MethodPut && req.URL.Path == "/v1/volume/host/create" ||
						phase == "environment" && req.Method == http.MethodPut && req.URL.Path == "/v1/var/nomad/jobs/volume" ||
						phase == "completion" && recordWrites == 3 && req.Method == http.MethodPut && req.URL.Path == "/v1/var/"+recordPrefix+"volume"
					response, err := transport.RoundTrip(req)
					if err == nil && match && !injected {
						if response.StatusCode < 200 || response.StatusCode >= 300 {
							t.Errorf("fault did not follow an acknowledged commit: %d", response.StatusCode)
						}
						injected = true
						_ = response.Body.Close()
						return nil, io.ErrUnexpectedEOF
					}
					return response, err
				})
				op, err := b.ImportServices(t.Context(), specs)
				if !injected || obscureReadback && (err == nil || !hidden) || !obscureReadback && err != nil {
					t.Fatalf("lost-response path not exercised: injected=%v hidden=%v err=%v", injected, hidden, err)
				}
				ids := op.Services
				if ids["volume"] == "" {
					t.Fatal("reserved creation identity was lost with the reply")
				}
				if err != nil {
					// No volatile backend state survives. Retry the original batch.
					b = fakeBackend(t, f, nil)
					op, err = b.ImportServices(t.Context(), specs)
					if err != nil || op.Services["volume"] != ids["volume"] {
						t.Fatalf("retry did not retain reserved identity: %+v %v", op.Services, err)
					}
				}
				awaitFake(t, b, op)
				for _, spec := range specs {
					r, err := b.require(t.Context(), spec.Hostname)
					if err != nil || r.Importing || r.ID != op.Services[spec.Hostname] {
						t.Fatalf("import did not finish with original identity: %+v %v", r, err)
					}
				}
				f.mu.Lock()
				if len(f.volumes) != 1 {
					t.Errorf("ambiguous import created %d physical volumes", len(f.volumes))
				}
				f.mu.Unlock()
				for _, req := range f.requests() {
					if req.Method == http.MethodDelete {
						t.Fatalf("lost response triggered destructive compensation: %+v", req)
					}
				}
			})
		}
	}
}

func TestImportReservesIdentityBeforePhysicalCreation(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	transport := b.c.http.Transport
	checked := false
	b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/v1/volume/host/create" {
			f.mu.Lock()
			reservation := f.variables[fakeKey("default", recordPrefix+"volume")]
			if reservation == nil {
				t.Error("volume create preceded durable identity reservation")
			} else {
				var r record
				_ = json.Unmarshal([]byte(fakeString(fakeMap(reservation["Items"]), "record")), &r)
				if r.ID == "" || r.Node == "" || !strings.HasSuffix(r.VolumeName, r.ID) || !r.Importing {
					t.Errorf("incomplete creation provenance: %+v", r)
				}
			}
			f.mu.Unlock()
			checked = true
		}
		return transport.RoundTrip(req)
	})
	importFake(t, b, volumeSpec("volume"))
	if !checked {
		t.Fatal("physical creation was not exercised")
	}
}

func TestVariableRevokedTokenKeepsUnauthorizedClassification(t *testing.T) {
	for _, path := range []string{"/v1/jobs", "/v1/vars", "/v1/var/karakoram/services/runtime"} {
		for _, body := range []string{"ACL token not found", `{"Items":{"PRIVATE":"ACL token not found"}}`, "invalid variable request", "ACL token not found" + strings.Repeat(" ", 80) + "not-auth"} {
			t.Run(path+"/"+body, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, body)
				}))
				t.Cleanup(server.Close)
				c := newClient(server.URL, "", "default", "", nil)
				_, err := c.requestJSON(t.Context(), http.MethodGet, path, nil, nil, nil, 0)
				wantAuth := body == "ACL token not found" || path == "/v1/jobs" && strings.Contains(body, "ACL token not found")
				if errors.Is(err, infra.ErrUnauthorized) != wantAuth || errors.Is(err, infra.ErrRejected) == wantAuth {
					t.Fatalf("credential classification=%v, want unauthorized=%v", err, wantAuth)
				}
				if path != "/v1/jobs" && strings.Contains(err.Error(), "Items") {
					t.Fatal("Variable failure exposed secret-bearing body")
				}
			})
		}
	}
}

func TestNomadRetryAfterPreservesBackendRetryDeadline(t *testing.T) {
	for _, path := range []string{"/v1/jobs", "/v1/vars"} {
		for _, status := range []int{429, 503} {
			for _, header := range []string{"3600", time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat)} {
				t.Run(path+"/"+http.StatusText(status)+"/"+header, func(t *testing.T) {
					calls := 0
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls++
						w.Header().Set("Retry-After", header)
						w.WriteHeader(status)
						_, _ = io.WriteString(w, "temporarily unavailable")
					}))
					t.Cleanup(server.Close)
					c := newClient(server.URL, "", "default", "", nil)
					_, err := c.requestJSON(t.Context(), http.MethodGet, path, nil, nil, nil, 0)
					var retry *infra.RetryAfterError
					if !errors.As(err, &retry) || !errors.Is(err, infra.ErrUnavailable) || retry.At.Before(time.Now().Add(59*time.Minute)) || calls != 1 {
						t.Fatalf("retry constraint was capped/lost/replayed: %v calls=%d", err, calls)
					}
				})
			}
		}
	}
}

func TestVolumeObservationReportsActualStorageAndNodeState(t *testing.T) {
	for _, fault := range []string{"missing-volume", "changed-volume", "pending-volume", "missing-node", "node-down", "node-drain", "node-ineligible"} {
		t.Run(fault, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			deployFake(t, b, "runtime", "volume")
			v, err := b.require(t.Context(), "volume")
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			node := f.nodes[v.Node]
			switch fault {
			case "missing-volume":
				delete(f.volumes, v.VolumeID)
			case "changed-volume":
				f.volumes[v.VolumeID]["Name"] = "foreign-volume"
			case "pending-volume":
				f.volumes[v.VolumeID]["State"] = "unavailable"
			case "missing-node":
				delete(f.nodes, v.Node)
			case "node-down":
				node.Status = "down"
				f.nodes[v.Node] = node
			case "node-drain":
				node.Drain = true
				f.nodes[v.Node] = node
			case "node-ineligible":
				node.SchedulingEligibility = "ineligible"
				f.nodes[v.Node] = node
			}
			f.mu.Unlock()
			service, err := b.GetService(t.Context(), "volume")
			if err == nil || service == nil || service.Status == infra.StatusActive || !strings.Contains(infra.SafeDetail(err), "original") && !strings.Contains(infra.SafeDetail(err), "identity") {
				t.Fatalf("unusable original storage was advertised healthy: %+v %v", service, err)
			}
			var lost *infra.VolumeLostError
			if errors.As(err, &lost) != (fault == "missing-volume") {
				t.Fatalf("physical loss was confused with unavailable/foreign storage: %v", err)
			}
			if lost != nil && (lost.Volume != v.Hostname || lost.ServiceID != v.ID) {
				t.Fatalf("physical loss omitted authoritative logical identity: %+v", lost)
			}
			if fault == "missing-volume" || fault == "changed-volume" || fault == "pending-volume" {
				before := len(f.requests())
				if _, err := b.Start(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
					t.Fatalf("unsafe runtime start was not fenced: %v", err)
				}
				for _, req := range f.requests()[before:] {
					if req.Method != http.MethodGet {
						t.Fatalf("unusable volume triggered a mutation: %+v", req)
					}
				}
			}
		})
	}
}

func TestObservationResumesOperationAfterBeginReplyIsLost(t *testing.T) {
	for _, action := range []string{"deploy", "resources", "scale", "start", "stop", "delete"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			transport := b.c.http.Transport
			injected := false
			b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(req)
				if !injected && err == nil && req.Method == http.MethodPut && req.URL.Path == "/v1/var/"+recordPrefix+"runtime" {
					body, bodyErr := req.GetBody()
					if bodyErr != nil {
						return response, bodyErr
					}
					var payload variable
					decodeErr := json.NewDecoder(body).Decode(&payload)
					_ = body.Close()
					var saved record
					if decodeErr != nil || json.Unmarshal([]byte(payload.Items["record"]), &saved) != nil || saved.Pending == nil || saved.Pending.Phase == "done" {
						return response, err
					}
					injected = true
					_ = response.Body.Close()
					return nil, io.ErrUnexpectedEOF
				}
				return response, err
			})
			var err error
			switch action {
			case "deploy":
				_, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			case "resources":
				_, err = b.SetResources(t.Context(), "runtime", infra.Resources{MinRAM: 1, MaxRAM: 2})
			case "scale":
				_, err = b.SetContainerCount(t.Context(), "runtime", 2, 2)
			case "start":
				_, err = b.Start(t.Context(), "runtime")
			case "stop":
				_, err = b.Stop(t.Context(), "runtime")
			case "delete":
				_, err = b.DeleteService(t.Context(), "runtime")
			}
			if err == nil || !injected {
				t.Fatalf("begin's durable receipt was not lost: %v", err)
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil || r.Pending == nil || r.Pending.Lease == nil {
				t.Fatalf("begin lost its durable recovery claim: %+v %v", r, err)
			}
			b = fakeBackend(t, f, nil)
			b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
			complete := false
			for range 30 {
				// No exported op handle and no reissued destructive action.
				if _, err := b.GetService(t.Context(), "runtime"); err != nil {
					t.Fatal(err)
				}
				current, err := b.load(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				if action == "delete" && current == nil || current != nil && current.Pending.Phase == "done" {
					complete = true
					break
				}
			}
			if !complete {
				t.Fatal("ordinary observation stranded the persisted operation")
			}
		})
	}
}

func TestObservationResumesCommittedStopAfterLostReply(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	transport := b.c.http.Transport
	injected := false
	b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(req)
		if !injected && err == nil && req.Method == http.MethodPost && req.URL.Path == "/v1/jobs" {
			injected = true
			_ = response.Body.Close()
			return nil, io.ErrUnexpectedEOF
		}
		return response, err
	})
	if _, err := b.Stop(t.Context(), "runtime"); err == nil || !injected {
		t.Fatalf("committed stop response was not lost: %v", err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil || r.Pending.Lease == nil {
		t.Fatalf("unknown stop lost its durable lease: %+v %v", r, err)
	}
	b = fakeBackend(t, f, nil)
	b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
	for range 10 {
		if _, err := b.ListServices(t.Context(), ""); err != nil {
			t.Fatal(err)
		}
		current, err := b.require(t.Context(), "runtime")
		if err != nil {
			t.Fatal(err)
		}
		if current.Pending.Phase == "done" {
			return
		}
	}
	t.Fatal("list observations stranded the committed stop")
}

// This deliberately documents the bounded-delivery contract, not a guarantee
// against arbitrarily queued server requests. An absent-only CAS cannot reject
// a request whose hostname becomes absent after the delivery fence expires.
func TestRegistrationBeyondDeliveryFenceDocumentsResidualRisk(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	ageRegistration(t, b)
	purgeRuntime(t, b)
	transport := b.c.http.Transport
	var delayed *http.Request
	b.c.http.Transport = testRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodPost && req.URL.Path == "/v1/jobs" {
			delayed = req.Clone(context.Background())
			delayed.Body, _ = req.GetBody()
			return nil, context.DeadlineExceeded
		}
		return transport.RoundTrip(req)
	})
	if _, err := b.GetService(t.Context(), "runtime"); err == nil || delayed == nil {
		t.Fatal("registration was not delayed before upstream delivery")
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	other := fakeBackend(t, f, nil)
	other.clock = func() time.Time { return r.RegistrationIssued.Add(restoreFence + time.Second) }
	op, err := other.DeleteService(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, other, op)
	if current, err := other.load(t.Context(), "runtime"); err != nil || current != nil {
		t.Fatalf("delete did not complete before late delivery: %+v %v", current, err)
	}
	response, err := transport.RoundTrip(delayed)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("fixture did not demonstrate the absent-hostname admission gap: %d", response.StatusCode)
	}
	var job observedJob
	if _, err := other.c.get(t.Context(), "/v1/job/runtime", nil, &job); err != nil || job.Meta["karakoram.id"] != r.ID {
		t.Fatalf("late-delivery evidence did not retain the retired incarnation: %+v %v", job, err)
	}
}

func TestOwnedExternalJobStopIsRecoverableThroughStart(t *testing.T) {
	for _, volume := range []string{"", "volume"} {
		t.Run(map[bool]string{false: "stateless", true: "persistent"}[volume != ""], func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			if volume != "" {
				importFake(t, b, volumeSpec(volume))
			}
			importFake(t, b, runtimeSpec("runtime", volume))
			deployFake(t, b, "runtime", volume)
			if err := b.c.delete(t.Context(), "/v1/job/runtime", nil, nil); err != nil {
				t.Fatal(err)
			}
			b = fakeBackend(t, f, nil)
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service.Status != infra.StatusStopped {
				t.Fatalf("confirmed external stop was not recoverable: %+v %v", service, err)
			}
			op, err := b.Start(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			awaitFake(t, b, op)
			service, err = b.GetService(t.Context(), "runtime")
			if err != nil || service.Status != infra.StatusRunning {
				t.Fatalf("saved deployment did not recover external stop: %+v %v", service, err)
			}
		})
	}
}

func TestStoppedStatusDoesNotFightLifecycleOrInferUnknownState(t *testing.T) {
	for _, blocked := range []string{"pending", "restart-lease", "restoration-fence", "failed", "unknown-without-stop"} {
		t.Run(blocked, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			if err := b.c.delete(t.Context(), "/v1/job/runtime", nil, nil); err != nil {
				t.Fatal(err)
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			switch blocked {
			case "pending":
				r.Pending.Phase = "stopping"
				r.Pending.Lease = &operationLease{Owner: "other-instance", Until: time.Now().Add(operationLeaseDuration)}
			case "restart-lease":
				r.RestartLease = &operationLease{Owner: "other-instance", Until: time.Now().Add(operationLeaseDuration)}
			case "restoration-fence":
				r.RegistrationIssued = time.Now()
			case "failed":
				r.Pending.Phase = "failed"
			case "unknown-without-stop":
				f.mu.Lock()
				f.jobs[fakeKey("default", "runtime")].row["Stop"] = false
				f.mu.Unlock()
			}
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			before := len(f.requests())
			service, err := b.GetService(t.Context(), "runtime")
			if err != nil || service.Status == infra.StatusStopped || service.Status == infra.StatusRunning {
				t.Fatalf("observation confused lifecycle/unknown state with external stop: %+v %v", service, err)
			}
			for _, request := range f.requests()[before:] {
				if request.Method != http.MethodGet {
					t.Fatalf("observation fought held/failed work: %+v", request)
				}
			}
		})
	}
}
