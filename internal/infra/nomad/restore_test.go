// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func ageRegistration(t *testing.T, b *Backend) *record {
	t.Helper()
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	r.RegistrationIssued = time.Now().Add(-2 * restoreFence)
	if r.Pending != nil {
		r.Pending.Started = r.RegistrationIssued
	}
	if err = b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	return r
}

func purgeRuntime(t *testing.T, b *Backend) {
	t.Helper()
	if err := b.c.delete(t.Context(), "/v1/job/runtime", query("purge", "true"), nil); err != nil {
		t.Fatal(err)
	}
}

func jobRegistrations(f *fakeNomad) int {
	count := 0
	for _, request := range f.requests() {
		if request.Method == http.MethodPost && request.Path == "/v1/jobs" {
			count++
		}
	}
	return count
}

func TestObservationRestoresPurgedRunningJob(t *testing.T) {
	for _, list := range []bool{false, true} {
		t.Run(map[bool]string{false: "get", true: "list"}[list], func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.Vars = map[string]string{"KARAKORAM_OWNER": "owner"}
			spec.Env = map[string]string{"KARAKORAM_AGENT_TOKEN": "secret"}
			imported := importFake(t, b, spec)
			deployFake(t, b, "runtime", "")
			r := ageRegistration(t, b)
			expected, err := b.renderJob(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			purgeRuntime(t, b)
			before := jobRegistrations(f)
			b = fakeBackend(t, f, nil) // No in-memory identity/specification required.
			var service *infra.Service
			for range 20 {
				if list {
					services, e := b.ListServices(t.Context(), "runtime")
					err = e
					if len(services) == 1 {
						service = &services[0]
					}
				} else {
					service, err = b.GetService(t.Context(), "runtime")
				}
				if err != nil || service == nil || service.ID != imported.Services["runtime"] {
					t.Fatalf("restoration observation: %+v %v", service, err)
				}
				if service.Status == infra.StatusRunning {
					break
				}
			}
			if service.Status != infra.StatusRunning || jobRegistrations(f) != before+1 {
				t.Fatalf("job did not restore exactly once: %+v; registrations %d -> %d", service, before, jobRegistrations(f))
			}
			var actual map[string]any
			if _, err = b.c.get(t.Context(), "/v1/job/runtime", nil, &actual); err != nil {
				t.Fatal(err)
			}
			// Decode through JSON just as Nomad does before comparing the saved spec.
			expectedJob := fakeCopy(expected["Job"].(map[string]any))
			current, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			fakeMap(expectedJob["Meta"])["karakoram.restore"] = strconv.FormatUint(current.Index, 10)
			for key, value := range expectedJob {
				if !reflect.DeepEqual(actual[key], value) {
					t.Fatalf("restoration changed derived field %s", key)
				}
			}
			if service.Ownership["KARAKORAM_OWNER"] != "owner" || !service.Masked {
				t.Fatalf("restoration lost durable ownership: %+v", service)
			}
		})
	}
}

func TestObservationDoesNotRestorePurgedStoppedJob(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	op, err := b.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op) // Stop purges the derived job itself.
	before := jobRegistrations(f)
	for range 3 {
		s, err := b.GetService(t.Context(), "runtime")
		if err != nil || s.Status != infra.StatusStopped {
			t.Fatalf("stopped observation: %+v %v", s, err)
		}
	}
	if jobRegistrations(f) != before {
		t.Fatal("observation recreated a stopped job")
	}
}

func TestObservationRefusesForeignJob(t *testing.T) {
	for _, field := range []string{"karakoram.id", "karakoram.kind", "KARAKORAM_OWNER", "Namespace", "missing-meta"} {
		t.Run(field, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			spec := runtimeSpec("runtime", "")
			spec.Vars = map[string]string{"KARAKORAM_OWNER": "owner"}
			importFake(t, b, spec)
			deployFake(t, b, "runtime", "")
			f.mu.Lock()
			job := f.jobs[fakeKey("default", "runtime")]
			switch field {
			case "Namespace":
				job.row[field] = "foreign"
			case "missing-meta":
				delete(job.row, "Meta")
			default:
				fakeMap(job.row["Meta"])[field] = "foreign"
			}
			f.mu.Unlock()
			before := jobRegistrations(f)
			s, err := b.GetService(t.Context(), "runtime")
			if !errors.Is(err, infra.ErrConflict) || s.Status != infra.StatusFailed || !strings.Contains(err.Error(), "refusing adoption") {
				t.Fatalf("foreign job was trusted: %+v %v", s, err)
			}
			if jobRegistrations(f) != before {
				t.Fatal("observation overwrote foreign job")
			}
		})
	}
}

func TestObservationRestoresStatefulJobOnlyWithOriginalVolume(t *testing.T) {
	for _, change := range []string{"retained", "missing-volume", "missing-record", "moved-volume", "renamed-volume", "pending-volume"} {
		t.Run(change, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
			deployFake(t, b, "runtime", "volume")
			r := ageRegistration(t, b)
			v, err := b.require(t.Context(), "volume")
			if err != nil {
				t.Fatal(err)
			}
			purgeRuntime(t, b)
			f.mu.Lock()
			switch change {
			case "missing-volume":
				delete(f.volumes, v.VolumeID)
			case "missing-record":
				delete(f.variables, fakeKey("default", recordPrefix+"volume"))
			case "moved-volume":
				f.volumes[v.VolumeID]["NodeID"] = "other-node"
			case "renamed-volume":
				f.volumes[v.VolumeID]["Name"] = "foreign-volume"
			case "pending-volume":
				f.volumes[v.VolumeID]["State"] = "unavailable"
			}
			f.mu.Unlock()
			before := jobRegistrations(f)
			s, err := b.GetService(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			if change != "retained" {
				if s.Status != infra.StatusFailed || jobRegistrations(f) != before {
					t.Fatalf("missing/changed data was silently recreated: %+v", s)
				}
				return
			}
			if jobRegistrations(f) != before+1 || s.ID != r.ID {
				t.Fatalf("retained data member not restored: %+v", s)
			}
			containers, err := b.Containers(t.Context(), "runtime")
			if err != nil || len(containers) != 1 || containers[0].PlacementGroup != v.Node {
				t.Fatalf("restored data moved nodes: %+v %v", containers, err)
			}
			f.mu.Lock()
			group := fakeMap(f.jobs[fakeKey("default", "runtime")].row["TaskGroups"].([]any)[0])
			constraint := fakeMap(group["Constraints"].([]any)[0])
			source := fakeMap(fakeMap(group["Volumes"])["data"])["Source"]
			f.mu.Unlock()
			if constraint["RTarget"] != v.Node || source != v.VolumeName {
				t.Fatalf("restored job lost node/volume pin: %+v %v", constraint, source)
			}
			if overlaps := f.writerOverlaps(); len(overlaps) != 0 {
				t.Fatalf("restoration overlapped writers: %v", overlaps)
			}
		})
	}
}

func TestObservationDoesNotRacePendingRegistration(t *testing.T) {
	for _, phase := range []string{"stopping", "registering", "starting", "failed", "grace", "restart-lease"} {
		t.Run(phase, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			r := ageRegistration(t, b)
			if phase == "grace" {
				r.RegistrationIssued = time.Now().UTC()
			} else if phase == "restart-lease" {
				r.RestartLease = &operationLease{Owner: "another-instance", Until: time.Now().Add(operationLeaseDuration)}
			} else {
				r.Pending.Phase = phase
				// Observations now resume abandoned lifecycle work, but must
				// never race a poller holding its durable mutation claim.
				r.Pending.Lease = &operationLease{Owner: "another-instance", Until: time.Now().Add(operationLeaseDuration)}
			}
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			purgeRuntime(t, b)
			before := jobRegistrations(f)
			s, err := b.GetService(t.Context(), "runtime")
			if err != nil || s == nil || jobRegistrations(f) != before {
				t.Fatalf("observation raced %s: %+v %v", phase, s, err)
			}
		})
	}
}

func TestConcurrentObservationsShareRegistrationGrace(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	ageRegistration(t, b)
	purgeRuntime(t, b)
	other := fakeBackend(t, f, nil)
	entered, release := make(chan struct{}), make(chan struct{})
	transport := b.c.http.Transport
	b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" {
			close(entered)
			<-release
		}
		return transport.RoundTrip(request)
	})
	before := jobRegistrations(f)
	result := make(chan error, 1)
	go func() {
		_, err := b.GetService(t.Context(), "runtime")
		result <- err
	}()
	<-entered
	s, err := other.GetService(t.Context(), "runtime")
	close(release)
	firstErr := <-result
	if err != nil || firstErr != nil || s.Status != infra.StatusCreating || jobRegistrations(f) != before+1 {
		t.Fatalf("concurrent observers raced registration: %+v %v %v; registrations %d -> %d", s, err, firstErr, before, jobRegistrations(f))
	}
}

func TestObservationRestoresRecordWithoutPendingOperation(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	r := ageRegistration(t, b)
	r.Pending = nil
	r.RegistrationIssued = time.Time{}
	if err := b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	purgeRuntime(t, b)
	before := jobRegistrations(f)
	s, err := b.GetService(t.Context(), "runtime")
	if err != nil || s.Status != infra.StatusCreating || jobRegistrations(f) != before+1 {
		t.Fatalf("record without an operation did not restore: %+v %v", s, err)
	}
}

func TestObservationReloadsStoppedDesiredStateBeforeRestoring(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	stale := ageRegistration(t, b)
	op, err := b.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	before := jobRegistrations(f)
	s, err := b.observed(t.Context(), stale)
	if err != nil || s.Status != infra.StatusStopped || jobRegistrations(f) != before {
		t.Fatalf("stale observation undid Stop: %+v %v", s, err)
	}
}

func TestRestorationNeverOverwritesConcurrentForeignRegistration(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	r := ageRegistration(t, b)
	job, err := b.renderJob(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	foreign := fakeCopy(job["Job"].(map[string]any))
	fakeMap(foreign["Meta"])["karakoram.id"] = "foreign"
	purgeRuntime(t, b)
	transport := b.c.http.Transport
	b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" {
			f.mu.Lock()
			f.jobs[fakeKey("default", "runtime")] = &fakeJob{row: foreign}
			f.mu.Unlock()
		}
		return transport.RoundTrip(request)
	})
	s, err := b.GetService(t.Context(), "runtime")
	if !errors.Is(err, infra.ErrConflict) || s.Status != infra.StatusFailed || !strings.Contains(err.Error(), "refusing adoption") {
		t.Fatalf("foreign registration race was not refused: %+v %v", s, err)
	}
	f.mu.Lock()
	id := fakeMap(f.jobs[fakeKey("default", "runtime")].row["Meta"])["karakoram.id"]
	f.mu.Unlock()
	if id != "foreign" {
		t.Fatal("restore overwrote the concurrent foreign job")
	}
}

func TestDelayedRestorationLifecycleFence(t *testing.T) {
	for _, action := range []string{"stop", "delete", "restart"} {
		for _, outcome := range []string{"success", "lost-response", "cancelled"} {
			t.Run(action+"/"+outcome, func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				spec := runtimeSpec("runtime", "")
				spec.Env = map[string]string{"SECRET": "value"}
				importFake(t, b, spec)
				deployFake(t, b, "runtime", "")
				ageRegistration(t, b)
				purgeRuntime(t, b)
				other := fakeBackend(t, f, nil)
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				transport := b.c.http.Transport
				b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
					if request.Method != http.MethodPost || request.URL.Path != "/v1/jobs" {
						return transport.RoundTrip(request)
					}
					deadline, ok := request.Context().Deadline()
					if !ok || time.Until(deadline) > restoreSubmitTimeout {
						return nil, errors.New("restoration submission has no bounded deadline")
					}
					close(entered)
					<-release
					response, err := transport.RoundTrip(request)
					if err != nil || outcome == "success" {
						return response, err
					}
					// The server committed the job, but the observer cannot learn
					// the outcome. Neither failure may discard the cleanup owner.
					_ = response.Body.Close()
					if outcome == "cancelled" {
						cancel()
						return nil, context.Canceled
					}
					return nil, io.ErrUnexpectedEOF
				})
				result := make(chan error, 1)
				go func() {
					_, err := b.GetService(ctx, "runtime")
					result <- err
				}()
				<-entered // The durable claim precedes the blocked jobs request.
				claim, err := other.require(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				now := claim.RegistrationIssued
				other.clock = func() time.Time { return now }
				var op infra.Op
				switch action {
				case "stop":
					op, err = other.Stop(t.Context(), "runtime")
				case "delete":
					op, err = other.DeleteService(t.Context(), "runtime")
				case "restart":
					op, err = other.Restart(t.Context(), "runtime")
				}
				if err != nil {
					t.Fatal(err)
				}
				// Discard the lifecycle issuer as well: only durable state can
				// tell a reconstructed backend that absence is not yet final.
				other = fakeBackend(t, f, nil)
				other.clock = func() time.Time { return now }
				for _, elapsed := range []time.Duration{0, restoreFence - time.Nanosecond} {
					now = claim.RegistrationIssued.Add(elapsed)
					state, err := other.OpStatus(t.Context(), op)
					if err != nil || state.Done || state.Status != "stopping" {
						t.Fatalf("lifecycle completed inside fence: %+v %v", state, err)
					}
					r, err := other.require(t.Context(), "runtime")
					if err != nil || r == nil || !r.RegistrationIssued.Equal(claim.RegistrationIssued) {
						t.Fatalf("lifecycle discarded registration fence: %+v %v", r, err)
					}
					env, err := other.readVariable(t.Context(), "nomad/jobs/runtime")
					if err != nil || env.ModifyIndex == 0 {
						t.Fatalf("lifecycle removed environment inside fence: %+v %v", env, err)
					}
					if action == "stop" {
						service, err := other.GetService(t.Context(), "runtime")
						if err != nil || service.Status == infra.StatusStopped {
							t.Fatalf("stop observation completed inside fence: %+v %v", service, err)
						}
					}
				}
				unblock()
				restoreErr := <-result
				if (restoreErr == nil) != (outcome == "success") {
					t.Fatalf("unexpected restoration outcome %s: %v", outcome, restoreErr)
				}
				var landed observedJob
				if _, err := other.c.get(t.Context(), "/v1/job/runtime", nil, &landed); err != nil || landed.Stop {
					t.Fatalf("delayed POST did not land a running job: %+v %v", landed, err)
				}
				state, err := other.OpStatus(t.Context(), op)
				if err != nil || state.Done || state.Status != "stopping" {
					t.Fatalf("landed job bypassed fence: %+v %v", state, err)
				}
				// A later control-plane restart still consumes the persisted
				// obligation, even though the restorer has exited with an error.
				other = fakeBackend(t, f, nil)
				now = claim.RegistrationIssued.Add(restoreFence)
				other.clock = func() time.Time { return now }
				state, err = other.OpStatus(t.Context(), op)
				if err != nil || state.Done || state.Status != "stopping" {
					t.Fatalf("late job was not stopped before completion: %+v %v", state, err)
				}
				var stopped observedJob
				if _, err := other.c.get(t.Context(), "/v1/job/runtime", nil, &stopped); err != nil || !stopped.Stop {
					t.Fatalf("fence owner did not stop landed job: %+v %v", stopped, err)
				}
				awaitFake(t, other, op)
				r, err := other.load(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				if action == "restart" {
					if r == nil || r.Desired != "running" || !r.RegistrationIssued.Equal(claim.RegistrationIssued) || jobRegistrations(f) != 4 {
						t.Fatalf("replacement failed to converge: %+v; registrations=%d", r, jobRegistrations(f))
					}
					return
				}
				if _, err = other.c.get(t.Context(), "/v1/job/runtime", nil, nil); !errors.Is(err, infra.ErrNotFound) {
					t.Fatalf("completed %s left a resurrected job: %v", action, err)
				}
				if action == "stop" {
					if r == nil || r.Desired != "stopped" || r.Pending.Phase != "done" {
						t.Fatalf("stop lost desired stopped record: %+v", r)
					}
				} else {
					env, err := other.readVariable(t.Context(), "nomad/jobs/runtime")
					if err != nil || r != nil || env.ModifyIndex != 0 {
						t.Fatalf("delete retained Variables after cleanup: %+v %+v %v", r, env, err)
					}
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				for _, a := range f.allocs {
					if a.row["JobID"] == "runtime" && !fakeTerminal(a) {
						t.Fatalf("completed %s left a live predecessor allocation", action)
					}
				}
			})
		}
	}
}

func TestRestorerNeverCompensatesAgainstSuccessor(t *testing.T) {
	for _, changed := range []string{"generation", "modify-index", "owner"} {
		t.Run(changed, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			ageRegistration(t, b)
			purgeRuntime(t, b)
			other := fakeBackend(t, f, nil)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			transport := b.c.http.Transport
			b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
				response, err := transport.RoundTrip(request)
				if err == nil && request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" {
					close(entered)
					<-release
				}
				return response, err
			})
			result := make(chan error, 1)
			go func() {
				_, err := b.GetService(t.Context(), "runtime")
				result <- err
			}()
			<-entered // After the restorer's last recheck, before it can return.
			r, err := other.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			r.Desired = "stopped"
			if err = other.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			job := fakeCopy(f.jobs[fakeKey("default", "runtime")].row)
			f.mu.Unlock()
			switch changed {
			case "generation":
				fakeMap(job["Meta"])["karakoram.restore"] = "successor"
			case "modify-index":
				// An otherwise identical registration still gets a new index.
			case "owner":
				fakeMap(job["Meta"])["karakoram.id"] = "foreign"
			}
			if err = other.c.post(t.Context(), "/v1/jobs", nil, map[string]any{"Job": job}, nil); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			successor := fakeCopy(f.jobs[fakeKey("default", "runtime")].row)
			f.mu.Unlock()
			before := len(f.requests())
			unblock()
			if err = <-result; err != nil {
				t.Fatal(err)
			}
			// With no compensating recheck/write, there is no GET/DELETE
			// interval in which a replacement can be destroyed.
			if requests := f.requests()[before:]; len(requests) != 0 {
				t.Fatalf("restorer issued post-submit compensation: %+v", requests)
			}
			f.mu.Lock()
			got := f.jobs[fakeKey("default", "runtime")]
			preserved := got != nil && reflect.DeepEqual(fakeCopy(got.row), successor) && got.row["Stop"] != true
			f.mu.Unlock()
			if !preserved {
				t.Fatal("restorer touched the successor job")
			}
		})
	}
}

func TestRestorationCleanupRetrySurvivesBackendReconstruction(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	spec := runtimeSpec("runtime", "")
	spec.Env = map[string]string{"SECRET": "value"}
	importFake(t, b, spec)
	deployFake(t, b, "runtime", "")
	ageRegistration(t, b)
	purgeRuntime(t, b)
	if _, err := b.GetService(t.Context(), "runtime"); err != nil {
		t.Fatal(err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	other := fakeBackend(t, f, nil)
	op, err := other.DeleteService(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	other.clock = func() time.Time { return r.RegistrationIssued.Add(restoreFence) }
	transport := other.c.http.Transport
	purgeFailed := false
	other.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodDelete && request.URL.Path == "/v1/job/runtime" && request.URL.Query().Get("purge") == "true" {
			purgeFailed = true
			return nil, io.ErrUnexpectedEOF
		}
		return transport.RoundTrip(request)
	})
	for range 20 {
		state, err := other.OpStatus(t.Context(), op)
		if state.Done {
			t.Fatal("delete completed despite failed cleanup")
		}
		if err != nil {
			if !purgeFailed {
				t.Fatalf("unexpected observation error: %v", err)
			}
			break
		}
	}
	if !purgeFailed {
		t.Fatal("cleanup did not attempt purge")
	}
	retained, err := other.require(t.Context(), "runtime")
	if err != nil || retained.Pending.Phase != "stopping" || !retained.RegistrationIssued.Equal(r.RegistrationIssued) {
		t.Fatalf("failed cleanup lost durable owner: %+v %v", retained, err)
	}
	env, err := other.readVariable(t.Context(), "nomad/jobs/runtime")
	if err != nil || env.ModifyIndex == 0 {
		t.Fatalf("failed cleanup discarded environment: %+v %v", env, err)
	}
	other = fakeBackend(t, f, nil)
	other.clock = func() time.Time { return retained.Pending.Lease.Until.Add(restoreFence) }
	awaitFake(t, other, op)
	if retained, err = other.load(t.Context(), "runtime"); err != nil || retained != nil {
		t.Fatalf("reconstructed cleanup did not remove record: %+v %v", retained, err)
	}
	if _, err = other.c.get(t.Context(), "/v1/job/runtime", nil, nil); !errors.Is(err, infra.ErrNotFound) {
		t.Fatalf("reconstructed cleanup left job: %v", err)
	}
}

func TestRestorationFenceRefusesForeignJobAtCompletion(t *testing.T) {
	for _, action := range []string{"stop", "delete", "restart"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			ageRegistration(t, b)
			purgeRuntime(t, b)
			if _, err := b.GetService(t.Context(), "runtime"); err != nil {
				t.Fatal(err)
			}
			var op infra.Op
			var err error
			switch action {
			case "stop":
				op, err = b.Stop(t.Context(), "runtime")
			case "delete":
				op, err = b.DeleteService(t.Context(), "runtime")
			case "restart":
				// Restart of a running allocation is not a replacement; force
				// the explicit replacement path with its missing-job fallback.
				purgeRuntime(t, b)
				op, err = b.Restart(t.Context(), "runtime")
			}
			if err != nil {
				t.Fatal(err)
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			job, err := b.renderJob(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			foreign := fakeCopy(job["Job"].(map[string]any))
			fakeMap(foreign["Meta"])["karakoram.id"] = "foreign"
			f.mu.Lock()
			f.jobs[fakeKey("default", "runtime")] = &fakeJob{row: foreign}
			f.mu.Unlock()
			b = fakeBackend(t, f, nil)
			b.clock = func() time.Time { return r.RegistrationIssued.Add(restoreFence) }
			before := len(f.requests())
			state, err := b.OpStatus(t.Context(), op)
			if !errors.Is(err, infra.ErrConflict) || state.Done {
				t.Fatalf("foreign job was accepted for completion: %+v %v", state, err)
			}
			for _, request := range f.requests()[before:] {
				if request.Method == http.MethodDelete {
					t.Fatalf("completion touched foreign job: %+v", request)
				}
			}
			if _, err := b.require(t.Context(), "runtime"); err != nil {
				t.Fatalf("foreign job conflict discarded cleanup owner: %v", err)
			}
		})
	}
}

func TestOperationLeaseExcludesConcurrentReplacementPollers(t *testing.T) {
	for _, pause := range []string{"record-load", "lease-cas", "job-observation", "purge", "expired-observation"} {
		t.Run(pause, func(t *testing.T) {
			f := newFakeNomad(t)
			issuer := fakeBackend(t, f, nil)
			importFake(t, issuer, runtimeSpec("runtime", ""))
			deployFake(t, issuer, "runtime", "")
			op, err := issuer.Start(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			a, b := fakeBackend(t, f, nil), fakeBackend(t, f, nil)
			now := time.Now()
			a.clock = func() time.Time { return now }
			b.clock = func() time.Time { return now }
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			transport := a.c.http.Transport
			a.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
				if pause == "record-load" && request.Method == http.MethodGet && request.URL.Path == "/v1/var/"+recordPrefix+"runtime" {
					response, err := transport.RoundTrip(request)
					close(entered)
					<-release
					return response, err
				}
				match := pause == "lease-cas" && request.Method == http.MethodPut && request.URL.Path == "/v1/var/"+recordPrefix+"runtime" ||
					(pause == "job-observation" || pause == "expired-observation") && request.Method == http.MethodGet && request.URL.Path == "/v1/job/runtime" ||
					pause == "purge" && request.Method == http.MethodDelete && request.URL.Path == "/v1/job/runtime"
				if match {
					close(entered)
					<-release
				}
				return transport.RoundTrip(request)
			})
			result := make(chan error, 1)
			go func() {
				_, err := a.OpStatus(t.Context(), op)
				result <- err
			}()
			<-entered
			canAdvance := pause == "record-load" || pause == "lease-cas" || pause == "expired-observation"
			if pause == "expired-observation" {
				r, err := b.require(t.Context(), "runtime")
				if err != nil || r.Pending.Lease == nil {
					t.Fatalf("paused poll did not persist its lease: %+v %v", r, err)
				}
				now = r.Pending.Lease.Until.Add(restoreFence)
			}
			if canAdvance {
				awaitFake(t, b, op)
			} else {
				before := len(f.requests())
				for range 3 {
					state, err := b.OpStatus(t.Context(), op)
					if err != nil || state.Done || state.Status != "stopping" {
						t.Fatalf("second poller bypassed held lease: %+v %v", state, err)
					}
				}
				if _, err := b.Start(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
					t.Fatalf("new operation bypassed held lease: %v", err)
				}
				for _, request := range f.requests()[before:] {
					if request.Method != http.MethodGet {
						t.Fatalf("lease contender issued a side effect: %+v", request)
					}
				}
			}
			before := len(f.requests())
			unblock()
			if err := <-result; err != nil && !errors.Is(err, infra.ErrConflict) {
				t.Fatalf("paused poll failed unexpectedly: %v", err)
			}
			if canAdvance {
				for _, request := range f.requests()[before:] {
					if request.Method == http.MethodPost && request.Path == "/v1/jobs" ||
						request.Method == http.MethodDelete && request.Path == "/v1/job/runtime" {
						t.Fatalf("stale stopping poll touched successor: %+v", request)
					}
				}
			} else {
				awaitFake(t, b, op)
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil || r.Pending.Phase != "done" {
				t.Fatalf("replacement did not complete: %+v %v", r, err)
			}
			var job observedJob
			if _, err := b.c.get(t.Context(), "/v1/job/runtime", nil, &job); err != nil || job.Stop ||
				job.Meta["karakoram.operation"] != r.Pending.Issued.Format(time.RFC3339Nano) {
				t.Fatalf("valid successor was stopped or purged: %+v %v", job, err)
			}
		})
	}
}

func TestOperationLeaseTakeoverAfterDeliveryMargin(t *testing.T) {
	f := newFakeNomad(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	op, err := b.Start(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeOp(op)
	if err != nil || decoded.Lease != nil {
		t.Fatalf("legacy lease-free handle no longer decodes: %+v %v", decoded, err)
	}
	r, err := b.require(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(operationLeaseDuration)
	r.Pending.Lease = &operationLease{Owner: "crashed-instance", Until: until}
	if err := b.save(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	b = fakeBackend(t, f, nil)
	for _, now := range []time.Time{until.Add(-time.Nanosecond), until, until.Add(restoreFence - time.Nanosecond)} {
		b.clock = func() time.Time { return now }
		before := len(f.requests())
		state, err := b.OpStatus(t.Context(), op)
		if err != nil || state.Done || state.Status != "stopping" {
			t.Fatalf("takeover before delivery margin elapsed: %+v %v", state, err)
		}
		for _, request := range f.requests()[before:] {
			if request.Method != http.MethodGet {
				t.Fatalf("blocked takeover issued side effect: %+v", request)
			}
		}
	}
	b.clock = func() time.Time { return until.Add(restoreFence) }
	awaitFake(t, b, op)
	r, err = b.require(t.Context(), "runtime")
	if err != nil || r.Pending.Phase != "done" || r.Pending.Lease != nil {
		t.Fatalf("takeover did not finish/release lease: %+v %v", r, err)
	}
}

func TestLifecycleBeginRefusesPreexistingForeignJob(t *testing.T) {
	for _, action := range []string{"deploy", "start", "stop", "delete"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			f.mu.Lock()
			fakeMap(f.jobs[fakeKey("default", "runtime")].row["Meta"])["karakoram.id"] = "foreign"
			f.mu.Unlock()
			before := len(f.requests())
			var err error
			switch action {
			case "deploy":
				_, err = b.Deploy(t.Context(), "runtime", testDeployment(t, ""))
			case "start":
				_, err = b.Start(t.Context(), "runtime")
			case "stop":
				_, err = b.Stop(t.Context(), "runtime")
			case "delete":
				_, err = b.DeleteService(t.Context(), "runtime")
			}
			if !errors.Is(err, infra.ErrConflict) {
				t.Fatalf("begin did not refuse foreign ownership: %v", err)
			}
			for _, request := range f.requests()[before:] {
				if request.Method == http.MethodPost && request.Path == "/v1/jobs" ||
					request.Method == http.MethodDelete && request.Path == "/v1/job/runtime" {
					t.Fatalf("begin touched foreign job: %+v", request)
				}
			}
			var job observedJob
			if _, err := b.c.get(t.Context(), "/v1/job/runtime", nil, &job); err != nil || job.Stop || job.Meta["karakoram.id"] != "foreign" {
				t.Fatalf("foreign job did not survive begin: %+v %v", job, err)
			}
		})
	}
}

func TestLifecycleEnforcedStopRejectsChangedJob(t *testing.T) {
	for _, phase := range []string{"begin", "stopping"} {
		t.Run(phase, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			var op infra.Op
			if phase == "stopping" {
				var err error
				op, err = b.Stop(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				f.mu.Lock()
				f.jobs[fakeKey("default", "runtime")].row["Stop"] = false
				f.mu.Unlock()
			}
			transport := b.c.http.Transport
			changed := false
			b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
				if request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" {
					body, err := request.GetBody()
					if err != nil {
						return nil, err
					}
					var registration struct {
						EnforceIndex   bool
						JobModifyIndex uint64
						Job            struct{ Stop bool }
					}
					err = json.NewDecoder(body).Decode(&registration)
					_ = body.Close()
					if err != nil || !registration.EnforceIndex || registration.JobModifyIndex == 0 || !registration.Job.Stop {
						return nil, errors.New("stop was not an exact index-enforced registration")
					}
					deadline, ok := request.Context().Deadline()
					if !ok || time.Until(deadline) > operationRequestTimeout {
						return nil, errors.New("stop request has no bounded deadline")
					}
					f.mu.Lock()
					job := f.jobs[fakeKey("default", "runtime")].row
					job["JobModifyIndex"] = f.nextIndex()
					fakeMap(job["Meta"])["karakoram.id"] = "foreign-successor"
					f.mu.Unlock()
					changed = true
				}
				return transport.RoundTrip(request)
			})
			if phase == "begin" {
				if _, err := b.Stop(t.Context(), "runtime"); err != nil {
					t.Fatalf("index conflict should re-poll: %v", err)
				}
			} else {
				state, err := b.OpStatus(t.Context(), op)
				if err != nil || state.Done {
					t.Fatalf("index conflict should remain pending: %+v %v", state, err)
				}
			}
			if !changed {
				t.Fatal("test did not exercise enforced stop")
			}
			var job observedJob
			if _, err := b.c.get(t.Context(), "/v1/job/runtime", nil, &job); err != nil || job.Stop || job.Meta["karakoram.id"] != "foreign-successor" {
				t.Fatalf("index mismatch stopped successor: %+v %v", job, err)
			}
		})
	}
}

func TestOperationLeaseKnownFailureReleasesImmediately(t *testing.T) {
	for _, phase := range []string{"begin", "poll"} {
		for _, status := range []int{400, 403, 422} {
			for _, retry := range []string{"same-backend", "other-backend"} {
				t.Run(phase+"/"+strconv.Itoa(status)+"/"+retry, func(t *testing.T) {
					f := newFakeNomad(t)
					b := fakeBackend(t, f, nil)
					importFake(t, b, runtimeSpec("runtime", ""))
					deployFake(t, b, "runtime", "")
					var op infra.Op
					var err error
					if phase == "poll" {
						op, err = b.Start(t.Context(), "runtime")
						if err != nil {
							t.Fatal(err)
						}
					}
					transport := b.c.http.Transport
					injected := false
					b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
						if phase == "begin" && request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" ||
							phase == "poll" && request.Method == http.MethodGet && request.URL.Path == "/v1/job/runtime" {
							injected = true
							return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("request rejected"))}, nil
						}
						return transport.RoundTrip(request)
					})
					if phase == "begin" {
						_, err = b.Start(t.Context(), "runtime")
					} else {
						_, err = b.OpStatus(t.Context(), op)
					}
					var apiErr *APIError
					if !injected || !errors.As(err, &apiErr) || apiErr.Status != status {
						t.Fatalf("did not exercise definitive rejection: %v", err)
					}
					b.c.http.Transport = transport
					r, err := b.require(t.Context(), "runtime")
					if err != nil || r.Pending.Lease != nil || r.Pending.Phase != "stopping" {
						t.Fatalf("definitive rejection retained lease or changed phase: %+v %v", r, err)
					}
					op = encodeOp(*r.Pending)
					if retry == "other-backend" {
						b = fakeBackend(t, f, nil)
					}
					awaitFake(t, b, op) // No time advance or quarantine bypass.
				})
			}
		}
	}
}

func TestOperationLeaseUnknownFailureRetainsDeliveryQuarantine(t *testing.T) {
	for _, phase := range []string{"begin", "stop", "purge", "register", "phase-save", "claim-save"} {
		for _, outcome := range []string{"transport", "deadline", "cancelled", "server-error"} {
			t.Run(phase+"/"+outcome, func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				importFake(t, b, runtimeSpec("runtime", ""))
				deployFake(t, b, "runtime", "")
				var op infra.Op
				var err error
				if phase != "begin" {
					op, err = b.Start(t.Context(), "runtime")
					if err != nil {
						t.Fatal(err)
					}
					if phase == "stop" {
						f.mu.Lock()
						f.jobs[fakeKey("default", "runtime")].row["Stop"] = false
						f.mu.Unlock()
					}
					if phase == "register" {
						for range 20 {
							state, err := b.OpStatus(t.Context(), op)
							if err != nil {
								t.Fatal(err)
							}
							if state.Status == "registering" {
								break
							}
						}
					}
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				transport := b.c.http.Transport
				injected := false
				writes := 0
				b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodPut && request.URL.Path == "/v1/var/"+recordPrefix+"runtime" {
						writes++
					}
					match := (phase == "begin" || phase == "stop" || phase == "register") && request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" ||
						phase == "purge" && request.Method == http.MethodDelete && request.URL.Path == "/v1/job/runtime" ||
						(phase == "phase-save" && writes == 2 || phase == "claim-save" && writes == 1) &&
							request.Method == http.MethodPut && request.URL.Path == "/v1/var/"+recordPrefix+"runtime"
					if match {
						injected = true
						// Commit the mutation, then hide its acknowledgment.
						response, err := transport.RoundTrip(request)
						if err != nil {
							return response, err
						}
						_ = response.Body.Close()
						switch outcome {
						case "transport":
							return nil, io.ErrUnexpectedEOF
						case "deadline":
							return nil, context.DeadlineExceeded
						case "cancelled":
							cancel()
							return nil, context.Canceled
						default:
							return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("temporarily unavailable"))}, nil
						}
					}
					return transport.RoundTrip(request)
				})
				if phase == "begin" {
					_, err = b.Start(ctx, "runtime")
				} else {
					_, err = b.OpStatus(ctx, op)
				}
				if !injected || err == nil {
					t.Fatalf("did not exercise unknown outcome: %v", err)
				}
				b.c.http.Transport = transport
				r, err := b.require(t.Context(), "runtime")
				if err != nil || r.Pending.Lease == nil {
					t.Fatalf("unknown outcome discarded exclusion: %+v %v", r, err)
				}
				op = encodeOp(*r.Pending)
				for _, candidate := range []*Backend{b, fakeBackend(t, f, nil)} {
					candidate.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence - time.Nanosecond) }
					before := len(f.requests())
					state, err := candidate.OpStatus(t.Context(), op)
					if err != nil || state.Done || state.Status != r.Pending.Phase {
						t.Fatalf("unknown outcome bypassed quarantine: %+v %v", state, err)
					}
					for _, request := range f.requests()[before:] {
						if request.Method != http.MethodGet {
							t.Fatalf("quarantined retry issued side effect: %+v", request)
						}
					}
				}
				b = fakeBackend(t, f, nil)
				b.clock = func() time.Time { return r.Pending.Lease.Until.Add(restoreFence) }
				awaitFake(t, b, op)
			})
		}
	}
}

func TestKnownFailureLeaseReleaseUsesFreshExactClaim(t *testing.T) {
	for _, changed := range []string{"none", "operation", "owner", "until", "record"} {
		t.Run(changed, func(t *testing.T) {
			f := newFakeNomad(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			if _, err := b.Start(t.Context(), "runtime"); err != nil {
				t.Fatal(err)
			}
			r, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			lease, err := b.newOperationLease()
			if err != nil {
				t.Fatal(err)
			}
			r.Pending.Lease = lease
			if err := b.save(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			fresh, err := b.require(t.Context(), "runtime")
			if err != nil {
				t.Fatal(err)
			}
			fresh.Vars["fresh"] = "preserved"
			switch changed {
			case "operation":
				fresh.Pending.Issued = fresh.Pending.Issued.Add(time.Second)
			case "owner":
				fresh.Pending.Lease.Owner = "another-instance"
			case "until":
				fresh.Pending.Lease.Until = fresh.Pending.Lease.Until.Add(time.Second)
			case "record":
				fresh.ID = "successor-record"
			}
			if err := b.save(t.Context(), fresh); err != nil {
				t.Fatal(err)
			}
			// These failed-attempt mutations must never be persisted by cleanup.
			r.Pending.Phase = "registering"
			r.Vars["unsaved"] = "discard"
			rejected := &APIError{Status: 400, Message: "request rejected"}
			if err := b.releaseFailedOperation(t.Context(), r, lease, rejected); !errors.Is(err, rejected) {
				t.Fatalf("cleanup lost original error: %v", err)
			}
			got, err := b.require(t.Context(), "runtime")
			if err != nil || got.Pending.Phase != "stopping" || got.Vars["fresh"] != "preserved" || got.Vars["unsaved"] != "" {
				t.Fatalf("cleanup persisted failed-attempt state: %+v %v", got, err)
			}
			if (got.Pending.Lease == nil) != (changed == "none") {
				t.Fatalf("cleanup released a different claim or retained ours: %+v", got.Pending.Lease)
			}
		})
	}
}

func TestOperationReadFailureReleasesLease(t *testing.T) {
	for _, stage := range []string{"allocations", "job", "evaluation", "after-register", "restart-allocations"} {
		for _, outcome := range []string{"transport", "deadline", "cancelled", "server-error"} {
			for _, retry := range []string{"same", "reconstructed"} {
				t.Run(stage+"/"+outcome+"/"+retry, func(t *testing.T) {
					f := newFakeNomad(t)
					b := fakeBackend(t, f, nil)
					importFake(t, b, runtimeSpec("runtime", ""))
					deployFake(t, b, "runtime", "")
					var op infra.Op
					var err error
					if stage != "restart-allocations" {
						op, err = b.Start(t.Context(), "runtime")
						if err != nil {
							t.Fatal(err)
						}
					}
					if stage == "evaluation" || stage == "after-register" {
						want := "starting"
						if stage == "after-register" {
							want = "registering"
						}
						for range 20 {
							state, err := b.OpStatus(t.Context(), op)
							if err != nil {
								t.Fatal(err)
							}
							if state.Status == want {
								break
							}
						}
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					transport := b.c.http.Transport
					injected := false
					jobReads := 0
					b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
						if request.Method == http.MethodGet && request.URL.Path == "/v1/job/runtime" {
							jobReads++
						}
						match := request.Method == http.MethodGet &&
							((stage == "allocations" || stage == "restart-allocations") && request.URL.Path == "/v1/job/runtime/allocations" ||
								stage == "job" && request.URL.Path == "/v1/job/runtime" ||
								stage == "evaluation" && strings.HasPrefix(request.URL.Path, "/v1/evaluation/") ||
								stage == "after-register" && request.URL.Path == "/v1/job/runtime" && jobReads == 2)
						if match {
							injected = true
							switch outcome {
							case "transport":
								return nil, io.ErrUnexpectedEOF
							case "deadline":
								return nil, context.DeadlineExceeded
							case "cancelled":
								cancel()
								return nil, context.Canceled
							default:
								return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("temporarily unavailable"))}, nil
							}
						}
						return transport.RoundTrip(request)
					})
					if stage == "restart-allocations" {
						_, err = b.Restart(ctx, "runtime")
					} else {
						_, err = b.OpStatus(ctx, op)
					}
					if !injected || err == nil {
						t.Fatalf("read failure was not exercised: %v", err)
					}
					b.c.http.Transport = transport
					r, err := b.require(t.Context(), "runtime")
					if err != nil || r.RestartLease != nil || r.Pending.Lease != nil {
						t.Fatalf("read-only failure retained exclusion: %+v %v", r, err)
					}
					if retry == "reconstructed" {
						b = fakeBackend(t, f, nil)
					}
					if stage == "restart-allocations" {
						if _, err := b.Restart(t.Context(), "runtime"); err != nil {
							t.Fatalf("healthy restart did not proceed immediately: %v", err)
						}
					} else {
						awaitFake(t, b, op)
					}
				})
			}
		}
	}
}

func TestRunningRestartHonorsHeldLifecycleLease(t *testing.T) {
	for _, action := range []string{"stop", "replacement"} {
		for _, state := range []string{"held", "quarantined"} {
			t.Run(action+"/"+state, func(t *testing.T) {
				f := newFakeNomad(t)
				issuer := fakeBackend(t, f, nil)
				importFake(t, issuer, runtimeSpec("runtime", ""))
				deployFake(t, issuer, "runtime", "")
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				transport := issuer.c.http.Transport
				issuer.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodPost && request.URL.Path == "/v1/jobs" {
						close(entered)
						if state == "quarantined" {
							return nil, io.ErrUnexpectedEOF
						}
						<-release
					}
					return transport.RoundTrip(request)
				})
				result := make(chan error, 1)
				go func() {
					var err error
					if action == "stop" {
						_, err = issuer.Stop(t.Context(), "runtime")
					} else {
						_, err = issuer.Start(t.Context(), "runtime")
					}
					result <- err
				}()
				<-entered
				if state == "quarantined" {
					if err := <-result; err == nil {
						t.Fatal("did not quarantine unknown initial stop")
					}
				}
				contender := fakeBackend(t, f, nil)
				original, err := contender.require(t.Context(), "runtime")
				if err != nil || original.Pending.Lease == nil {
					t.Fatalf("issuer did not acquire lifecycle lease: %+v %v", original, err)
				}
				if state == "quarantined" {
					contender.clock = func() time.Time { return original.Pending.Lease.Until.Add(time.Second) }
				}
				before := len(f.requests())
				if _, err := contender.Restart(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
					t.Fatalf("restart bypassed %s lifecycle lease: %v", state, err)
				}
				for _, request := range f.requests()[before:] {
					if request.Method != http.MethodGet || strings.HasSuffix(request.Path, "/allocations") {
						t.Fatalf("contender selected/restarted allocation before acquiring exclusion: %+v", request)
					}
				}
				current, err := contender.require(t.Context(), "runtime")
				if err != nil || !reflect.DeepEqual(current.Pending, original.Pending) || current.RestartLease != nil {
					t.Fatalf("restart replaced caller's pending operation: %+v %v", current, err)
				}
				if state == "held" {
					unblock()
					if err := <-result; err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestRunningRestartLeaseOutcomesPreservePending(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, outcome := range []string{"success", "rejected", "transport", "deadline", "cancelled", "server-error"} {
			t.Run(strconv.FormatBool(legacy)+"/"+outcome, func(t *testing.T) {
				f := newFakeNomad(t)
				b := fakeBackend(t, f, nil)
				importFake(t, b, runtimeSpec("runtime", ""))
				deployFake(t, b, "runtime", "")
				original, err := b.require(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				if legacy {
					original.Pending = nil
					if err := b.save(t.Context(), original); err != nil {
						t.Fatal(err)
					}
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				transport := b.c.http.Transport
				restarts := 0
				b.c.http.Transport = testRoundTripper(func(request *http.Request) (*http.Response, error) {
					if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/restart") {
						restarts++
						deadline, ok := request.Context().Deadline()
						if !ok || time.Until(deadline) > operationRequestTimeout {
							return nil, errors.New("allocation restart is not bounded")
						}
						switch outcome {
						case "rejected":
							return &http.Response{StatusCode: 403, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("request rejected"))}, nil
						case "transport":
							return nil, io.ErrUnexpectedEOF
						case "deadline":
							return nil, context.DeadlineExceeded
						case "cancelled":
							cancel()
							return nil, context.Canceled
						case "server-error":
							return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("temporarily unavailable"))}, nil
						}
					}
					return transport.RoundTrip(request)
				})
				before := len(f.requests())
				op, err := b.Restart(ctx, "runtime")
				if restarts != 1 || (err == nil) != (outcome == "success") || op.ID != "" {
					t.Fatalf("unexpected direct restart outcome: %d %+v %v", restarts, op, err)
				}
				b.c.http.Transport = transport
				current, err := b.require(t.Context(), "runtime")
				if err != nil || !reflect.DeepEqual(current.Pending, original.Pending) {
					t.Fatalf("direct restart replaced pending operation: %+v %v", current, err)
				}
				uncertain := outcome != "success" && outcome != "rejected"
				if (current.RestartLease != nil) != uncertain {
					t.Fatalf("incorrect direct restart delivery debt: %+v", current.RestartLease)
				}
				for _, request := range f.requests()[before:] {
					if request.Method == http.MethodPost && request.Path == "/v1/jobs" ||
						request.Method == http.MethodDelete && request.Path == "/v1/job/runtime" {
						t.Fatalf("running restart replaced the job: %+v", request)
					}
				}
				other := fakeBackend(t, f, nil)
				if uncertain {
					if _, err := other.Restart(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
						t.Fatalf("unknown restart outcome did not exclude a contender: %v", err)
					}
					if _, err := other.Start(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
						t.Fatalf("unknown restart outcome did not exclude replacement: %v", err)
					}
					other.clock = func() time.Time { return current.RestartLease.Until.Add(restoreFence) }
				}
				if _, err := other.Restart(t.Context(), "runtime"); err != nil {
					t.Fatalf("healthy direct restart could not acquire/release exclusion: %v", err)
				}
				final, err := other.require(t.Context(), "runtime")
				if err != nil || final.RestartLease != nil || !reflect.DeepEqual(final.Pending, original.Pending) {
					t.Fatalf("direct restart did not release/preserve pending: %+v %v", final, err)
				}
			})
		}
	}
}
