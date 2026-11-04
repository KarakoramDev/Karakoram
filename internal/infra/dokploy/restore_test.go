// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func removeFakeService(f *fakeAPI, id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.services, fakeString(f.apps[id].row, "appName"))
}
func observeRestored(t *testing.T, b *Backend, list bool) infra.Service {
	t.Helper()
	for range 20 {
		var s *infra.Service
		var err error
		if list {
			var services []infra.Service
			services, err = b.ListServices(t.Context(), "runtime")
			if len(services) == 1 {
				s = &services[0]
			}
		} else {
			s, err = b.GetService(t.Context(), "runtime")
		}
		if err != nil || s == nil {
			t.Fatalf("observation: %+v %v", s, err)
		}
		if s.Status == infra.StatusRunning {
			return *s
		}
	}
	t.Fatal("restored service did not run")
	return infra.Service{}
}

func TestServiceInspectionAvoidsSameNamedImage(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
	deployFake(t, b, "runtime", "")
	name := fakeString(fakeAppSnapshot(t, f, "runtime"), "appName")
	var image map[string]any
	if err := b.c.get(t.Context(), "docker.getConfig", map[string]string{"containerId": name}, &image); err != nil {
		t.Fatal(err)
	}
	if image["RepoTags"] == nil || image["Spec"] != nil {
		t.Fatalf("fake name lookup must return image inspect: %v", image)
	}
	service, err := b.GetService(t.Context(), "runtime")
	if err != nil || service.Status != infra.StatusRunning {
		t.Fatalf("same-named image prevented observation: %+v %v", service, err)
	}
	op, err := b.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
	f.mu.Lock()
	f.apps[id].tasks = nil
	f.mu.Unlock()
	// A new backend has no cached service ID and there are no task containers.
	b = fakeBackend(t, f, nil)
	op, err = b.Start(t.Context(), "runtime")
	if err != nil {
		t.Fatalf("stopped service without tasks could not restart: %v", err)
	}
	awaitFake(t, b, op)
}

func TestServiceInspectionDistinguishesForeignFromUnknown(t *testing.T) {
	for _, variant := range []string{"foreign", "image-only", "unavailable", "invalid-id"} {
		t.Run(variant, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			name := fakeString(fakeAppSnapshot(t, f, "runtime"), "appName")
			f.mu.Lock()
			s := f.services[name]
			switch variant {
			case "foreign":
				s["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"] = map[string]any{"karakoram.owner": "another-owner"}
			case "image-only":
				// Even an image returned for the inventory ID is unknown, not
				// positive evidence that a foreign service owns this name.
				f.images[fakeString(s, "ID")[:12]] = true
			case "unavailable":
				f.uninspectable[name] = true
			case "invalid-id":
				s["ID"] = "--invalid"
			}
			f.mu.Unlock()
			_, err := b.GetService(t.Context(), "runtime")
			if err == nil {
				t.Fatal("unsafe inspection was accepted by observation")
			}
			if foreign := strings.Contains(infra.SafeDetail(err), "foreign"); foreign != (variant == "foreign") {
				t.Fatalf("incorrect observation diagnosis: %v", err)
			}
			before := fakeCalls(f, "application.stop")
			_, err = b.Stop(t.Context(), "runtime")
			if err == nil {
				t.Fatal("unsafe inspection permitted a lifecycle mutation")
			}
			if foreign := strings.Contains(infra.SafeDetail(err), "foreign"); foreign != (variant == "foreign") {
				t.Fatalf("incorrect ownership diagnosis: %v", err)
			}
			if fakeCalls(f, "application.stop") != before {
				t.Fatal("unsafe inspection submitted a stop")
			}
		})
	}
}

func TestInventoryExcludedServiceCannotBeMutatedOrRestored(t *testing.T) {
	for _, hostname := range []string{"dokploy", "dokploy-example"} {
		for _, state := range []string{"owned", "foreign", "missing"} {
			t.Run(hostname+"/"+state, func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				id := importFake(t, b, runtimeSpec("runtime", "")).Services["runtime"]
				deployFake(t, b, "runtime", "")
				// Represent an already-deployed application with an excluded name.
				// The current backend also refuses initial deployment for such names.
				f.mu.Lock()
				a := f.apps[id]
				oldName := fakeString(a.row, "appName")
				name := hostname + "-generated"
				a.row["name"], a.row["appName"] = hostname, name
				m, _ := parseMetadata(fakeString(a.row, "description"))
				m.Hostname = hostname
				a.row["description"] = m.description()
				service := f.services[oldName]
				delete(f.services, oldName)
				f.services[name] = service
				spec := service["Spec"].(map[string]any)
				spec["Name"] = name
				spec["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"].(map[string]any)["karakoram.hostname"] = hostname
				if state == "foreign" {
					f.services[name]["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"] = map[string]any{"karakoram.owner": "another-owner"}
				} else if state == "missing" {
					delete(f.services, name)
				}
				f.mu.Unlock()
				procedures := []string{"application.reload", "application.deploy", "application.dropDeployment", "application.stop", "application.delete"}
				before := make(map[string]int, len(procedures))
				for _, procedure := range procedures {
					before[procedure] = fakeCalls(f, procedure)
				}
				s, err := b.GetService(t.Context(), hostname)
				if err == nil || s == nil || s.Status != infra.StatusFailed || !strings.Contains(infra.SafeDetail(err), "excluded") {
					t.Fatalf("excluded service was not diagnosed as unknown: %+v %v", s, err)
				}
				for _, mutate := range []func() (infra.Op, error){
					func() (infra.Op, error) { return b.Start(t.Context(), hostname) },
					func() (infra.Op, error) { return b.Restart(t.Context(), hostname) },
					func() (infra.Op, error) { return b.Deploy(t.Context(), hostname, testDeployment(t, "")) },
					func() (infra.Op, error) { return b.DeleteService(t.Context(), hostname) },
					func() (infra.Op, error) { return b.Stop(t.Context(), hostname) },
				} {
					if _, err := mutate(); err == nil || !strings.Contains(infra.SafeDetail(err), "excluded") {
						t.Fatalf("excluded service lifecycle mutation was not refused: %v", err)
					}
				}
				for _, procedure := range procedures {
					if fakeCalls(f, procedure) != before[procedure] {
						t.Fatalf("excluded service submitted %s", procedure)
					}
				}
			})
		}
	}
}

func TestObservationRestoresDeletedSwarmService(t *testing.T) {
	for _, list := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			t.Run(map[bool]string{false: "get", true: "list"}[list]+map[bool]string{false: "/rebuild", true: "/reload"}[cached], func(t *testing.T) {
				f := newFakeAPI(t)
				b := fakeBackend(t, f, nil)
				imported := importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume"))
				deployFake(t, b, "runtime", "volume")
				before := fakeAppSnapshot(t, f, "runtime")
				m := fakeMeta(t, f, "runtime")
				if m.Image == "" || !m.WriterFenced || m.Desired != "running" {
					t.Fatalf("missing durable intent/image/fence: %+v", m)
				}
				removeFakeService(f, imported.Services["runtime"])
				if !cached {
					f.mu.Lock()
					f.images = map[string]bool{}
					f.mu.Unlock()
				}
				builds, reloads := fakeCalls(f, "application.deploy"), fakeCalls(f, "application.reload")
				s := observeRestored(t, fakeBackend(t, f, nil), list)
				after := fakeAppSnapshot(t, f, "runtime")
				if s.ID != imported.Services["runtime"] || before["appName"] != after["appName"] {
					t.Fatal("restoration changed durable identity")
				}
				wantBuild, wantReload := builds, reloads
				if cached {
					wantReload++
				} else {
					wantBuild++
				}
				if fakeCalls(f, "application.deploy") != wantBuild || fakeCalls(f, "application.reload") != wantReload {
					t.Fatal("restoration did not submit exactly once through the selected path")
				}
				if fakeCalls(f, "application.dropDeployment") != 1 {
					t.Fatal("repair unnecessarily uploaded another artifact")
				}
				if got := fakeMeta(t, f, "runtime"); got.Node != m.Node || got.Volume != m.Volume || got.Pending != nil {
					t.Fatalf("repair changed storage or retained pending op: %+v", got)
				}
			})
		}
	}
}

func TestObservationConvergesExternalStopToDesiredRunning(t *testing.T) {
	for _, ui := range []bool{false, true} {
		t.Run(map[bool]string{false: "scale-zero", true: "dokploy-stop"}[ui], func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			imported := importFake(t, b, runtimeSpec("runtime", ""))
			deployFake(t, b, "runtime", "")
			id := imported.Services["runtime"]
			if ui {
				if err := b.c.post(t.Context(), "application.stop", map[string]any{"applicationId": id}, nil); err != nil {
					t.Fatal(err)
				}
			} else {
				f.mu.Lock()
				a := f.apps[id]
				f.shutdown(a)
				f.services[fakeString(a.row, "appName")]["Spec"].(map[string]any)["Mode"] = map[string]any{"Replicated": map[string]any{"Replicas": 0}}
				f.mu.Unlock()
			}
			observeRestored(t, b, false)
			if fakeCalls(f, "application.reload") != 1 {
				t.Fatal("operator stop was not reconciled exactly once")
			}
		})
	}
}

func TestObservationRefusesUnsafeRestoration(t *testing.T) {
	for _, variant := range []string{"stopped", "holder-removed", "holder-replaced", "holder-wiping", "foreign-owner", "foreign-name", "unknown-inspect", "unknown-inventory", "failed-deployment", "pending-build", "legacy-intent", "legacy-writer"} {
		t.Run(variant, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			ids := importFake(t, b, volumeSpec("volume"), runtimeSpec("runtime", "volume")).Services
			deployFake(t, b, "runtime", "volume")
			if variant == "stopped" {
				op, err := b.Stop(t.Context(), "runtime")
				if err != nil {
					t.Fatal(err)
				}
				awaitFake(t, b, op)
			}
			f.mu.Lock()
			a := f.apps[ids["runtime"]]
			name := fakeString(a.row, "appName")
			m, _ := parseMetadata(fakeString(a.row, "description"))
			deleteService := true
			switch variant {
			case "holder-removed":
				delete(f.apps, ids["volume"])
			case "holder-replaced":
				f.apps[ids["volume"]].row["appName"] = "replacement-volume"
			case "holder-wiping":
				vm, _ := parseMetadata(fakeString(f.apps[ids["volume"]].row, "description"))
				vm.Wiping = true
				f.apps[ids["volume"]].row["description"] = vm.description()
			case "foreign-owner", "foreign-name":
				deleteService = false
				spec := f.services[name]["Spec"].(map[string]any)
				if variant == "foreign-name" {
					spec["Name"] = "foreign"
				} else {
					spec["TaskTemplate"].(map[string]any)["ContainerSpec"].(map[string]any)["Labels"] = map[string]any{"karakoram.owner": "foreign"}
				}
			case "unknown-inspect":
				deleteService = false
				f.uninspectable[name] = true
			case "unknown-inventory":
				f.services = map[string]map[string]any{}
			case "failed-deployment":
				a.deployments[0].status = "error"
			case "pending-build":
				o := operation{Action: "deploy", AppID: ids["runtime"], AppName: name, Hostname: "runtime", Issued: time.Now()}
				o.PreviousDeployments = []string{a.deployments[0].id}
				m.Pending = &o
			case "legacy-intent":
				m.Desired = ""
			case "legacy-writer":
				m.WriterFenced = false
			}
			a.row["description"] = m.description()
			if deleteService {
				delete(f.services, name)
			}
			f.mu.Unlock()
			builds, reloads := fakeCalls(f, "application.deploy"), fakeCalls(f, "application.reload")
			s, err := b.GetService(t.Context(), "runtime")
			if s == nil {
				t.Fatal("lost durable service")
			}
			if variant == "stopped" {
				if err != nil || s.Status != infra.StatusStopped {
					t.Fatalf("stopped: %+v %v", s, err)
				}
			} else if variant == "pending-build" {
				if err != nil || s.Status != infra.StatusCreating {
					t.Fatalf("pending: %+v %v", s, err)
				}
			} else if variant == "failed-deployment" {
				if err != nil || s.Status != infra.StatusFailed {
					t.Fatalf("failed deployment identity unavailable: %+v %v", s, err)
				}
			} else if err == nil || s.Status != infra.StatusFailed {
				t.Fatalf("unsafe variant was not diagnosed: %+v %v", s, err)
			}
			if fakeCalls(f, "application.deploy") != builds || fakeCalls(f, "application.reload") != reloads {
				t.Fatal("unsafe variant submitted repair")
			}
		})
	}
}

func TestApplicationRecordLossRefusesOrphanAdoption(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, runtimeSpec("runtime", "")).Services
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	delete(f.apps, ids["runtime"])
	f.mu.Unlock()
	s, err := b.GetService(t.Context(), "runtime")
	if s == nil || s.Status != infra.StatusFailed || !errors.Is(err, infra.ErrConflict) || !strings.Contains(infra.SafeDetail(err), "without its Dokploy application") {
		t.Fatalf("orphan was adopted or hidden: %+v %v", s, err)
	}
	if _, err = b.DeleteService(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("orphan delete ignored ambiguity: %v", err)
	}
	if _, err = b.ListServices(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("orphan list ignored ambiguity: %v", err)
	}
}

func TestSupersededDeleteCannotRemoveRestartedService(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	importFake(t, b, runtimeSpec("runtime", ""))
	deployFake(t, b, "runtime", "")
	old, err := b.DeleteService(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	start, err := b.Start(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, start)
	state, err := b.OpStatus(t.Context(), old)
	if err != nil || !state.Expired {
		t.Fatalf("superseded deletion was not expired: %+v %v", state, err)
	}
	observeRestored(t, b, false)
	if fakeCalls(f, "application.delete") != 0 {
		t.Fatal("stale delete removed a successor")
	}
}

type restoreTransport func(*http.Request) (*http.Response, error)

func (f restoreTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRestorationSerializesStopAndDeleteAcrossBackends(t *testing.T) {
	for _, action := range []string{"stop", "delete"} {
		t.Run(action, func(t *testing.T) {
			f := newFakeAPI(t)
			b := fakeBackend(t, f, nil)
			other := fakeBackend(t, f, nil)
			ids := importFake(t, b, runtimeSpec("runtime", "")).Services
			deployFake(t, b, "runtime", "")
			removeFakeService(f, ids["runtime"])
			entered, release := make(chan struct{}), make(chan struct{})
			transport := b.c.http.Transport
			if transport == nil {
				transport = http.DefaultTransport
			}
			b.c.http.Transport = restoreTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.Path == "/api/application.reload" {
					close(entered)
					<-release
				}
				return transport.RoundTrip(r)
			})
			observed := make(chan error, 1)
			go func() { _, err := b.GetService(t.Context(), "runtime"); observed <- err }()
			<-entered
			mutated := make(chan error, 1)
			go func() {
				var op infra.Op
				var err error
				if action == "stop" {
					op, err = other.Stop(t.Context(), "runtime")
				} else {
					// Deletion now retains the restoration restart receipt
					// until its exact replacement tasks converge.
					for range 20 {
						op, err = other.DeleteService(t.Context(), "runtime")
						if !errors.Is(err, infra.ErrConflict) {
							break
						}
					}
				}
				if err == nil {
					for range 20 {
						var state infra.OpState
						state, err = other.OpStatus(t.Context(), op)
						if err != nil || state.Done {
							break
						}
					}
				}
				mutated <- err
			}()
			select {
			case err := <-mutated:
				t.Fatalf("lifecycle escaped shared exclusion: %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			close(release)
			if err := <-observed; err != nil {
				t.Fatal(err)
			}
			if err := <-mutated; err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			defer f.mu.Unlock()
			if action == "delete" {
				if f.apps[ids["runtime"]] != nil {
					t.Fatal("deleted record remains")
				}
			} else if m, _ := parseMetadata(fakeString(f.apps[ids["runtime"]].row, "description")); m.Desired != "stopped" {
				t.Fatal("Stop lost durable intent")
			}
			for name, service := range f.services {
				if name == "dokploy" {
					continue
				}
				replicas := service["Spec"].(map[string]any)["Mode"].(map[string]any)["Replicated"].(map[string]any)["Replicas"]
				if replicas != 0 {
					t.Fatalf("restoration resurrected lifecycle predecessor: %v", replicas)
				}
			}
		})
	}
}

func TestAmbiguousRestorationRetainsDurableFence(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, runtimeSpec("runtime", "")).Services
	deployFake(t, b, "runtime", "")
	removeFakeService(f, ids["runtime"])
	transport := b.c.http.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	b.c.http.Transport = restoreTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/application.reload" {
			return nil, context.Canceled
		}
		return transport.RoundTrip(r)
	})
	if _, err := b.GetService(t.Context(), "runtime"); err == nil {
		t.Fatal("lost response not exercised")
	}
	other := fakeBackend(t, f, nil)
	if _, err := other.Stop(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("lost-response restoration was not fenced: %v", err)
	}
	m := fakeMeta(t, f, "runtime")
	if m.Desired != "stopped" || m.Pending == nil || m.MutationUntil.IsZero() {
		t.Fatalf("fence/stop intent lost: %+v", m)
	}
	other.now = func() time.Time { return m.MutationUntil.Add(time.Second) }
	op, err := other.Stop(t.Context(), "runtime")
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, other, op)
	if fakeCalls(f, "application.reload") != 0 {
		t.Fatal("ambiguous request was retried")
	}
}

func TestRepairBuildBlocksLifecycleAndDuplicateSubmissions(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, runtimeSpec("runtime", "")).Services
	deployFake(t, b, "runtime", "")
	removeFakeService(f, ids["runtime"])
	f.mu.Lock()
	f.images = map[string]bool{}
	f.apps[ids["runtime"]].holdDeployments = true
	f.mu.Unlock()
	for range 3 {
		if _, err := b.GetService(t.Context(), "runtime"); err != nil {
			t.Fatal(err)
		}
	}
	if fakeCalls(f, "application.deploy") != 1 {
		t.Fatal("duplicate builds were queued")
	}
	if _, err := b.DeleteService(t.Context(), "runtime"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("delete raced queued build: %v", err)
	}
	f.mu.Lock()
	f.apps[ids["runtime"]].holdDeployments = false
	f.mu.Unlock()
	var op infra.Op
	var err error
	for range 10 {
		op, err = b.DeleteService(t.Context(), "runtime")
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	awaitFake(t, b, op)
}

func TestRenderedVolumeWriterFenceSurvivesExec(t *testing.T) {
	if _, err := exec.LookPath("flock"); err != nil {
		t.Skip("flock not installed")
	}
	dir := t.TempDir()
	volume := filepath.Join(dir, "volume")
	if err := os.MkdirAll(volume, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "karakoram.env"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	// Match the already-initialized volume owner without requiring root in tests.
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte("#!/bin/sh\nprintf '10001:10001\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	script := strings.ReplaceAll(renderEntrypoint("data"), layout.VolumePath("data"), volume)
	script = strings.ReplaceAll(script, layout.AppDir, dir)
	entry := filepath.Join(dir, "entrypoint")
	if err := os.WriteFile(entry, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(volume, ".karakoram-writer.lock")
	cmd := exec.CommandContext(t.Context(), entry, "sh", "-c", `if flock -n "$1" true; then exit 77; fi`, "sh", lock)
	cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("volume lock not retained across exec: %s %v", output, err)
	}
	if strings.Contains(renderEntrypoint(""), "flock") {
		t.Fatal("stateless entrypoint acquired a volume lock")
	}
}

func TestHistoricalDeploymentFailureDoesNotBlockRepair(t *testing.T) {
	f := newFakeAPI(t)
	b := fakeBackend(t, f, nil)
	ids := importFake(t, b, runtimeSpec("runtime", "")).Services
	deployFake(t, b, "runtime", "")
	f.mu.Lock()
	a := f.apps[ids["runtime"]]
	a.deployments = append(a.deployments, &fakeDeployment{id: "historical-failure", status: "error"})
	f.mu.Unlock()
	removeFakeService(f, ids["runtime"])
	observeRestored(t, b, false)
	if fakeCalls(f, "application.reload") != 1 {
		t.Fatal("historical failure blocked repair after a successful deployment")
	}
}
