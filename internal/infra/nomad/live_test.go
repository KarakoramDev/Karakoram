// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build e2e

package nomad

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/contracttest"
)

func requireLive(t *testing.T) {
	t.Helper()
	for _, key := range []string{"NOMAD_ADDR", "NOMAD_TOKEN", "NOMAD_NAMESPACE", "NOMAD_ARTIFACT_BIND"} {
		if os.Getenv(key) == "" {
			t.Skip("NOMAD_ADDR, NOMAD_TOKEN, NOMAD_NAMESPACE and NOMAD_ARTIFACT_BIND are required")
		}
	}
}
func liveBackend(t *testing.T, artifactURL string) *Backend {
	t.Helper()
	requireLive(t)
	getenv := func(key string) string { return os.Getenv(strings.Replace(key, "KARAKORAM_NOMAD_", "NOMAD_", 1)) }
	factory := newFactory(getenv, WithAllowedAddrs(os.Getenv("NOMAD_ADDR")), WithArtifactURL(artifactURL))
	target, err := factory.TargetFromEnv(getenv)
	if err != nil {
		t.Fatal(err)
	}
	backend, _, err := factory.Validate(t.Context(), target)
	if err != nil {
		t.Fatal(err)
	}
	return backend.(*Backend)
}
func livePrefix(t *testing.T) string {
	t.Helper()
	var random [5]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	return "kknt" + hex.EncodeToString(random[:])
}
func liveAwait(ctx context.Context, b *Backend, op infra.Op) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		state, err := b.OpStatus(ctx, op)
		if err != nil {
			return err
		}
		if state.Failed {
			return errors.New("live operation failed: " + state.Message)
		}
		if state.Done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func liveComplete(t *testing.T, b *Backend, op infra.Op, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	if err = liveAwait(ctx, b, op); err != nil {
		t.Fatal(err)
	}
}
func liveCleanup(t *testing.T, b *Backend, names []string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()
		for _, name := range names {
			op, err := b.DeleteService(ctx, name)
			if err == nil {
				err = liveAwait(ctx, b, op)
			}
			if err != nil {
				t.Errorf("cleanup %s: %v", name, err)
			}
		}
	})
}

// liveArtifact serves the exact archive supplied to Deploy on an operator-chosen,
// client-reachable address. The Linux binary keeps its writer lock and counters
// on the mounted volume, so replacement allocations can prove non-overlap.
func liveArtifact(t *testing.T) ([]byte, string) {
	t.Helper()
	requireLive(t)
	dir := t.TempDir()
	source := filepath.Join(dir, "main.go")
	code := `package main
import("context";"encoding/json";"fmt";"net";"net/http";"os";"os/exec";"os/signal";"path/filepath";"strconv";"syscall";"time")
type marker struct{Starts int;Stops int;Overlaps int}
type security struct{UID int;SudoMode uint32;SudoersInaccessible bool;SudoWorks bool;PrepareSucceeded bool;SelfAtStart []string;Peer []string}
func main(){
 for _,arg:=range os.Args[1:]{if arg=="check"||arg=="readiness"{c:=http.Client{Timeout:time.Second};r,e:=c.Get("http://127.0.0.1:8080/");if e!=nil{os.Exit(1)};r.Body.Close();if r.StatusCode!=200{os.Exit(1)};return}}
 selfAtStart,_:=net.LookupHost(os.Getenv("NOMAD_JOB_ID"))
 http.HandleFunc("/security",func(w http.ResponseWriter,r *http.Request){
  s:=security{UID:os.Getuid(),SelfAtStart:selfAtStart}
  if info,e:=os.Stat("/usr/bin/sudo");e==nil{s.SudoMode=uint32(info.Mode().Perm());if info.Mode()&os.ModeSetuid!=0{s.SudoMode|=04000}}
  _,e:=os.Stat("/etc/sudoers.d/karakoram");s.SudoersInaccessible=os.IsNotExist(e)||os.IsPermission(e)
  s.SudoWorks=exec.Command("/usr/bin/sudo","-n","true").Run()==nil
  _,e=os.Stat("/opt/karakoram/.live-security-prepare");s.PrepareSucceeded=e==nil
  if peer:=r.URL.Query().Get("peer");peer!=""{ctx,cancel:=context.WithTimeout(r.Context(),time.Second);defer cancel();s.Peer,_=net.DefaultResolver.LookupHost(ctx,peer)}
  json.NewEncoder(w).Encode(s)
 })
 dir:=os.Getenv("KARAKORAM_DATA_DIR");if dir==""{dir="."};if os.MkdirAll(dir,0755)!=nil{panic("volume directory unavailable")}
 lock,e:=os.OpenFile(filepath.Join(dir,"writer.lock"),os.O_CREATE|os.O_RDWR,0600);if e!=nil{panic("writer lock unavailable")};defer lock.Close()
 overlap:=filepath.Join(dir,"overlap");if syscall.Flock(int(lock.Fd()),syscall.LOCK_EX|syscall.LOCK_NB)!=nil{os.WriteFile(overlap,[]byte("overlap"),0600);fmt.Println("nomad-live overlap detected");os.Exit(71)}
 markerPath:=filepath.Join(dir,"writer-marker.json");m:=marker{};if data,e:=os.ReadFile(markerPath);e==nil{if json.Unmarshal(data,&m)!=nil{panic("invalid writer marker")}};if _,e:=os.Stat(overlap);e==nil{m.Overlaps++}
 save:=func(){data,e:=json.Marshal(m);if e!=nil||os.WriteFile(markerPath,data,0600)!=nil{panic("writer marker unavailable")};fmt.Printf("nomad-live marker starts=%d stops=%d overlaps=%d\n",m.Starts,m.Stops,m.Overlaps)}
 m.Starts++;save();if m.Overlaps>0{os.Exit(72)}
 signals:=make(chan os.Signal,1);signal.Notify(signals,syscall.SIGTERM,syscall.SIGINT)
 go func(){<-signals;m.Stops++;save();syscall.Flock(int(lock.Fd()),syscall.LOCK_UN);os.Exit(0)}()
 http.HandleFunc("/",func(w http.ResponseWriter,r *http.Request){fmt.Fprintln(w,"ready "+strconv.Itoa(m.Starts))});fmt.Println("contract runtime ready");if http.ListenAndServe(":8080",nil)!=nil{panic("HTTP listener failed")}
}`
	if err := os.WriteFile(source, []byte(code), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(dir, "karakoram")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", binary, source)
	for _, entry := range os.Environ() {
		key := strings.SplitN(entry, "=", 2)[0]
		if strings.Contains(key, "TOKEN") || strings.Contains(key, "KEY") || strings.Contains(key, "SECRET") || strings.Contains(key, "PASSWORD") || strings.HasPrefix(key, "NOMAD_") || strings.HasPrefix(key, "KARAKORAM_") || key == "CGO_ENABLED" || key == "GOOS" || key == "GOARCH" {
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	cmd.Env = append(cmd.Env, "CGO_ENABLED=0", "GOOS=linux", "GOARCH=amd64")
	if _, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("building live artifact failed")
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	if err = tw.WriteHeader(&tar.Header{Name: "karakoram", Mode: 0755, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err = tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err = gz.Close(); err != nil {
		t.Fatal(err)
	}
	host, _, err := net.SplitHostPort(os.Getenv("NOMAD_ARTIFACT_BIND"))
	if err != nil {
		t.Fatal("NOMAD_ARTIFACT_BIND must be host:port")
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		t.Fatal("NOMAD_ARTIFACT_BIND must name an address reachable by Nomad clients, not a wildcard")
	}
	listener, err := net.Listen("tcp", os.Getenv("NOMAD_ARTIFACT_BIND"))
	if err != nil {
		t.Fatal("listening on NOMAD_ARTIFACT_BIND failed")
	}
	body := archive.Bytes()
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return body, "http://" + net.JoinHostPort(host, port) + "/karakoram.tar.gz"
}
func liveDeployment(t *testing.T, b *Backend, archive []byte, volume string) infra.Deployment {
	t.Helper()
	return infra.Deployment{Artifact: testArtifact(archive), Runtime: infra.RuntimeSpec{Base: infra.BaseUbuntu2404, Args: []string{"agent"}, Volume: volume, Env: map[string]string{"KARAKORAM_DATA_DIR": b.Layout().VolumePath(volume)}, Ports: []infra.Port{{Port: 8080, Protocol: "tcp", HTTP: true}}, StopBeforeStart: true}}
}

func TestLiveContract(t *testing.T) {
	archive, source := liveArtifact(t)
	contracttest.Run(t, func(t *testing.T) contracttest.Harness {
		b := liveBackend(t, source)
		prefix := livePrefix(t)
		liveCleanup(t, b, []string{prefix + "runtime", prefix + "volume"})
		return contracttest.Harness{Backend: b, Hostname: func(s string) string { return prefix + s }, Artifact: func() infra.Artifact { return testArtifact(archive) }, PollInterval: time.Second, Timeout: 12 * time.Minute}
	})
}
func TestLivePlacement(t *testing.T) {
	archive, source := liveArtifact(t)
	b := liveBackend(t, source)
	nodes, err := b.nodes(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	eligible := 0
	for _, n := range nodes {
		if n.Status == "ready" && n.SchedulingEligibility == "eligible" && !n.Drain {
			eligible++
		}
	}
	if eligible < 3 {
		t.Skip("three eligible Nomad clients are required")
	}
	prefix := livePrefix(t)
	var specs []infra.ServiceSpec
	var names, volumes []string
	for i := range 3 {
		name := fmt.Sprintf("%s%d", prefix, i)
		volume := name + "v"
		v := volumeSpec(volume)
		v.SpreadGroup = prefix
		r := runtimeSpec(name, volume)
		r.Resources = infra.Resources{MinRAM: 0.125, MaxRAM: 0.25}
		specs = append(specs, v, r)
		names = append(names, name)
		volumes = append(volumes, volume)
	}
	liveCleanup(t, b, append(append([]string{}, names...), volumes...))
	op, err := b.ImportServices(t.Context(), specs)
	liveComplete(t, b, op, err)
	placed := map[string]bool{}
	for i, name := range names {
		op, err = b.Deploy(t.Context(), name, liveDeployment(t, b, archive, volumes[i]))
		liveComplete(t, b, op, err)
		containers, err := b.Containers(t.Context(), name)
		if err != nil || len(containers) != 1 {
			t.Fatalf("placement containers: %+v %v", containers, err)
		}
		node := containers[0].PlacementGroup
		if node == "" || placed[node] {
			t.Fatalf("volume runtimes not distinct: %q", node)
		}
		placed[node] = true
	}
}
func awaitLiveMarker(t *testing.T, b *Backend, name string, starts int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		lines, err := b.Logs(ctx, name, infra.LogQuery{Limit: 100})
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range lines {
			if strings.Contains(line.Message, "overlap detected") {
				t.Fatal("live runtime observed simultaneous volume writers")
			}
			var gotStarts, stops, overlaps int
			if _, err := fmt.Sscanf(line.Message, "nomad-live marker starts=%d stops=%d overlaps=%d", &gotStarts, &stops, &overlaps); err == nil {
				if overlaps != 0 {
					t.Fatal("persistent marker recorded writer overlap")
				}
				if gotStarts >= starts && stops >= starts-1 {
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("live runtime did not report persistent start/stop marker")
		case <-ticker.C:
		}
	}
}
func TestLiveStopBeforeStart(t *testing.T) {
	archive, source := liveArtifact(t)
	b := liveBackend(t, source)
	prefix := livePrefix(t)
	name, volume := prefix+"runtime", prefix+"volume"
	liveCleanup(t, b, []string{name, volume})
	op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{volumeSpec(volume), runtimeSpec(name, volume)})
	liveComplete(t, b, op, err)
	for starts := 1; starts <= 2; starts++ {
		op, err = b.Deploy(t.Context(), name, liveDeployment(t, b, archive, volume))
		liveComplete(t, b, op, err)
		awaitLiveMarker(t, b, name, starts)
	}
}

// TestLiveRuntimeSecurity checks privileges and discovery in real allocations,
// including an allocation-local task restart after privileged preparation.
func TestLiveRuntimeSecurity(t *testing.T) {
	archive, source := liveArtifact(t)
	b := liveBackend(t, source)
	prefix := livePrefix(t)
	name, volume, peer, peerVolume := prefix+"security", prefix+"securityv", prefix+"peer", prefix+"peerv"
	liveCleanup(t, b, []string{name, peer, volume, peerVolume})
	specs := []infra.ServiceSpec{volumeSpec(volume), runtimeSpec(name, volume), volumeSpec(peerVolume), runtimeSpec(peer, peerVolume)}
	for i := range specs {
		if specs[i].Kind == infra.KindRuntime {
			specs[i].Resources = infra.Resources{MinCPU: 0.1, MaxCPU: 0.1, MinRAM: 0.0625, MaxRAM: 0.125}
		}
	}
	op, err := b.ImportServices(t.Context(), specs)
	liveComplete(t, b, op, err)
	d := liveDeployment(t, b, archive, volume)
	d.Runtime.Prepare = []string{"sudo -n true; touch /opt/karakoram/.live-security-prepare"}
	op, err = b.Deploy(t.Context(), name, d)
	liveComplete(t, b, op, err)
	awaitLiveMarker(t, b, name, 1)
	containers, err := b.Containers(t.Context(), name)
	if err != nil || len(containers) != 1 {
		t.Fatalf("security containers: %+v %v", containers, err)
	}
	original := containers[0]
	var job struct {
		TaskGroups []struct {
			Networks []struct {
				DynamicPorts  []struct{ Value, To int }
				ReservedPorts []struct{ Value, To int }
			}
			Services []struct{ Name, AddressMode, PortLabel string }
		}
	}
	if _, err := b.c.get(t.Context(), "/v1/job/"+name, nil, &job); err != nil {
		t.Fatal(err)
	}
	if len(job.TaskGroups) != 1 || len(job.TaskGroups[0].Services) != 1 {
		t.Fatal("missing native security service")
	}
	service := job.TaskGroups[0].Services[0]
	if service.Name != name || service.AddressMode != "alloc" || service.PortLabel != "9443" {
		t.Fatalf("native service is not numeric allocation discovery: %+v", service)
	}
	for _, network := range job.TaskGroups[0].Networks {
		if len(network.DynamicPorts) != 0 {
			t.Fatal("private fixture published a dynamic host port")
		}
		for _, port := range network.ReservedPorts {
			if port.To == 9443 || port.Value == 9443 {
				t.Fatal("private agent port was published on the host")
			}
		}
	}
	var state struct {
		UID                                              int
		SudoMode                                         uint32
		SudoersInaccessible, SudoWorks, PrepareSucceeded bool
		SelfAtStart, Peer                                []string
	}
	client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	check := func(peerName string) bool {
		t.Helper()
		response, err := client.Get("http://" + net.JoinHostPort(original.IPv4, "8080") + "/security?peer=" + peerName)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		state.Peer = nil
		if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&state) != nil {
			t.Fatal("invalid live security response")
		}
		if state.UID != 10001 || state.SudoMode != 0755 || !state.SudoersInaccessible || state.SudoWorks || !state.PrepareSucceeded || len(state.SelfAtStart) == 0 {
			t.Fatalf("allocation security boundary failed: %+v", state)
		}
		return len(state.Peer) != 0
	}
	if check(peer) {
		t.Fatal("undeployed peer unexpectedly resolved")
	}
	t.Log("Prepared with sudo; runtime uid10001, sudo0755 denied, sudoers inaccessible; self resolved at startup; no host port mapping")
	op, err = b.Deploy(t.Context(), peer, liveDeployment(t, b, archive, peerVolume))
	liveComplete(t, b, op, err)
	deadline := time.Now().Add(time.Minute)
	for !check(peer) {
		if time.Now().After(deadline) {
			t.Fatal("watched hosts did not discover the newly deployed peer")
		}
		time.Sleep(time.Second)
	}
	containers, err = b.Containers(t.Context(), name)
	if err != nil || len(containers) != 1 || containers[0].ID != original.ID {
		t.Fatal("hosts update replaced the observing allocation")
	}
	t.Log("Watched hosts resolved newly deployed peer without replacing the observer")
	op, err = b.Restart(t.Context(), name)
	liveComplete(t, b, op, err)
	awaitLiveMarker(t, b, name, 2)
	check(peer)
	containers, err = b.Containers(t.Context(), name)
	if err != nil || len(containers) != 1 || containers[0].ID != original.ID {
		t.Fatal("task restart unexpectedly replaced the security allocation")
	}
	t.Log("Same-allocation restart retained sudo denial, preparation marker, self discovery and zero writer overlaps")
}
