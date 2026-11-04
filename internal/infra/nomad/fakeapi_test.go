// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

// This fixture speaks Nomad's HTTP wire format independently of backend types.
// A tick runs BEFORE every request (including reads), or explicitly with tick().
// setAutoTick(false) disables automatic ticks. Evaluations and volumes converge
// after one tick; allocations start after two by default, and stop after one.
// A destructive registration places replacements immediately, before asking old
// clients to stop, as an ordinary rolling update can. The transient double claim
// is recorded even if the next request observes the old allocation already dead.
// Tests requiring a single writer must deregister and await terminal allocations
// before registering replacements, rather than relying on this scheduler to fence.
// Canary jobs retain their predecessors until all replacement tasks are healthy.
// This models the availability/health boundary, not Nomad's serial scheduling.

import (
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const fakeNomadToken = "test-nomad-token"
const fakeNomadManagementToken = "test-nomad-management-token"

type fakeRow = map[string]any

type fakeToken struct {
	accessor   string
	management bool
	namespaces map[string]bool
}

type fakeNode struct {
	ID, Name, Datacenter, NodePool, Status, SchedulingEligibility, Address string
	Drain                                                                  bool
}

type fakeRequest struct {
	Method, Path string
	Query        url.Values // Only known, non-secret query parameters are retained.
}

type fakeJob struct {
	row         fakeRow
	evaluations []string
	deployment  fakeRow
	previous    []*fakeAlloc
}

type fakeAlloc struct {
	row             fakeRow
	volumes         []string
	startAt, stopAt int
	fail            bool
	logs            map[string]string
}

type fakeNomad struct {
	mu                 sync.Mutex
	latency            time.Duration
	projectAllocations bool
	requestHook        func(http.ResponseWriter, *http.Request) (bool, func())
	*httptest.Server
	serial, clock int
	index         uint64
	autoTick      bool
	startTicks    int
	stall         bool
	tokens        map[string]fakeToken
	namespaces    map[string]fakeRow
	nodes         map[string]fakeNode
	jobs          map[string]*fakeJob
	allocs        map[string]*fakeAlloc
	evals         map[string]fakeRow
	volumes       map[string]fakeRow
	variables     map[string]fakeRow
	failNext      map[string]bool
	calls         []fakeRequest
	overlaps      []string
	seenOverlaps  map[string]bool
}

func newFakeNomad(t *testing.T) *fakeNomad {
	t.Helper()
	f := &fakeNomad{
		index: 1, autoTick: true, startTicks: 2,
		tokens: map[string]fakeToken{}, namespaces: map[string]fakeRow{}, nodes: map[string]fakeNode{},
		jobs: map[string]*fakeJob{}, allocs: map[string]*fakeAlloc{}, evals: map[string]fakeRow{},
		volumes: map[string]fakeRow{}, variables: map[string]fakeRow{}, failNext: map[string]bool{}, seenOverlaps: map[string]bool{},
	}
	f.addNamespace("default")
	f.addToken(fakeNomadToken, false, "default")
	f.addToken(fakeNomadManagementToken, true)
	for _, name := range []string{"node-a", "node-b", "node-c"} {
		f.addNode(fakeNode{ID: name, Name: name})
	}
	f.Server = httptest.NewTLSServer(http.HandlerFunc(f.serveHTTP))
	t.Cleanup(f.Close)
	return f
}

// CA returns the PEM trust anchor for constructing the backend's own transport.
func (f *fakeNomad) CA() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: f.Certificate().Raw})
}

func (f *fakeNomad) certPool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(f.Certificate())
	return pool
}

func (f *fakeNomad) addNamespace(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.namespaces[name] = fakeRow{"Name": name, "Description": "Test namespace", "Capabilities": fakeRow{"EnabledTaskDrivers": []string{"exec", "raw_exec", "docker"}}}
}

func (f *fakeNomad) addToken(secret string, management bool, namespaces ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	token := fakeToken{management: management, namespaces: map[string]bool{}}
	token.accessor = f.tokens[secret].accessor
	if token.accessor == "" {
		token.accessor = f.nextID()
	}
	for _, ns := range namespaces {
		token.namespaces[ns] = true
	}
	f.tokens[secret] = token
}

func (f *fakeNomad) addNode(node fakeNode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if node.ID == "" {
		node.ID = node.Name
	}
	if node.Name == "" {
		node.Name = node.ID
	}
	if node.Datacenter == "" {
		node.Datacenter = "dc1"
	}
	if node.NodePool == "" {
		node.NodePool = "default"
	}
	if node.Status == "" {
		node.Status = "ready"
	}
	if node.SchedulingEligibility == "" {
		node.SchedulingEligibility = "eligible"
	}
	if node.Address == "" {
		node.Address = "192.0.2.10"
	}
	f.nodes[node.ID] = node
}

func (f *fakeNomad) setNodeDown(id string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	node := f.nodes[id]
	if down {
		node.Status = "down"
	} else {
		node.Status = "ready"
	}
	f.nodes[id] = node
	if down {
		for _, a := range f.allocs {
			if fakeString(a.row, "NodeID") == id && !fakeTerminal(a) {
				f.setAllocStatus(a, "lost")
			}
		}
	} else {
		f.retryBlocked()
	}
}

func (f *fakeNomad) setNodeEligible(id string, eligible bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	node := f.nodes[id]
	node.SchedulingEligibility = "ineligible"
	if eligible {
		node.SchedulingEligibility = "eligible"
	}
	f.nodes[id] = node
	if eligible {
		f.retryBlocked()
	}
}

// Node/capacity recovery wakes Nomad's blocked evaluations without a new job
// registration. Keep the original evaluation's historical failure diagnostics.
func (f *fakeNomad) retryBlocked() {
	var blocked []fakeRow
	for _, evaluation := range f.evals {
		if evaluation["Status"] == "blocked" {
			blocked = append(blocked, evaluation)
		}
	}
	for _, evaluation := range blocked {
		job := f.jobs[fakeKey(fakeString(evaluation, "Namespace"), fakeString(evaluation, "JobID"))]
		if job == nil || job.row["Stop"] == true || fakeInt(job.row, "JobModifyIndex") != fakeInt(evaluation, "JobModifyIndex") {
			continue
		}
		evaluation["Status"] = "pending"
		f.schedule(job, evaluation)
	}
}

func (f *fakeNomad) setAutoTick(enabled bool) { f.mu.Lock(); defer f.mu.Unlock(); f.autoTick = enabled }
func (f *fakeNomad) setAllocStartTicks(ticks int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startTicks = max(1, ticks)
}
func (f *fakeNomad) stallAllocs(stall bool) { f.mu.Lock(); defer f.mu.Unlock(); f.stall = stall }
func (f *fakeNomad) failNextAlloc(jobID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failNext[jobID] = true
}

// appendLogs appends verbatim text to one task's stdout or stderr stream.
func (f *fakeNomad) appendLogs(allocID, task, stream, text string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if a := f.allocs[allocID]; a != nil {
		a.logs[task+"/"+stream] += text
	}
}

func (f *fakeNomad) requests() []fakeRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakeRequest, len(f.calls))
	for i, call := range f.calls {
		out[i] = fakeRequest{Method: call.Method, Path: call.Path, Query: url.Values{}}
		for k, v := range call.Query {
			out[i].Query[k] = slices.Clone(v)
		}
	}
	return out
}

func (f *fakeNomad) writerOverlaps() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.overlaps)
}

func (f *fakeNomad) nextID() string {
	f.serial++
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", f.serial)
}

func (f *fakeNomad) nextIndex() uint64 { f.index++; return f.index }
func (f *fakeNomad) now() int64 {
	return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() + int64(f.clock)*int64(time.Second)
}
func fakeKey(ns, id string) string              { return ns + "\x00" + id }
func fakeString(row fakeRow, key string) string { s, _ := row[key].(string); return s }
func fakeInt(row fakeRow, key string) int {
	switch n := row[key].(type) {
	case float64:
		return int(n)
	case int:
		return n
	case uint64:
		return int(n)
	case int64:
		return int(n)
	}
	return 0
}
func fakeMap(v any) fakeRow { row, _ := v.(map[string]any); return row }
func fakeRows(v any) []fakeRow {
	rows := []fakeRow{}
	if values, ok := v.([]any); ok {
		for _, value := range values {
			if row := fakeMap(value); row != nil {
				rows = append(rows, row)
			}
		}
	}
	return rows
}
func fakeCopy(row fakeRow) fakeRow {
	data, _ := json.Marshal(row)
	var out fakeRow
	_ = json.Unmarshal(data, &out)
	return out
}
func fakeTerminal(a *fakeAlloc) bool {
	switch fakeString(a.row, "ClientStatus") {
	case "complete", "failed", "lost":
		return true
	}
	return false
}
func fakeNodeRow(node fakeNode) fakeRow {
	return fakeRow{"ID": node.ID, "Name": node.Name, "Datacenter": node.Datacenter, "NodePool": node.NodePool, "Status": node.Status, "SchedulingEligibility": node.SchedulingEligibility, "Drain": node.Drain, "Address": node.Address}
}
func fakeSorted(rows []fakeRow, key string) []fakeRow {
	slices.SortFunc(rows, func(a, b fakeRow) int { return strings.Compare(fakeString(a, key), fakeString(b, key)) })
	return rows
}
func fakeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fakeDecode(w http.ResponseWriter, r *http.Request) fakeRow {
	var row fakeRow
	if err := json.NewDecoder(r.Body).Decode(&row); err != nil || row == nil {
		http.Error(w, "Invalid JSON request", http.StatusBadRequest)
		return nil
	}
	return row
}

func (f *fakeNomad) tick() { f.mu.Lock(); defer f.mu.Unlock(); f.tickLocked() }
func (f *fakeNomad) tickLocked() {
	f.clock++
	for _, v := range f.volumes {
		if v["State"] == "pending" {
			v["State"] = "ready"
			v["ModifyIndex"] = f.nextIndex()
		}
	}
	for _, e := range f.evals {
		if e["Status"] == "pending" {
			e["Status"] = "complete"
			e["ModifyIndex"] = f.nextIndex()
		}
	}
	for _, a := range f.allocs {
		if fakeTerminal(a) {
			continue
		}
		if a.stopAt > 0 && f.clock >= a.stopAt {
			f.setAllocStatus(a, "complete")
			continue
		}
		if a.row["ClientStatus"] == "pending" && !f.stall && f.clock >= a.startAt {
			if a.fail {
				f.setAllocStatus(a, "failed")
			} else {
				f.setAllocStatus(a, "running")
			}
		}
	}
	for _, job := range f.jobs {
		f.updateJobStatus(job)
	}
	f.checkOverlaps()
}

func (f *fakeNomad) setAllocStatus(a *fakeAlloc, status string) {
	a.row["ClientStatus"] = status
	a.row["ModifyTime"] = f.now()
	a.row["ModifyIndex"] = f.nextIndex()
	a.row["DeploymentStatus"] = fakeRow{"Healthy": status == "running"}
	for _, value := range fakeMap(a.row["TaskStates"]) {
		state := fakeMap(value)
		state["State"] = "dead"
		if status == "running" {
			state["State"] = "running"
			state["StartedAt"] = time.Unix(0, f.now()).UTC().Format(time.RFC3339Nano)
		}
		state["Failed"] = status == "failed" || status == "lost"
		state["Events"] = append(state["Events"].([]any), fakeRow{"Type": status, "Time": f.now(), "FailsTask": state["Failed"], "DisplayMessage": "Allocation " + status})
		if fakeTerminal(a) {
			state["FinishedAt"] = time.Unix(0, f.now()).UTC().Format(time.RFC3339Nano)
		}
	}
}

func (f *fakeNomad) updateJobStatus(job *fakeJob) {
	status := "dead"
	for _, a := range f.allocs {
		if a.row["JobID"] == job.row["ID"] && a.row["Namespace"] == job.row["Namespace"] && !fakeTerminal(a) {
			if a.row["ClientStatus"] == "running" {
				status = "running"
				break
			}
			status = "pending"
		}
	}
	if status == "dead" && job.row["Stop"] != true {
		status = "pending"
	}
	job.row["Status"] = status
	if job.deployment != nil {
		running, desired, failed := 0, 0, false
		for _, group := range fakeRows(job.row["TaskGroups"]) {
			desired += fakeInt(group, "Count")
		}
		for _, a := range f.allocs {
			retained := len(fakeRows(job.row["TaskGroups"])) == 1 &&
				fakeInt(fakeMap(fakeRows(job.row["TaskGroups"])[0]["Update"]), "Canary") == 0 &&
				sameScaleSpecification(fakeRawJob(job.row), fakeRawJob(fakeMap(a.row["Job"]))) &&
				reflect.DeepEqual(fakeCombinedTaskMeta(job.row), fakeCombinedTaskMeta(fakeMap(a.row["Job"])))
			if a.row["JobID"] != job.row["ID"] || a.row["Namespace"] != job.row["Namespace"] || a.row["JobVersion"] != job.row["Version"] && !retained {
				continue
			}
			if a.row["ClientStatus"] == "running" && a.row["DesiredStatus"] == "run" {
				running++
			}
			failed = failed || a.row["ClientStatus"] == "failed"
		}
		job.deployment["Status"] = "running"
		if running >= desired {
			job.deployment["Status"] = "successful"
			for _, a := range job.previous {
				f.stopAlloc(a)
			}
			job.previous = nil
		} else if failed {
			job.deployment["Status"] = "failed"
		}
	}
}

func (f *fakeNomad) stopAlloc(a *fakeAlloc) {
	if fakeTerminal(a) {
		return
	}
	a.row["DesiredStatus"] = "stop"
	a.stopAt = f.clock + 1
	a.row["ModifyTime"] = f.now()
}

func (f *fakeNomad) checkOverlaps() {
	claims := map[string][]string{}
	for id, a := range f.allocs {
		if fakeTerminal(a) || a.row["DesiredStatus"] != "run" {
			continue
		}
		for _, volumeID := range a.volumes {
			v := f.volumes[volumeID]
			if v == nil {
				continue
			}
			for _, cap := range fakeRows(v["RequestedCapabilities"]) {
				if cap["AccessMode"] == "single-node-single-writer" {
					claims[volumeID] = append(claims[volumeID], id)
					break
				}
			}
		}
	}
	for volumeID, ids := range claims {
		slices.Sort(ids)
		for i := range ids {
			for j := i + 1; j < len(ids); j++ {
				key := volumeID + ":" + ids[i] + ":" + ids[j]
				if !f.seenOverlaps[key] {
					f.seenOverlaps[key] = true
					f.overlaps = append(f.overlaps, key)
				}
			}
		}
	}
}

func (f *fakeNomad) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	delay := f.latency
	hook := f.requestHook
	f.mu.Unlock()
	if hook != nil {
		handled, finish := hook(w, r)
		if finish != nil {
			defer finish()
		}
		if handled {
			return
		}
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	query := r.URL.Query()
	safe := url.Values{}
	for _, key := range []string{"namespace", "region", "prefix", "meta", "purge", "type", "task", "origin", "offset", "plain", "cas", "node_id", "plugin_id"} {
		if values, ok := query[key]; ok {
			safe[key] = slices.Clone(values)
		}
	}
	f.calls = append(f.calls, fakeRequest{Method: r.Method, Path: r.URL.Path, Query: safe})
	if f.autoTick {
		f.tickLocked()
	}
	w.Header().Set("X-Nomad-Index", strconv.FormatUint(f.index, 10))
	w.Header().Set("X-Nomad-KnownLeader", "true")
	w.Header().Set("X-Nomad-LastContact", "0")
	token, ok := f.tokens[r.Header.Get("X-Nomad-Token")]
	if !ok {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}
	ns := query.Get("namespace")
	if ns == "" {
		ns = "default"
	}
	if !token.management && !token.namespaces[ns] {
		http.Error(w, "Permission denied", http.StatusForbidden)
		return
	}
	path := r.URL.Path
	if path == "/v1/acl/token/self" && r.Method == http.MethodGet {
		typ := "client"
		if token.management {
			typ = "management"
		}
		fakeJSON(w, 200, fakeRow{"AccessorID": token.accessor, "SecretID": r.Header.Get("X-Nomad-Token"), "Name": "Test token", "Type": typ, "Policies": []string{"test-policy"}, "Global": false})
		return
	}
	if path == "/v1/namespaces" && r.Method == http.MethodGet {
		rows := []fakeRow{}
		for name, row := range f.namespaces {
			if token.management || token.namespaces[name] {
				rows = append(rows, row)
			}
		}
		fakeJSON(w, 200, fakeSorted(rows, "Name"))
		return
	}
	if strings.HasPrefix(path, "/v1/namespace/") && r.Method == http.MethodGet {
		name := strings.TrimPrefix(path, "/v1/namespace/")
		if !token.management && !token.namespaces[name] {
			http.Error(w, "Permission denied", http.StatusForbidden)
			return
		}
		if row := f.namespaces[name]; row != nil {
			fakeJSON(w, 200, row)
		} else {
			http.NotFound(w, r)
		}
		return
	}
	if path == "/v1/nodes" && r.Method == http.MethodGet {
		rows := []fakeRow{}
		for _, node := range f.nodes {
			rows = append(rows, fakeNodeRow(node))
		}
		fakeJSON(w, 200, fakeSorted(rows, "ID"))
		return
	}
	if strings.HasPrefix(path, "/v1/node/") && r.Method == http.MethodGet {
		if node, ok := f.nodes[strings.TrimPrefix(path, "/v1/node/")]; ok {
			fakeJSON(w, 200, fakeNodeRow(node))
		} else {
			http.NotFound(w, r)
		}
		return
	}
	if strings.HasPrefix(path, "/v1/job/") && strings.HasSuffix(path, "/plan") && r.Method == http.MethodPost {
		body := fakeDecode(w, r)
		if body == nil {
			return
		}
		job := fakeMap(body["Job"])
		if fakeString(job, "ID") != strings.TrimSuffix(strings.TrimPrefix(path, "/v1/job/"), "/plan") || fakeString(job, "Namespace") != ns {
			http.Error(w, "Invalid plan job", 400)
			return
		}
		// The default fixture has unlimited CPU/RAM, but an unchanged task
		// spec is not a replacement-placement proof.
		replacements := 1
		if existing := f.jobs[fakeKey(ns, fakeString(job, "ID"))]; existing != nil {
			oldGroups, groups := fakeRows(existing.row["TaskGroups"]), fakeRows(job["TaskGroups"])
			if len(oldGroups) == 1 && len(groups) == 1 && reflect.DeepEqual(oldGroups[0]["Tasks"], groups[0]["Tasks"]) {
				replacements = 0
			}
		}
		fakeJSON(w, 200, fakeRow{"FailedTGAllocs": nil, "Annotations": fakeRow{"DesiredTGUpdates": fakeRow{"member": fakeRow{"DestructiveUpdate": replacements}}}})
		return
	}
	if path == "/v1/jobs" {
		switch r.Method {
		case http.MethodPut, http.MethodPost:
			f.register(w, r, ns)
			return
		case http.MethodGet:
			rows := []fakeRow{}
			for _, job := range f.jobs {
				if job.row["Namespace"] != ns || !strings.HasPrefix(fakeString(job.row, "ID"), query.Get("prefix")) {
					continue
				}
				row := fakeRow{}
				for _, key := range []string{"ID", "Name", "Namespace", "NodePool", "Type", "Priority", "Status", "Stop", "Version", "CreateIndex", "ModifyIndex", "JobModifyIndex"} {
					row[key] = job.row[key]
				}
				if query.Get("meta") == "true" {
					row["Meta"] = job.row["Meta"]
				}
				rows = append(rows, row)
			}
			fakeJSON(w, 200, fakeSorted(rows, "ID"))
			return
		}
	}
	if strings.HasPrefix(path, "/v1/job/") {
		f.jobEndpoint(w, r, ns)
		return
	}
	if strings.HasPrefix(path, "/v1/evaluation/") && r.Method == http.MethodGet {
		row := f.evals[strings.TrimPrefix(path, "/v1/evaluation/")]
		if row != nil && row["Namespace"] == ns {
			fakeJSON(w, 200, row)
		} else {
			http.NotFound(w, r)
		}
		return
	}
	if strings.HasPrefix(path, "/v1/allocation/") && r.Method == http.MethodGet {
		a := f.allocs[strings.TrimPrefix(path, "/v1/allocation/")]
		if a != nil && a.row["Namespace"] == ns {
			fakeJSON(w, 200, a.row)
		} else {
			http.NotFound(w, r)
		}
		return
	}
	if strings.HasPrefix(path, "/v1/client/allocation/") && strings.HasSuffix(path, "/restart") && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/v1/client/allocation/"), "/restart")
		a := f.allocs[id]
		if a == nil || a.row["Namespace"] != ns {
			http.NotFound(w, r)
			return
		}
		if fakeTerminal(a) {
			http.Error(w, "Allocation is terminal", 400)
			return
		}
		body := fakeDecode(w, r)
		if body == nil {
			return
		}
		task := fakeString(body, "TaskName")
		states := fakeMap(a.row["TaskStates"])
		if task != "" && states[task] == nil {
			http.Error(w, "Task not found", 404)
			return
		}
		for name, value := range states {
			if task != "" && task != name {
				continue
			}
			state := fakeMap(value)
			state["Restarts"] = fakeInt(state, "Restarts") + 1
			state["LastRestart"] = time.Unix(0, f.now()).UTC().Format(time.RFC3339Nano)
			state["Events"] = append(state["Events"].([]any), fakeRow{"Type": "Restarting", "Time": f.now(), "DisplayMessage": "Task restarting"})
		}
		fakeJSON(w, 200, fakeRow{})
		return
	}
	if strings.HasPrefix(path, "/v1/client/fs/logs/") && r.Method == http.MethodGet {
		f.logsEndpoint(w, r, ns)
		return
	}
	if path == "/v1/volume/host/create" && r.Method == http.MethodPut {
		f.createVolume(w, r, ns)
		return
	}
	if strings.HasPrefix(path, "/v1/volume/host/") {
		f.volumeEndpoint(w, r, ns)
		return
	}
	if path == "/v1/volumes" && r.Method == http.MethodGet {
		if query.Get("type") != "host" {
			http.Error(w, "Invalid volume type", 400)
			return
		}
		rows := []fakeRow{}
		for _, v := range f.volumes {
			if v["Namespace"] != ns || !strings.HasPrefix(fakeString(v, "ID"), query.Get("prefix")) || !strings.HasPrefix(fakeString(v, "NodeID"), query.Get("node_id")) || !strings.HasPrefix(fakeString(v, "PluginID"), query.Get("plugin_id")) {
				continue
			}
			row := fakeCopy(v)
			for _, key := range []string{"Parameters", "RequestedCapabilities", "Allocations"} {
				delete(row, key)
			}
			rows = append(rows, row)
		}
		fakeJSON(w, 200, fakeSorted(rows, "ID"))
		return
	}
	if strings.HasPrefix(path, "/v1/var/") {
		f.variableEndpoint(w, r, ns)
		return
	}
	if path == "/v1/vars" && r.Method == http.MethodGet {
		rows := []fakeRow{}
		for _, v := range f.variables {
			if v["Namespace"] == ns && strings.HasPrefix(fakeString(v, "Path"), query.Get("prefix")) {
				row := fakeCopy(v)
				delete(row, "Items")
				rows = append(rows, row)
			}
		}
		fakeJSON(w, 200, fakeSorted(rows, "Path"))
		return
	}
	http.NotFound(w, r)
}

func (f *fakeNomad) register(w http.ResponseWriter, r *http.Request, ns string) {
	body := fakeDecode(w, r)
	if body == nil {
		return
	}
	row := fakeMap(body["Job"])
	if row == nil || fakeString(row, "ID") == "" {
		http.Error(w, "Job ID must be specified", 400)
		return
	}
	if value := fakeString(row, "Namespace"); value != "" && value != ns {
		http.Error(w, "Job namespace does not match request", 400)
		return
	}
	if f.namespaces[ns] == nil {
		http.Error(w, "Namespace does not exist", 400)
		return
	}
	for _, group := range fakeRows(row["TaskGroups"]) {
		if update := fakeMap(group["Update"]); update != nil && update["AutoPromote"] == true && fakeInt(update, "Canary") == 0 {
			// Nomad 2.0 returns job validation failures as HTTP 500.
			http.Error(w, "Task group "+fakeString(group, "Name")+" validation failed: Auto Promote requires a Canary count greater than zero", http.StatusInternalServerError)
			return
		}
	}
	if fakeString(row, "NodePool") == "" {
		row["NodePool"] = "default"
	}
	id := fakeString(row, "ID")
	key := fakeKey(ns, id)
	old := f.jobs[key]
	if body["EnforceIndex"] == true {
		requested := fakeInt(body, "JobModifyIndex")
		message := ""
		switch {
		case old != nil && requested == 0:
			message = "Enforcing job modify index 0: job already exists"
		case old != nil && requested != fakeInt(old.row, "JobModifyIndex"):
			message = fmt.Sprintf("Enforcing job modify index %d: job exists with conflicting job modify index: %d", requested, fakeInt(old.row, "JobModifyIndex"))
		case old == nil && requested != 0:
			message = fmt.Sprintf("Enforcing job modify index %d: job does not exist", requested)
		}
		if message != "" {
			// Nomad 2.0 returns this uncoded RPC error as HTTP 500.
			http.Error(w, message, http.StatusInternalServerError)
			return
		}
	}
	version := 0
	createIndex := f.index + 1
	changed := true
	if old != nil {
		version = fakeInt(old.row, "Version") + 1
		createIndex = uint64(fakeInt(old.row, "CreateIndex"))
		changed = !reflect.DeepEqual(row["TaskGroups"], old.row["TaskGroups"]) || !reflect.DeepEqual(fakeCombinedTaskMeta(row), fakeCombinedTaskMeta(old.row)) || row["NodePool"] != old.row["NodePool"] || old.row["Stop"] == true
	}
	row["Namespace"] = ns
	if fakeString(row, "Name") == "" {
		row["Name"] = id
	}
	if fakeString(row, "Type") == "" {
		row["Type"] = "service"
	}
	if row["Priority"] == nil {
		row["Priority"] = 50
	}
	if row["Stop"] == nil {
		row["Stop"] = false
	}
	// Nomad overwrites this server-maintained field with the authenticated
	// registering token's accessor, even when a job body carries an older one.
	row["NomadTokenID"] = f.tokens[r.Header.Get("X-Nomad-Token")].accessor
	idx := f.nextIndex()
	row["Version"], row["CreateIndex"], row["ModifyIndex"], row["JobModifyIndex"] = version, createIndex, idx, idx
	job := &fakeJob{row: row}
	if old != nil {
		job.evaluations = old.evaluations
	}
	f.jobs[key] = job
	eval := f.newEval(ns, id, "job-register", idx)
	job.evaluations = append(job.evaluations, fakeString(eval, "ID"))
	// Count-only changes reconcile existing healthy allocations and schedule
	// only the deficit, even if the superseded canary was blocked.
	if old != nil && row["Stop"] != true && fakeRetainScale(f, old, job, eval) {
		f.updateJobStatus(job)
		fakeJSON(w, 200, fakeRow{"EvalID": eval["ID"], "JobModifyIndex": idx, "Index": f.index})
		return
	}
	if row["Stop"] == true {
		f.stopJobAllocs(ns, id)
	} else if changed {
		previous := []*fakeAlloc{}
		for _, a := range f.allocs {
			if a.row["Namespace"] == ns && a.row["JobID"] == id && !fakeTerminal(a) {
				previous = append(previous, a)
			}
		}
		f.schedule(job, eval)
		// Observe the replacement claim before old clients receive their stop.
		f.checkOverlaps()
		canary := false
		for _, group := range fakeRows(row["TaskGroups"]) {
			canary = canary || fakeInt(fakeMap(group["Update"]), "Canary") > 0
		}
		if canary {
			job.previous = previous
		} else {
			for _, a := range previous {
				f.stopAlloc(a)
			}
		}
	}
	f.updateJobStatus(job)
	fakeJSON(w, 200, fakeRow{"EvalID": eval["ID"], "EvalCreateIndex": eval["CreateIndex"], "JobModifyIndex": idx, "Index": f.index, "Warnings": ""})
}

func fakeRawJob(row fakeRow) map[string]json.RawMessage {
	data, _ := json.Marshal(row)
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(data, &raw)
	return raw
}

func fakeRetainScale(f *fakeNomad, old, job *fakeJob, eval fakeRow) bool {
	asRaw := fakeRawJob
	groups := fakeRows(job.row["TaskGroups"])
	if len(groups) != 1 || fakeInt(fakeMap(groups[0]["Update"]), "Canary") != 0 ||
		!sameScaleSpecification(asRaw(old.row), asRaw(job.row)) {
		return false
	}
	// Nomad compares CombinedTaskMeta for each allocation, not just the
	// current job's task groups. Job Meta changes are destructive too.
	count := fakeInt(groups[0], "Count")
	serving := []*fakeAlloc{}
	for _, a := range f.allocs {
		if a.row["Namespace"] == job.row["Namespace"] && a.row["JobID"] == job.row["ID"] &&
			a.row["DesiredStatus"] == "run" && a.row["ClientStatus"] == "running" &&
			sameScaleSpecification(asRaw(fakeMap(a.row["Job"])), asRaw(job.row)) &&
			reflect.DeepEqual(fakeCombinedTaskMeta(fakeMap(a.row["Job"])), fakeCombinedTaskMeta(job.row)) {
			serving = append(serving, a)
		}
	}
	if len(serving) < count {
		if len(serving) < fakeInt(fakeRows(old.row["TaskGroups"])[0], "Count") {
			return false
		}
		f.schedule(job, eval)
		return true
	}
	for _, a := range serving[count:] {
		f.stopAlloc(a)
	}
	for _, id := range old.evaluations {
		if e := f.evals[id]; e != nil && e["Status"] == "blocked" {
			e["Status"] = "cancelled"
		}
	}
	eval["Status"] = "complete"
	return true
}

// Task Meta overrides group Meta, which overrides job Meta, as in Nomad.
func fakeCombinedTaskMeta(job fakeRow) map[string]map[string]any {
	combined := map[string]map[string]any{}
	for _, group := range fakeRows(job["TaskGroups"]) {
		for _, task := range fakeRows(group["Tasks"]) {
			meta := map[string]any{}
			for _, scope := range []fakeRow{job, group, task} {
				for key, value := range fakeMap(scope["Meta"]) {
					meta[key] = value
				}
			}
			combined[fakeString(group, "Name")+"/"+fakeString(task, "Name")] = meta
		}
	}
	return combined
}

func (f *fakeNomad) newEval(ns, jobID, trigger string, modify uint64) fakeRow {
	id := f.nextID()
	idx := f.nextIndex()
	row := fakeRow{"ID": id, "Namespace": ns, "JobID": jobID, "Type": "service", "Priority": 50, "TriggeredBy": trigger, "Status": "pending", "JobModifyIndex": modify, "CreateIndex": idx, "ModifyIndex": idx, "CreateTime": f.now(), "ModifyTime": f.now(), "FailedTGAllocs": nil, "BlockedEval": ""}
	f.evals[id] = row
	return row
}

func (f *fakeNomad) stopJobAllocs(ns, id string) {
	for _, a := range f.allocs {
		if a.row["Namespace"] == ns && a.row["JobID"] == id {
			f.stopAlloc(a)
		}
	}
}

func (f *fakeNomad) schedule(job *fakeJob, eval fakeRow) {
	ns, jobID := fakeString(job.row, "Namespace"), fakeString(job.row, "ID")
	failed := fakeRow{}
	deploymentID := f.nextID()
	job.deployment = fakeRow{"ID": deploymentID, "Namespace": ns, "JobID": jobID, "JobVersion": job.row["Version"], "Status": "running", "TaskGroups": fakeRow{}}
	for _, group := range fakeRows(job.row["TaskGroups"]) {
		count := 1
		if group["Count"] != nil {
			count = fakeInt(group, "Count")
		}
		existing := 0
		for _, a := range f.allocs {
			sameVersion := a.row["JobVersion"] == job.row["Version"]
			retained := fakeInt(fakeMap(group["Update"]), "Canary") == 0 &&
				sameScaleSpecification(fakeRawJob(job.row), fakeRawJob(fakeMap(a.row["Job"]))) &&
				reflect.DeepEqual(fakeCombinedTaskMeta(job.row), fakeCombinedTaskMeta(fakeMap(a.row["Job"])))
			if a.row["Namespace"] == ns && a.row["JobID"] == jobID && (sameVersion || retained) && a.row["TaskGroup"] == group["Name"] && !fakeTerminal(a) {
				existing++
			}
		}
		count = max(0, count-existing)
		for i := range count {
			node, volumeIDs, ok := f.chooseNode(job.row, group)
			if !ok {
				failed[fakeString(group, "Name")] = fakeRow{"NodesEvaluated": len(f.nodes), "NodesFiltered": len(f.nodes), "ConstraintFiltered": fakeRow{"no feasible nodes or ready host volumes": len(f.nodes)}, "CoalescedFailures": count - i - 1}
				break
			}
			id := f.nextID()
			idx := f.nextIndex()
			states := fakeRow{}
			logs := map[string]string{}
			for _, task := range fakeRows(group["Tasks"]) {
				name := fakeString(task, "Name")
				states[name] = fakeRow{"State": "pending", "Failed": false, "Restarts": 0, "Events": []any{fakeRow{"Type": "Received", "Time": f.now(), "DisplayMessage": "Task received"}}}
				logs[name+"/stdout"] = "Task started\n"
				logs[name+"/stderr"] = ""
			}
			row := fakeRow{"ID": id, "Name": fmt.Sprintf("%s.%s[%d]", jobID, fakeString(group, "Name"), i), "Namespace": ns, "JobID": jobID, "JobVersion": job.row["Version"], "Job": fakeCopy(job.row), "TaskGroup": group["Name"], "NodeID": node.ID, "NodeName": node.Name, "EvalID": eval["ID"], "DeploymentID": deploymentID, "DesiredStatus": "run", "ClientStatus": "pending", "TaskStates": states, "CreateIndex": idx, "ModifyIndex": idx, "CreateTime": f.now(), "ModifyTime": f.now(), "AllocatedResources": fakeRow{"Shared": fakeRow{"Networks": []any{fakeRow{"IP": node.Address, "Mode": "host", "ReservedPorts": []any{}, "DynamicPorts": []any{}}}, "Ports": []any{}}}, "NetworkStatus": fakeRow{"Address": fmt.Sprintf("192.0.2.%d", 20+f.serial%200), "AddressIPv6": ""}}
			a := &fakeAlloc{row: row, volumes: volumeIDs, startAt: f.clock + f.startTicks, fail: f.failNext[jobID], logs: logs}
			delete(f.failNext, jobID)
			f.allocs[id] = a
		}
	}
	if len(failed) > 0 {
		eval["FailedTGAllocs"] = failed
		blocked := f.newEval(ns, jobID, "queued-allocs", uint64(fakeInt(job.row, "JobModifyIndex")))
		blocked["Status"] = "blocked"
		blocked["PreviousEval"] = eval["ID"]
		blocked["FailedTGAllocs"] = failed
		eval["BlockedEval"] = blocked["ID"]
		job.evaluations = append(job.evaluations, fakeString(blocked, "ID"))
	}
}

func (f *fakeNomad) chooseNode(job, group fakeRow) (fakeNode, []string, bool) {
	pinned := ""
	ids := []string{}
	for _, value := range fakeMap(group["Volumes"]) {
		request := fakeMap(value)
		if request["Type"] != "host" {
			continue
		}
		source := fakeString(request, "Source")
		var selected fakeRow
		for _, v := range f.volumes {
			if v["Namespace"] == job["Namespace"] && (v["ID"] == source || v["Name"] == source) {
				selected = v
				break
			}
		}
		if selected == nil || selected["State"] != "ready" {
			return fakeNode{}, nil, false
		}
		nodeID := fakeString(selected, "NodeID")
		if pinned != "" && pinned != nodeID {
			return fakeNode{}, nil, false
		}
		pinned = nodeID
		ids = append(ids, fakeString(selected, "ID"))
	}
	candidates := []fakeNode{}
	for _, node := range f.nodes {
		if node.Status != "ready" || node.SchedulingEligibility != "eligible" || node.Drain || node.NodePool != fakeString(job, "NodePool") || (pinned != "" && pinned != node.ID) {
			continue
		}
		if dcs, ok := job["Datacenters"].([]any); ok && len(dcs) > 0 {
			match := false
			for _, dc := range dcs {
				if dc == "*" || dc == node.Datacenter {
					match = true
				}
			}
			if !match {
				continue
			}
		}
		candidates = append(candidates, node)
	}
	load := func(id string) int {
		n := 0
		for _, a := range f.allocs {
			if a.row["NodeID"] == id && !fakeTerminal(a) {
				n++
			}
		}
		return n
	}
	slices.SortFunc(candidates, func(a, b fakeNode) int {
		if diff := load(a.ID) - load(b.ID); diff != 0 {
			return diff
		}
		return strings.Compare(a.Name, b.Name)
	})
	if len(candidates) == 0 {
		return fakeNode{}, nil, false
	}
	return candidates[0], ids, true
}

func (f *fakeNomad) jobEndpoint(w http.ResponseWriter, r *http.Request, ns string) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/job/"), "/")
	id := parts[0]
	job := f.jobs[fakeKey(ns, id)]
	if job == nil {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			fakeJSON(w, 200, job.row)
			return
		case http.MethodDelete:
			job.row["Stop"] = true
			f.stopJobAllocs(ns, id)
			idx := f.nextIndex()
			job.row["ModifyIndex"], job.row["JobModifyIndex"] = idx, idx
			eval := f.newEval(ns, id, "job-deregister", idx)
			job.evaluations = append(job.evaluations, fakeString(eval, "ID"))
			f.updateJobStatus(job)
			if r.URL.Query().Get("purge") == "true" {
				delete(f.jobs, fakeKey(ns, id))
			}
			fakeJSON(w, 200, fakeRow{"EvalID": eval["ID"], "EvalCreateIndex": eval["CreateIndex"], "JobModifyIndex": idx, "Index": f.index})
			return
		}
	}
	if len(parts) == 2 && r.Method == http.MethodGet {
		switch parts[1] {
		case "allocations":
			rows := []fakeRow{}
			for _, a := range f.allocs {
				if a.row["Namespace"] == ns && a.row["JobID"] == id {
					row := a.row
					if f.projectAllocations {
						row = maps.Clone(row)
						delete(row, "NetworkStatus")
					}
					rows = append(rows, row)
				}
			}
			fakeJSON(w, 200, fakeSorted(rows, "ID"))
			return
		case "evaluations":
			rows := []fakeRow{}
			for _, evalID := range job.evaluations {
				rows = append(rows, f.evals[evalID])
			}
			fakeJSON(w, 200, rows)
			return
		case "deployment":
			fakeJSON(w, 200, job.deployment)
			return
		}
	}
	http.NotFound(w, r)
}

func (f *fakeNomad) logsEndpoint(w http.ResponseWriter, r *http.Request, ns string) {
	a := f.allocs[strings.TrimPrefix(r.URL.Path, "/v1/client/fs/logs/")]
	if a == nil || a.row["Namespace"] != ns {
		http.NotFound(w, r)
		return
	}
	q := r.URL.Query()
	if q.Get("plain") != "true" {
		http.Error(w, "This fixture requires plain logs", 400)
		return
	}
	text, ok := a.logs[q.Get("task")+"/"+q.Get("type")]
	if !ok {
		http.Error(w, "Task or log type not found", 404)
		return
	}
	offset := int64(0)
	if q.Get("offset") != "" {
		var err error
		offset, err = strconv.ParseInt(q.Get("offset"), 10, 64)
		if err != nil || offset < 0 {
			http.Error(w, "Invalid offset", 400)
			return
		}
	}
	start := min(offset, int64(len(text)))
	if q.Get("origin") == "end" {
		start = max(0, int64(len(text))-offset)
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = io.WriteString(w, text[start:])
}

func (f *fakeNomad) createVolume(w http.ResponseWriter, r *http.Request, ns string) {
	body := fakeDecode(w, r)
	if body == nil {
		return
	}
	v := fakeMap(body["Volume"])
	if v == nil || fakeString(v, "Name") == "" || v["PluginID"] != "mkdir" {
		http.Error(w, "Invalid host volume", 400)
		return
	}
	if value := fakeString(v, "Namespace"); value != "" && value != ns {
		http.Error(w, "Volume namespace does not match request", 400)
		return
	}
	if f.namespaces[ns] == nil {
		http.Error(w, "Namespace does not exist", 400)
		return
	}
	for parameter := range fakeMap(v["Parameters"]) {
		if parameter != "mode" && parameter != "uid" && parameter != "gid" {
			http.Error(w, "rpc error: HostVolume.Create error: unknown mkdir parameter", 500)
			return
		}
	}
	if fakeString(v, "NodePool") == "" {
		v["NodePool"] = "default"
	}
	if fakeString(v, "NodeID") == "" {
		node, _, ok := f.chooseNode(fakeRow{"Namespace": ns, "NodePool": v["NodePool"]}, fakeRow{})
		if !ok {
			http.Error(w, "No eligible node for host volume", 400)
			return
		}
		v["NodeID"] = node.ID
	}
	node, ok := f.nodes[fakeString(v, "NodeID")]
	if !ok || node.Status != "ready" || node.NodePool != v["NodePool"] {
		http.Error(w, "Host volume node is unavailable", 400)
		return
	}
	var previous fakeRow
	if id := fakeString(v, "ID"); id != "" {
		previous = f.volumes[id]
		if previous == nil || previous["Namespace"] != ns {
			http.Error(w, "Host volume does not exist", 400)
			return
		}
	}
	for _, existing := range f.volumes {
		if previous != nil && existing["ID"] == previous["ID"] {
			continue
		}
		// The client-side name registry is node-local, not namespace-local.
		if existing["Name"] == v["Name"] && existing["NodeID"] == v["NodeID"] {
			http.Error(w, "rpc error: HostVolume.Create error: volume name already exists", 500)
			return
		}
	}
	id := fakeString(v, "ID")
	if previous == nil {
		id = f.nextID()
	}
	v["ID"], v["Namespace"], v["State"] = id, ns, "pending"
	idx := f.nextIndex()
	v["CreateIndex"], v["ModifyIndex"], v["CreateTime"], v["ModifyTime"] = idx, idx, f.now(), f.now()
	if previous != nil {
		v["CreateIndex"], v["CreateTime"] = previous["CreateIndex"], previous["CreateTime"]
	}
	capacity := v["RequestedCapacityMinBytes"]
	if capacity == nil {
		capacity = v["CapacityMinBytes"]
	}
	if capacity == nil {
		capacity = 0
	}
	v["CapacityBytes"] = capacity
	f.volumes[fakeString(v, "ID")] = v
	fakeJSON(w, 200, fakeRow{"Volume": v, "Index": idx})
}

func (f *fakeNomad) volumeEndpoint(w http.ResponseWriter, r *http.Request, ns string) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/volume/host/")
	v := f.volumes[id]
	if v == nil || v["Namespace"] != ns {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		row := fakeCopy(v)
		rows := []fakeRow{}
		for _, a := range f.allocs {
			if slices.Contains(a.volumes, id) && !fakeTerminal(a) {
				rows = append(rows, a.row)
			}
		}
		row["Allocations"] = fakeSorted(rows, "ID")
		fakeJSON(w, 200, row)
		return
	case http.MethodDelete:
		for _, a := range f.allocs {
			if slices.Contains(a.volumes, id) && !fakeTerminal(a) {
				http.Error(w, "Host volume is in use by an allocation", http.StatusConflict)
				return
			}
		}
		delete(f.volumes, id)
		fakeJSON(w, 200, fakeRow{"Index": f.nextIndex()})
		return
	}
	http.NotFound(w, r)
}

func (f *fakeNomad) variableEndpoint(w http.ResponseWriter, r *http.Request, ns string) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/var/")
	key := fakeKey(ns, path)
	stored := f.variables[key]
	if r.Method == http.MethodGet {
		if stored == nil {
			http.NotFound(w, r)
		} else {
			fakeJSON(w, 200, stored)
		}
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodDelete {
		http.NotFound(w, r)
		return
	}
	if values, ok := r.URL.Query()["cas"]; ok {
		cas, err := strconv.ParseUint(values[0], 10, 64)
		if err != nil {
			http.Error(w, "Invalid check-and-set index", 400)
			return
		}
		actual := uint64(0)
		if stored != nil {
			actual = uint64(fakeInt(stored, "ModifyIndex"))
		}
		if cas != actual {
			if stored == nil {
				fakeJSON(w, 409, fakeRow{"Path": path, "Namespace": ns})
			} else {
				fakeJSON(w, 409, stored)
			}
			return
		}
	}
	if r.Method == http.MethodDelete {
		delete(f.variables, key)
		f.nextIndex()
		fakeJSON(w, 200, nil)
		return
	}
	v := fakeDecode(w, r)
	if v == nil {
		return
	}
	if value := fakeString(v, "Namespace"); value != "" && value != ns {
		http.Error(w, "Variable namespace does not match request", 400)
		return
	}
	if value := fakeString(v, "Path"); value != "" && value != path {
		http.Error(w, "Variable path does not match request", 400)
		return
	}
	if len(path) == 0 || len(path) > 128 || strings.ContainsFunc(path, func(c rune) bool {
		return !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_~/", c))
	}) {
		http.Error(w, "Invalid variable path", 400)
		return
	}
	items := fakeMap(v["Items"])
	if len(items) == 0 {
		http.Error(w, "Variable items must not be empty", 400)
		return
	}
	// Nomad counts UTF-8 bytes of keys + values, NOT JSON encoding overhead:
	// https://developer.hashicorp.com/nomad/api-docs/variables/variables#restrictions
	// The documented limit is 64 KiB (65,536 bytes).
	size := 0
	for k, value := range items {
		s, ok := value.(string)
		if !ok {
			http.Error(w, "Variable items must be strings", 400)
			return
		}
		size += len(k) + len(s)
	}
	if size > 64*1024 {
		http.Error(w, "Variable items exceed 64 KiB", 400)
		return
	}
	if f.namespaces[ns] == nil {
		http.Error(w, "Namespace does not exist", 400)
		return
	}
	idx := f.nextIndex()
	v = fakeRow{"Path": path, "Namespace": ns, "Items": items, "CreateIndex": idx, "ModifyIndex": idx, "CreateTime": f.now(), "ModifyTime": f.now()}
	if stored != nil {
		v["CreateIndex"], v["CreateTime"] = stored["CreateIndex"], stored["CreateTime"]
	}
	f.variables[key] = v
	fakeJSON(w, 200, v)
}
