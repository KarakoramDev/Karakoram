// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func fakeCall(t *testing.T, f *fakeNomad, method, path, token string, body any, wantStatus int) []byte {
	t.Helper()
	var data []byte
	if body != nil {
		var err error
		data, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, f.URL+path, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("X-Nomad-Token", token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := f.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, resp.StatusCode, wantStatus, out)
	}
	return out
}

func fakeObject(t *testing.T, data []byte) fakeRow {
	t.Helper()
	var row fakeRow
	if err := json.Unmarshal(data, &row); err != nil {
		t.Fatal(err)
	}
	return row
}

func fakeList(t *testing.T, data []byte) []fakeRow {
	t.Helper()
	var rows []fakeRow
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func fakeJobBody(id, volume string, count int) fakeRow {
	group := fakeRow{"Name": "member", "Count": count, "Tasks": []any{fakeRow{
		"Name": "agent", "Driver": "exec", "Config": fakeRow{"command": "/bin/agent", "args": []string{"start"}},
		"Env": fakeRow{"EXAMPLE": "value"}, "Templates": []any{fakeRow{"EmbeddedTmpl": "configuration", "DestPath": "local/config", "ChangeMode": "noop"}},
		"Artifacts": []any{fakeRow{"GetterSource": "https://downloads.example.test/release.tar.gz", "RelativeDest": "local/"}},
		"Resources": fakeRow{"CPU": 100, "MemoryMB": 128},
	}}}
	if volume != "" {
		group["Volumes"] = fakeRow{"data": fakeRow{"Type": "host", "Source": volume, "AccessMode": "single-node-single-writer", "AttachmentMode": "file-system", "Sticky": true}}
	}
	return fakeRow{"Job": fakeRow{"ID": id, "Name": id, "Namespace": "default", "NodePool": "default", "Type": "service", "Meta": fakeRow{"KARAKORAM_OWNER": "test-owner"}, "TaskGroups": []any{group}}}
}

func fakeRegister(t *testing.T, f *fakeNomad, body fakeRow) fakeRow {
	t.Helper()
	return fakeObject(t, fakeCall(t, f, "PUT", "/v1/jobs", fakeNomadToken, body, 200))
}

func fakeCreateVolume(t *testing.T, f *fakeNomad, name, node string) fakeRow {
	t.Helper()
	body := fakeRow{"Volume": fakeRow{"Name": name, "Namespace": "default", "PluginID": "mkdir", "NodeID": node, "NodePool": "default", "RequestedCapacityMinBytes": 1024, "RequestedCapacityMaxBytes": 4096, "RequestedCapabilities": []any{fakeRow{"AccessMode": "single-node-single-writer", "AttachmentMode": "file-system"}}, "Parameters": fakeRow{"mode": "0700"}}}
	return fakeMap(fakeObject(t, fakeCall(t, f, "PUT", "/v1/volume/host/create", fakeNomadToken, body, 200))["Volume"])
}

func fakeAllocs(t *testing.T, f *fakeNomad, job string) []fakeRow {
	t.Helper()
	return fakeList(t, fakeCall(t, f, "GET", "/v1/job/"+job+"/allocations", fakeNomadToken, nil, 200))
}

func TestFakeNomadTokenScoping(t *testing.T) {
	f := newFakeNomad(t)
	f.addNamespace("other")
	f.addToken("other-token", false, "other")
	for _, token := range []string{"", "unknown-token"} {
		data := fakeCall(t, f, "GET", "/v1/nodes", token, nil, 403)
		if !strings.Contains(string(data), "Permission denied") {
			t.Fatalf("unexpected ACL error: %s", data)
		}
	}
	self := fakeObject(t, fakeCall(t, f, "GET", "/v1/acl/token/self", fakeNomadToken, nil, 200))
	if self["Type"] != "client" {
		t.Fatalf("self: %#v", self)
	}
	for _, endpoint := range []struct{ method, path string }{
		{"GET", "/v1/nodes"}, {"GET", "/v1/node/node-a"}, {"GET", "/v1/namespace/other"},
		{"GET", "/v1/namespaces"}, {"GET", "/v1/acl/token/self"}, {"PUT", "/v1/jobs"},
		{"GET", "/v1/jobs"}, {"GET", "/v1/job/absent"}, {"DELETE", "/v1/job/absent"},
		{"GET", "/v1/evaluation/absent"}, {"GET", "/v1/allocation/absent"},
		{"POST", "/v1/client/allocation/absent/restart"}, {"GET", "/v1/client/fs/logs/absent"},
		{"PUT", "/v1/volume/host/create"}, {"GET", "/v1/volume/host/absent"}, {"DELETE", "/v1/volume/host/absent"},
		{"GET", "/v1/volumes"}, {"GET", "/v1/vars"}, {"PUT", "/v1/var/private/path"}, {"GET", "/v1/var/private/path"}, {"DELETE", "/v1/var/private/path"},
	} {
		fakeCall(t, f, endpoint.method, endpoint.path+"?namespace=other", fakeNomadToken, nil, 403)
	}
	fakeCall(t, f, "GET", "/v1/namespace/other", fakeNomadToken, nil, 403)
	fakeCall(t, f, "GET", "/v1/namespace/other?namespace=other", "other-token", nil, 200)
	fakeCall(t, f, "GET", "/v1/namespace/missing", fakeNomadManagementToken, nil, 404)
	namespaces := fakeList(t, fakeCall(t, f, "GET", "/v1/namespaces", fakeNomadToken, nil, 200))
	if len(namespaces) != 1 || namespaces[0]["Name"] != "default" {
		t.Fatalf("namespace disclosure: %#v", namespaces)
	}
	all := fakeList(t, fakeCall(t, f, "GET", "/v1/namespaces", fakeNomadManagementToken, nil, 200))
	if len(all) != 2 {
		t.Fatalf("management namespace list: %#v", all)
	}
	body := fakeRow{"Namespace": "other", "Path": "private/path", "Items": fakeRow{"value": "other"}}
	fakeCall(t, f, "PUT", "/v1/var/private/path?namespace=other", "other-token", body, 200)
	fakeCall(t, f, "GET", "/v1/var/private/path", fakeNomadToken, nil, 404)
	// A body cannot smuggle a different namespace past the query ACL check.
	fakeCall(t, f, "PUT", "/v1/var/private/path", fakeNomadToken, body, 400)
	job := fakeJobBody("wrong-scope", "", 1)
	fakeMap(job["Job"])["Namespace"] = "other"
	fakeCall(t, f, "PUT", "/v1/jobs", fakeNomadToken, job, 400)
}

func TestFakeNomadRejectsAutoPromoteWithoutCanary(t *testing.T) {
	f := newFakeNomad(t)
	job := fakeJobBody("promote", "", 1)
	group := fakeRows(fakeMap(job["Job"])["TaskGroups"])[0]
	group["Update"] = fakeRow{"MaxParallel": 1, "Canary": 0, "AutoPromote": true}
	rejected := fakeCall(t, f, "PUT", "/v1/jobs", fakeNomadToken, job, 500)
	if !strings.Contains(string(rejected), "Auto Promote requires a Canary count greater than zero") {
		t.Fatalf("wrong validation failure: %s", rejected)
	}
	fakeCall(t, f, "GET", "/v1/job/promote", fakeNomadToken, nil, 404)
	group["Update"] = fakeRow{"MaxParallel": 1, "Canary": 0, "AutoPromote": false}
	fakeRegister(t, f, job)
}

func TestFakeNomadRegisterConverges(t *testing.T) {
	f := newFakeNomad(t)
	f.setAutoTick(false)
	body := fakeJobBody("member", "", 3)
	registered := fakeRegister(t, f, body)
	evalPath := "/v1/evaluation/" + fakeString(registered, "EvalID")
	eval := fakeObject(t, fakeCall(t, f, "GET", evalPath, fakeNomadToken, nil, 200))
	if eval["Status"] != "pending" {
		t.Fatalf("initial eval: %#v", eval)
	}
	allocs := fakeAllocs(t, f, "member")
	if len(allocs) != 3 {
		t.Fatalf("allocation count: %d", len(allocs))
	}
	nodes := []string{}
	for _, a := range allocs {
		if a["ClientStatus"] != "pending" {
			t.Fatalf("initial alloc: %#v", a)
		}
		nodes = append(nodes, fakeString(a, "NodeID"))
	}
	slices.Sort(nodes)
	if !slices.Equal(nodes, []string{"node-a", "node-b", "node-c"}) {
		t.Fatalf("unbalanced placement: %v", nodes)
	}
	f.tick()
	eval = fakeObject(t, fakeCall(t, f, "GET", evalPath, fakeNomadToken, nil, 200))
	if eval["Status"] != "complete" {
		t.Fatalf("eval did not complete: %#v", eval)
	}
	if fakeAllocs(t, f, "member")[0]["ClientStatus"] != "pending" {
		t.Fatal("allocation started too soon")
	}
	f.tick()
	for _, a := range fakeAllocs(t, f, "member") {
		if a["ClientStatus"] != "running" || fakeMap(fakeMap(a["TaskStates"])["agent"])["State"] != "running" {
			t.Fatalf("allocation not running: %#v", a)
		}
		if fakeInt(a, "JobVersion") != 0 || fakeInt(a, "CreateTime") == 0 || fakeMap(a["NetworkStatus"])["Address"] == "" {
			t.Fatalf("incomplete allocation shape: %#v", a)
		}
	}
	job := fakeObject(t, fakeCall(t, f, "GET", "/v1/job/member", fakeNomadToken, nil, 200))
	if job["Status"] != "running" {
		t.Fatalf("job status: %#v", job)
	}
	if !bytes.Equal(fakeCall(t, f, "GET", "/v1/jobs?prefix=absent&meta=true", fakeNomadToken, nil, 200), []byte("[]\n")) {
		t.Fatal("job prefix was ignored")
	}
	stubs := fakeList(t, fakeCall(t, f, "GET", "/v1/jobs?prefix=mem&meta=true", fakeNomadToken, nil, 200))
	if len(stubs) != 1 || fakeMap(stubs[0]["Meta"])["KARAKORAM_OWNER"] != "test-owner" {
		t.Fatalf("job stubs: %#v", stubs)
	}
	deployment := fakeObject(t, fakeCall(t, f, "GET", "/v1/job/member/deployment", fakeNomadToken, nil, 200))
	if deployment["Status"] != "successful" {
		t.Fatalf("deployment: %#v", deployment)
	}
	// Nomad's CombinedTaskMeta makes job metadata a destructive task update.
	fakeMap(fakeMap(body["Job"])["Meta"])["label"] = "updated"
	fakeRegister(t, f, body)
	if len(fakeAllocs(t, f, "member")) != 6 {
		t.Fatal("metadata update did not replace allocations")
	}
	job = fakeObject(t, fakeCall(t, f, "GET", "/v1/job/member", fakeNomadToken, nil, 200))
	if fakeInt(job, "Version") != 1 {
		t.Fatalf("job version: %#v", job)
	}
}

func TestFakeNomadStickyVolumeAndClaim(t *testing.T) {
	f := newFakeNomad(t)
	f.setAutoTick(false)
	volume := fakeCreateVolume(t, f, "data", "node-c")
	id := fakeString(volume, "ID")
	if volume["State"] != "pending" || len(id) != 36 {
		t.Fatalf("volume create: %#v", volume)
	}
	f.tick()
	ready := fakeObject(t, fakeCall(t, f, "GET", "/v1/volume/host/"+id, fakeNomadToken, nil, 200))
	if ready["State"] != "ready" {
		t.Fatalf("volume not ready: %#v", ready)
	}
	fakeRegister(t, f, fakeJobBody("sticky", "data", 1))
	alloc := fakeAllocs(t, f, "sticky")[0]
	if alloc["NodeID"] != "node-c" {
		t.Fatalf("host volume did not pin allocation: %#v", alloc)
	}
	fakeCall(t, f, "DELETE", "/v1/volume/host/"+id, fakeNomadToken, nil, 409)
	// A name may be registered on another node, but not twice on one node.
	duplicate := fakeRow{"Volume": fakeRow{"Name": "data", "NodeID": "node-c", "PluginID": "mkdir"}}
	fakeCall(t, f, "PUT", "/v1/volume/host/create", fakeNomadToken, duplicate, 500)
	volumes := fakeList(t, fakeCall(t, f, "GET", "/v1/volumes?type=host&prefix="+id[:8], fakeNomadToken, nil, 200))
	if len(volumes) != 1 || volumes[0]["ID"] != id {
		t.Fatalf("volume stubs: %#v", volumes)
	}
	fakeCall(t, f, "DELETE", "/v1/job/sticky", fakeNomadToken, nil, 200)
	// DesiredStatus stop alone does not release the claim until client terminal.
	fakeCall(t, f, "DELETE", "/v1/volume/host/"+id, fakeNomadToken, nil, 409)
	f.tick()
	fakeCall(t, f, "DELETE", "/v1/volume/host/"+id, fakeNomadToken, nil, 200)
	fakeCall(t, f, "GET", "/v1/volume/host/"+id, fakeNomadToken, nil, 404)
	job := fakeObject(t, fakeCall(t, f, "GET", "/v1/job/sticky", fakeNomadToken, nil, 200))
	if job["Stop"] != true || job["Status"] != "dead" {
		t.Fatalf("stopped job: %#v", job)
	}
	fakeCall(t, f, "DELETE", "/v1/job/sticky?purge=true", fakeNomadToken, nil, 200)
	fakeCall(t, f, "GET", "/v1/job/sticky", fakeNomadToken, nil, 404)
}

func TestFakeNomadDestructiveUpdateOrdering(t *testing.T) {
	for _, stopFirst := range []bool{false, true} {
		t.Run(strconv.FormatBool(stopFirst), func(t *testing.T) {
			f := newFakeNomad(t)
			f.setAutoTick(false)
			fakeCreateVolume(t, f, "data", "node-b")
			f.tick()
			body := fakeJobBody("writer", "data", 1)
			fakeRegister(t, f, body)
			f.tick()
			f.tick()
			before := fakeAllocs(t, f, "writer")[0]
			if before["ClientStatus"] != "running" {
				t.Fatal("old allocation not running")
			}
			if stopFirst {
				fakeCall(t, f, "DELETE", "/v1/job/writer", fakeNomadToken, nil, 200)
				f.tick()
				if fakeAllocs(t, f, "writer")[0]["ClientStatus"] != "complete" {
					t.Fatal("stop did not converge")
				}
			}
			group := fakeRows(fakeMap(body["Job"])["TaskGroups"])[0]
			fakeMap(fakeRows(group["Tasks"])[0]["Resources"])["MemoryMB"] = 256
			fakeRegister(t, f, body)
			allocs := fakeAllocs(t, f, "writer")
			if len(allocs) != 2 {
				t.Fatalf("destructive update allocations: %d", len(allocs))
			}
			if got := len(f.writerOverlaps()); (got > 0) == stopFirst {
				t.Fatalf("stopFirst=%t: overlap count %d", stopFirst, got)
			}
			if !stopFirst && (allocs[0]["DesiredStatus"] != "stop" || allocs[0]["ClientStatus"] != "running") {
				t.Fatalf("old client must still be stopping: %#v", allocs[0])
			}
			f.tick()
			f.tick()
			allocs = fakeAllocs(t, f, "writer")
			if allocs[0]["ClientStatus"] != "complete" || allocs[1]["ClientStatus"] != "running" || fakeInt(allocs[1], "JobVersion") != 1 {
				t.Fatalf("replacement convergence: %#v", allocs)
			}
		})
	}
}

func TestFakeNomadVariablesCAS(t *testing.T) {
	f := newFakeNomad(t)
	path := "/v1/var/karakoram/services/example"
	fakeCall(t, f, "GET", path, fakeNomadToken, nil, 404)
	body := fakeRow{"Path": "karakoram/services/example", "Items": fakeRow{"record": "first"}}
	v := fakeObject(t, fakeCall(t, f, "PUT", path+"?cas=0", fakeNomadToken, body, 200))
	index := fakeInt(v, "ModifyIndex")
	conflict := fakeObject(t, fakeCall(t, f, "PUT", path+"?cas=0", fakeNomadToken, body, 409))
	if fakeInt(conflict, "ModifyIndex") != index || fakeMap(conflict["Items"])["record"] != "first" {
		t.Fatalf("CAS conflict: %#v", conflict)
	}
	body["Items"] = fakeRow{"replacement": "second"}
	v = fakeObject(t, fakeCall(t, f, "PUT", path+"?cas="+strconv.Itoa(index), fakeNomadToken, body, 200))
	if fakeInt(v, "ModifyIndex") <= index || fakeInt(v, "CreateIndex") != index || fakeMap(v["Items"])["record"] != nil {
		t.Fatalf("variable replacement: %#v", v)
	}
	stubs := fakeList(t, fakeCall(t, f, "GET", "/v1/vars?prefix=karakoram/services/", fakeNomadToken, nil, 200))
	if len(stubs) != 1 || stubs[0]["Items"] != nil {
		t.Fatalf("variable metadata leaked Items: %#v", stubs)
	}
	fakeCall(t, f, "DELETE", path+"?cas="+strconv.Itoa(index), fakeNomadToken, nil, 409)
	fakeCall(t, f, "DELETE", path+"?cas="+strconv.Itoa(fakeInt(v, "ModifyIndex")), fakeNomadToken, nil, 200)
	fakeCall(t, f, "GET", path, fakeNomadToken, nil, 404)
	fakeCall(t, f, "PUT", path+"?cas=not-a-number", fakeNomadToken, body, 400)
	// JSON escaping adds bytes, but Nomad limits the raw UTF-8 keys + values.
	body["Items"] = fakeRow{"k": strings.Repeat("\n", 64*1024-1)}
	fakeCall(t, f, "PUT", path, fakeNomadToken, body, 200)
	body["Items"] = fakeRow{"k": strings.Repeat("é", 32*1024)}
	fakeCall(t, f, "PUT", path, fakeNomadToken, body, 400)
}

func TestFakeNomadFailureStallAndPlacement(t *testing.T) {
	t.Run("stall and failure", func(t *testing.T) {
		f := newFakeNomad(t)
		f.setAutoTick(false)
		f.setAllocStartTicks(3)
		f.stallAllocs(true)
		f.failNextAlloc("failure")
		fakeRegister(t, f, fakeJobBody("failure", "", 1))
		for range 4 {
			f.tick()
		}
		if fakeAllocs(t, f, "failure")[0]["ClientStatus"] != "pending" {
			t.Fatal("stall knob ignored")
		}
		f.stallAllocs(false)
		f.tick()
		a := fakeAllocs(t, f, "failure")[0]
		if a["ClientStatus"] != "failed" || fakeMap(fakeMap(a["TaskStates"])["agent"])["Failed"] != true {
			t.Fatalf("failure knob: %#v", a)
		}
		fakeRegister(t, f, fakeJobBody("unaffected", "", 1))
		for range 3 {
			f.tick()
		}
		if fakeAllocs(t, f, "unaffected")[0]["ClientStatus"] != "running" {
			t.Fatal("one-shot failure leaked")
		}
	})
	t.Run("pinned down node blocks", func(t *testing.T) {
		f := newFakeNomad(t)
		fakeCreateVolume(t, f, "data", "node-c")
		fakeRegister(t, f, fakeJobBody("first", "data", 1))
		f.tick()
		f.tick()
		f.setNodeDown("node-c", true)
		if fakeAllocs(t, f, "first")[0]["ClientStatus"] != "lost" {
			t.Fatal("down node did not lose allocation")
		}
		registered := fakeRegister(t, f, fakeJobBody("blocked", "data", 1))
		eval := fakeObject(t, fakeCall(t, f, "GET", "/v1/evaluation/"+fakeString(registered, "EvalID"), fakeNomadToken, nil, 200))
		if eval["Status"] != "complete" || len(fakeMap(eval["FailedTGAllocs"])) != 1 || fakeString(eval, "BlockedEval") == "" {
			t.Fatalf("missing placement diagnostics: %#v", eval)
		}
		blocked := fakeObject(t, fakeCall(t, f, "GET", "/v1/evaluation/"+fakeString(eval, "BlockedEval"), fakeNomadToken, nil, 200))
		if blocked["Status"] != "blocked" || len(fakeAllocs(t, f, "blocked")) != 0 {
			t.Fatalf("infeasible job placed: %#v", blocked)
		}
	})
	t.Run("eligibility and pool", func(t *testing.T) {
		f := newFakeNomad(t)
		f.setNodeEligible("node-a", false)
		f.setNodeDown("node-b", true)
		fakeRegister(t, f, fakeJobBody("eligible", "", 1))
		if fakeAllocs(t, f, "eligible")[0]["NodeID"] != "node-c" {
			t.Fatal("ineligible/down node was selected")
		}
		f.addNode(fakeNode{ID: "special", Name: "special", NodePool: "special"})
		body := fakeJobBody("pooled", "", 1)
		fakeMap(body["Job"])["NodePool"] = "special"
		fakeRegister(t, f, body)
		if fakeAllocs(t, f, "pooled")[0]["NodeID"] != "special" {
			t.Fatal("node pool ignored")
		}
	})
}

func TestFakeNomadRestartLogsAndRequests(t *testing.T) {
	f := newFakeNomad(t)
	fakeRegister(t, f, fakeJobBody("logs", "", 1))
	f.tick()
	f.tick()
	alloc := fakeAllocs(t, f, "logs")[0]
	id := fakeString(alloc, "ID")
	fakeCall(t, f, "POST", "/v1/client/allocation/"+id+"/restart", fakeNomadToken, fakeRow{"TaskName": "agent"}, 200)
	alloc = fakeObject(t, fakeCall(t, f, "GET", "/v1/allocation/"+id, fakeNomadToken, nil, 200))
	if alloc["ID"] != id || alloc["ClientStatus"] != "running" || fakeInt(fakeMap(fakeMap(alloc["TaskStates"])["agent"]), "Restarts") != 1 {
		t.Fatalf("restart rescheduled or lost state: %#v", alloc)
	}
	f.appendLogs(id, "agent", "stdout", "next line\n")
	f.appendLogs(id, "agent", "stderr", "diagnostic\n")
	base := "/v1/client/fs/logs/" + id + "?task=agent&plain=true&type="
	logs := fakeCall(t, f, "GET", base+"stdout&origin=end&offset=10", fakeNomadToken, nil, 200)
	if string(logs) != "next line\n" {
		t.Fatalf("tail logs: %q", logs)
	}
	logs = fakeCall(t, f, "GET", base+"stderr&origin=start&offset=1", fakeNomadToken, nil, 200)
	if string(logs) != "iagnostic\n" {
		t.Fatalf("stderr offset: %q", logs)
	}
	fakeCall(t, f, "GET", "/v1/not-implemented?namespace=default&token=must-not-record&secret=must-not-record", fakeNomadToken, nil, 404)
	calls := f.requests()
	last := calls[len(calls)-1]
	if last.Method != "GET" || last.Path != "/v1/not-implemented" || last.Query.Get("namespace") != "default" || strings.Contains(last.Query.Encode(), "must-not-record") {
		t.Fatalf("request recording: %#v", last)
	}
	// Returned snapshots must not let assertions mutate the fixture's history.
	last.Query.Set("namespace", "changed")
	if f.requests()[len(calls)-1].Query.Get("namespace") != "default" {
		t.Fatal("requests returned shared query state")
	}
	if len(f.CA()) == 0 || f.certPool() == nil {
		t.Fatal("missing TLS trust material")
	}
	if _, err := url.ParseRequestURI(f.URL); err != nil {
		t.Fatal(err)
	}
}

func TestFakeNomadJobRegistrationCAS(t *testing.T) {
	f := newFakeNomad(t)
	f.setAutoTick(false)
	body := fakeJobBody("cas", "", 1)
	body["EnforceIndex"] = true
	body["JobModifyIndex"] = 12
	fakeCall(t, f, "POST", "/v1/jobs", fakeNomadToken, body, 500)
	fakeCall(t, f, "GET", "/v1/job/cas", fakeNomadToken, nil, 404)
	body["JobModifyIndex"] = 0
	first := fakeObject(t, fakeCall(t, f, "POST", "/v1/jobs", fakeNomadToken, body, 200))
	index := fakeInt(first, "JobModifyIndex")
	rejected := fakeCall(t, f, "POST", "/v1/jobs", fakeNomadToken, body, 500)
	if !strings.Contains(string(rejected), "Enforcing job modify index 0: job already exists") {
		t.Fatalf("wrong CAS failure: %s", rejected)
	}
	body["JobModifyIndex"] = index + 1
	fakeCall(t, f, "POST", "/v1/jobs", fakeNomadToken, body, 500)
	job := fakeObject(t, fakeCall(t, f, "GET", "/v1/job/cas", fakeNomadToken, nil, 200))
	if fakeInt(job, "JobModifyIndex") != index || fakeInt(job, "Version") != 0 || len(fakeAllocs(t, f, "cas")) != 1 {
		t.Fatalf("failed CAS changed job: %#v", job)
	}
	body["JobModifyIndex"] = index
	fakeMap(fakeMap(body["Job"])["Meta"])["changed"] = "yes"
	second := fakeObject(t, fakeCall(t, f, "POST", "/v1/jobs", fakeNomadToken, body, 200))
	if fakeInt(second, "JobModifyIndex") <= index {
		t.Fatalf("successful CAS did not advance index: %#v", second)
	}
	fakeCall(t, f, "DELETE", "/v1/job/cas?purge=true", fakeNomadToken, nil, 200)
	f.tick()
	body["JobModifyIndex"] = 0
	fakeCall(t, f, "POST", "/v1/jobs", fakeNomadToken, body, 200)
}

func TestFakeNomadHostVolumeNodeNamesAndUpdates(t *testing.T) {
	f := newFakeNomad(t)
	f.addNamespace("other")
	f.addToken("other-volume-token", false, "other")
	first := fakeCreateVolume(t, f, "data", "node-a")
	id := fakeString(first, "ID")
	request := fakeRow{"Volume": fakeRow{"Name": "data", "NodeID": "node-a", "PluginID": "mkdir"}}
	for _, scope := range []struct{ path, token string }{
		{"/v1/volume/host/create", fakeNomadToken},
		{"/v1/volume/host/create?namespace=other", "other-volume-token"},
	} {
		response := fakeCall(t, f, "PUT", scope.path, scope.token, request, 500)
		if !strings.Contains(string(response), "rpc error: HostVolume.Create error: volume name already exists") {
			t.Fatalf("unexpected duplicate-name response: %s", response)
		}
	}
	second := fakeCreateVolume(t, f, "data", "node-b")
	if second["ID"] == id {
		t.Fatal("same name on another node did not receive a new ID")
	}
	v := fakeMap(request["Volume"])
	v["ID"] = id
	v["RequestedCapacityMinBytes"] = 8192
	updated := fakeMap(fakeObject(t, fakeCall(t, f, "PUT", "/v1/volume/host/create", fakeNomadToken, request, 200))["Volume"])
	if updated["ID"] != id || updated["CreateIndex"] != first["CreateIndex"] || fakeInt(updated, "ModifyIndex") <= fakeInt(first, "ModifyIndex") || fakeInt(updated, "CapacityBytes") != 8192 {
		t.Fatalf("volume update did not preserve identity: %#v", updated)
	}
	v["Name"] = "invalid-parameters"
	delete(v, "ID")
	v["Parameters"] = fakeRow{"unrecognized": "value"}
	fakeCall(t, f, "PUT", "/v1/volume/host/create", fakeNomadToken, request, 500)
	rows := fakeList(t, fakeCall(t, f, "GET", "/v1/volumes?type=host", fakeNomadToken, nil, 200))
	if len(rows) != 2 {
		t.Fatalf("rejected create or update created an extra volume: %#v", rows)
	}
	v["Parameters"] = fakeRow{"mode": "0700", "uid": "1000", "gid": "1000"}
	fakeCall(t, f, "PUT", "/v1/volume/host/create", fakeNomadToken, request, 200)
}
