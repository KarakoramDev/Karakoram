// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import "strings"

type fakeProxyRollout struct {
	old, next []map[string]any
	index     int
	starting  bool
	failed    bool
}

func (f *fakeAPI) beginProxyRollout(a *fakeApplication) {
	old := a.tasks
	f.applyReplicaSpec(a, fakeInt(a.row, "replicas"), true)
	next := a.tasks[:fakeInt(a.row, "replicas")]
	a.tasks = old
	for _, task := range a.tasks {
		if strings.HasPrefix(fakeString(task, "currentState"), "Running") {
			task["health"] = "healthy"
		}
	}
	a.proxyRollout = &fakeProxyRollout{old: old, next: next}
	f.services[fakeString(a.row, "appName")]["UpdateStatus"] = map[string]any{"State": "updating"}
	f.recordProxyHealth(a)
}

func (f *fakeAPI) recordProxyHealth(a *fakeApplication) {
	healthy := 0
	for _, task := range a.tasks {
		if strings.HasPrefix(fakeString(task, "currentState"), "Running") && task["health"] == "healthy" {
			healthy++
		}
	}
	a.healthyHistory = append(a.healthyHistory, healthy)
}

func (f *fakeAPI) advanceProxyRollout(a *fakeApplication) {
	r := a.proxyRollout
	if r.failed || a.holdTasks {
		return
	}
	service := f.services[fakeString(a.row, "appName")]
	if r.index == len(r.next) {
		service["UpdateStatus"] = map[string]any{"State": "completed"}
		return
	}
	task := r.next[r.index]
	if !r.starting {
		active := 0
		for _, old := range a.tasks {
			if fakeString(old, "state") == "running" {
				active++
			}
		}
		if a.taskCapacity > 0 && active >= a.taskCapacity {
			return
		}
		task["currentState"] = "Starting 1 second ago"
		task["health"] = "starting"
		a.tasks = append(a.tasks, task)
		r.starting = true
	} else if a.neverHealthyProxy {
		task["health"] = "unhealthy"
		task["currentState"] = "Failed 1 second ago"
		task["state"] = "shutdown"
		service["UpdateStatus"] = map[string]any{"State": "paused", "Message": "replacement healthcheck failed"}
		r.failed = true
	} else {
		task["health"] = "healthy"
		task["currentState"] = "Running 1 second ago"
		// Retire a predecessor only AFTER recording the replacement's admission.
		f.recordProxyHealth(a)
		for _, old := range r.old {
			if strings.HasPrefix(fakeString(old, "currentState"), "Running") {
				old["state"] = "shutdown"
				old["currentState"] = "Shutdown 1 second ago"
				break
			}
		}
		r.index++
		r.starting = false
		if r.index == len(r.next) {
			// Scale-in also retires surplus old slots, never the new healthy fleet.
			for _, old := range r.old {
				old["state"] = "shutdown"
				old["currentState"] = "Shutdown 1 second ago"
			}
		}
	}
	f.recordProxyHealth(a)
}
