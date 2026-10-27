// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type allocationTaskState struct {
	StartedAt, FinishedAt time.Time
}

type allocation struct {
	ID, JobID, NodeID, NodeName, ClientStatus, DesiredStatus string
	JobVersion                                               uint64
	CreateIndex                                              uint64
	NetworkStatus                                            struct {
		Address, AddressIPv6 string
	}
	TaskStates       map[string]allocationTaskState
	DeploymentStatus *struct {
		Healthy *bool
	}
}

func allocationTerminal(a allocation) bool {
	switch a.ClientStatus {
	case "complete", "failed", "lost":
		return true
	default:
		return false
	}
}

func (b *Backend) allocations(ctx context.Context, hostname string) ([]allocation, error) {
	var out []allocation
	_, err := b.c.get(ctx, "/v1/job/"+url.PathEscape(hostname)+"/allocations", url.Values{"all": {"true"}}, &out)
	if errors.Is(err, infra.ErrNotFound) {
		return []allocation{}, nil
	}
	return out, err
}

func (b *Backend) Containers(ctx context.Context, hostname string) ([]infra.Container, error) {
	r, err := b.require(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if err := b.resumePending(ctx, r); err != nil {
		return nil, err
	}
	out := []infra.Container{}
	if r.Kind != infra.KindRuntime {
		return out, nil
	}
	allocs, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return nil, err
	}
	hydration := make([]struct {
		gone bool
	}, len(allocs))
	if err := infra.ObserveParallel(ctx, len(allocs), func(i int) error {
		a := allocs[i]
		if allocationTerminal(a) || a.NetworkStatus.Address != "" || a.NetworkStatus.AddressIPv6 != "" {
			return nil
		}
		// Hydrate CNI addresses independently; host-network addresses remain
		// excluded and disappeared allocations are omitted, never adopted.
		_, err := b.c.get(ctx, "/v1/allocation/"+url.PathEscape(a.ID), nil, &a)
		if errors.Is(err, infra.ErrNotFound) {
			hydration[i].gone = true
			return nil
		}
		if err != nil {
			return err
		}
		allocs[i] = a
		return nil
	}); err != nil {
		return nil, err
	}
	for i, a := range allocs {
		if hydration[i].gone || allocationTerminal(a) {
			continue
		}
		status := infra.StatusCreating
		if a.ClientStatus == "running" {
			status = infra.ContainerRunning
		}
		placement := a.NodeName
		if placement == "" {
			placement = a.NodeID
		}
		container := infra.Container{ID: a.ID, Hostname: r.Hostname, Status: status, PlacementGroup: placement}
		for _, address := range []string{a.NetworkStatus.Address, a.NetworkStatus.AddressIPv6} {
			if ip, err := netip.ParseAddr(address); err == nil {
				ip = ip.Unmap()
				if ip.Is4() {
					container.IPv4 = ip.String()
				} else {
					container.IPv6 = ip.String()
				}
			}
		}
		out = append(out, container)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// Logs uses plain Nomad logs, which have no per-line timestamps. Each line is
// stamped with its task's latest start time. Since excludes only runs known to
// have finished before it; live runs and unknown finish times remain included.
func (b *Backend) Logs(ctx context.Context, hostname string, q infra.LogQuery) ([]infra.LogLine, error) {
	r, err := b.require(ctx, hostname)
	if err != nil {
		return nil, err
	}
	out := []infra.LogLine{}
	if r.Kind != infra.KindRuntime {
		return out, nil
	}
	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	limit = min(limit, 10000)
	byteLimit := min(int64(limit)*4096, 8<<20)
	allocs, err := b.allocations(ctx, r.Hostname)
	if err != nil {
		return nil, err
	}
	sort.Slice(allocs, func(i, j int) bool { return allocs[i].ID < allocs[j].ID })
	for _, a := range allocs {
		if q.Container != "" && q.Container != a.ID {
			continue
		}
		if len(a.TaskStates) == 0 {
			if _, err := b.c.get(ctx, "/v1/allocation/"+url.PathEscape(a.ID), nil, &a); err != nil {
				if allocationTerminal(a) || a.ClientStatus == "unknown" {
					continue
				}
				return nil, err
			}
		}
		tasks := make([]string, 0, len(a.TaskStates))
		for task := range a.TaskStates {
			tasks = append(tasks, task)
		}
		sort.Strings(tasks)
		for _, task := range tasks {
			started := a.TaskStates[task].StartedAt
			finished := a.TaskStates[task].FinishedAt
			if !q.Since.IsZero() && !finished.IsZero() && finished.Before(q.Since) {
				continue
			}
			for _, stream := range []string{"stdout", "stderr"} {
				query := url.Values{
					"task":   {task},
					"type":   {stream},
					"plain":  {"true"},
					"follow": {"false"},
					"origin": {"end"},
					"offset": {strconv.FormatInt(byteLimit, 10)},
				}
				// Allow writes racing the end-relative offset, then bound the tail.
				data, err := b.c.getRaw(ctx, "/v1/client/fs/logs/"+url.PathEscape(a.ID), query, byteLimit+(64<<10))
				if err != nil {
					// A task can have no retained file for one output stream.
					// That stream is empty, not a missing member/allocation.
					if missingTaskLog(err, task, stream) {
						continue
					}
					if allocationTerminal(a) || a.ClientStatus == "unknown" {
						continue
					}
					return nil, err
				}
				if int64(len(data)) > byteLimit {
					data = data[int64(len(data))-byteLimit:]
				}
				lines := strings.Split(string(data), "\n")
				if len(lines) > 0 && lines[len(lines)-1] == "" {
					lines = lines[:len(lines)-1]
				}
				if len(lines) > limit {
					lines = lines[len(lines)-limit:]
				}
				for _, text := range lines {
					out = append(out, infra.LogLine{Timestamp: started, Container: a.ID, Stream: stream, Message: strings.TrimSuffix(text, "\r")})
				}
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}

func missingTaskLog(err error, task, stream string) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == 404 &&
		apiErr.Message == fmt.Sprintf("log entry for task %q and log type %q not found", task, stream)
}
