//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"errors"
	"fmt"
	api "github.com/KarakoramDev/Karakoram/pkg/client"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestScalingUpgrade(t *testing.T) {
	t.Run("ResourcesMinor", func(t *testing.T) {
		s := newSuite(t, true)
		older, latest := s.upgradeVersions("8.0", "8.0")
		spec := s.spec(3)
		spec.Version = older
		name := s.create(spec)
		s.put(name, "upgrade-proof", "durable")
		writer := s.startWriter(name)
		defer writer.finish()
		// Stay inside the requested 0.5-1 GiB range while changing RAM bounds.
		s.update(name, func(spec *api.ClusterSpec) {
			r := &spec.Components.Engine.Resources
			r.MinRam = 0.5
			r.MaxRam = 0.5
			r.MinCpu = 2
			r.MaxCpu = 2
			r.StartCpu = ptr(float32(2))
		})
		// Zerops accepts autoscaling bounds independently of current allocation.
		// Assert the platform configuration readback rather than requiring
		// immediate changes to container allocation.
		s.poll("observed Zerops CPU/RAM autoscaling configuration", 5*time.Minute, func() bool {
			var members api.MemberList
			s.expect(&members)(s.client.ListClusterMembers(s.ctx, name, nil))
			if len(members.Items) != 3 {
				return false
			}
			for _, m := range members.Items {
				r := m.Autoscaling
				if r == nil || r.CpuMode != api.ResourcesCpuModeSHARED || r.MinCpu != 2 || r.MaxCpu != 2 || r.MinRam != 0.5 || r.MaxRam != 0.5 || r.StartCpu == nil || *r.StartCpu != 2 {
					return false
				}
				if m.Resources == nil || m.Resources.CpuCores <= 0 || m.Resources.MemoryGB <= 0 {
					return false
				}
			}
			s.snapshot(name+"-resources", members.Items)
			return true
		})
		before := s.members(name)
		if len(before) != 3 {
			t.Fatal("expected three members before disk growth")
		}
		for _, m := range before {
			if m.DiskTotalBytes == nil || *m.DiskTotalBytes <= 0 {
				t.Fatal("member disk capacity missing before growth")
			}
		}
		s.update(name, func(spec *api.ClusterSpec) { spec.Components.Engine.Resources.MaxDisk = 8 })
		s.poll("grown mounted data volumes", 5*time.Minute, func() bool {
			now := s.members(name)
			if len(now) != len(before) {
				return false
			}
			for _, m := range now {
				found := false
				for _, old := range before {
					if m.Name == old.Name {
						found = m.DiskTotalBytes != nil && *m.DiskTotalBytes > *old.DiskTotalBytes
						break
					}
				}
				if !found {
					return false
				}
			}
			s.snapshot(name+"-grown-disks", now)
			return true
		})
		s.rollingUpgrade(name, latest)
		s.assertVersions(name, latest)
		s.proof(name, "upgrade-proof", "durable")
		s.rollingRestart(name)
		s.assertReplicaMembers(name, 3)
		s.proof(name, "upgrade-proof", "durable")
		writer.finish()
		s.assertWriterData(name, writer)
	})
	t.Run("Major", func(t *testing.T) {
		s := newSuite(t, true)
		from, to := s.upgradeVersions("7.0", "8.0")
		spec := s.spec(3)
		spec.Version = from
		name := s.create(spec)
		s.put(name, "major-proof", "durable")
		initial := s.cluster(name)
		if initial.Status.Fcv == nil || *initial.Status.Fcv != "7.0" {
			t.Fatal("7.0 cluster FCV missing or incorrect")
		}
		writer := s.startWriter(name)
		defer writer.finish()
		s.rollingUpgrade(name, to)
		s.assertVersions(name, to)
		upgraded := s.cluster(name)
		if upgraded.Status.Fcv == nil || *upgraded.Status.Fcv != "7.0" {
			t.Fatal("major upgrade must not automatically raise FCV")
		}
		s.operation(s.client.RaiseClusterFCV(s.ctx, name, &api.RaiseClusterFCVParams{}, api.RaiseFCVRequest{Version: "8.0"}))
		s.ready(name)
		s.poll("explicit FCV 8.0", 5*time.Minute, func() bool { c := s.cluster(name); return c.Status.Fcv != nil && *c.Status.Fcv == "8.0" })
		s.rollingRestart(name)
		writer.finish()
		s.proof(name, "major-proof", "durable")
		c := s.mongo(name)
		defer c.Disconnect(context.Background())
		count, err := c.Database("e2e").Collection("continuous").CountDocuments(s.ctx, bson.M{})
		if err != nil {
			t.Fatal(err)
		}
		if count != writer.success.Load() {
			t.Fatalf("continuous-write data count=%d acknowledged=%d", count, writer.success.Load())
		}
	})
	t.Run("CPUMode", func(t *testing.T) {
		s := newSuite(t, false)
		name := s.create(s.privateSpec(1))
		for _, mode := range []api.ResourcesCpuMode{api.ResourcesCpuModeDEDICATED, api.ResourcesCpuModeSHARED} {
			c := s.cluster(name)
			c.Spec.Components.Engine.Resources.CpuMode = mode
			var updated api.Cluster
			s.expect(&updated)(s.client.ReplaceClusterWithBody(s.ctx, name, &api.ReplaceClusterParams{}, "application/json", s.clusterBody(clusterWriteEnvelope{Spec: c.Spec})))
			// The platform may rate-limit CPU-mode changes; the second change
			// proves automatic convergence through a scheduled retry.
			timeout := operationTimeout
			if mode == api.ResourcesCpuModeSHARED {
				timeout = 75 * time.Minute
			}
			s.poll(name+" cpuMode="+string(mode)+" Ready", timeout, func() bool {
				updated = s.cluster(name)
				if updated.Status.Phase == api.ClusterStatusPhaseError {
					message := ""
					if updated.Status.Message != nil {
						message = *updated.Status.Message
					}
					s.t.Fatalf("CPU-mode change entered Error: %s", message)
				}
				return updated.Status.Phase == api.ClusterStatusPhaseReady && updated.Status.ObservedGeneration >= updated.Generation
			})
			if updated.Spec.Components.Engine.Resources.CpuMode != mode {
				t.Fatalf("cpuMode=%s want=%s", updated.Spec.Components.Engine.Resources.CpuMode, mode)
			}
			s.poll("observed Zerops CPU mode "+string(mode), 5*time.Minute, func() bool {
				var members api.MemberList
				s.expect(&members)(s.client.ListClusterMembers(s.ctx, name, nil))
				if len(members.Items) != 1 || members.Items[0].Autoscaling == nil || members.Items[0].Autoscaling.CpuMode != mode {
					return false
				}
				s.snapshot(name+"-observed-cpu-mode-"+string(mode), members.Items)
				return true
			})
			s.snapshot(name+"-cpu-mode-"+string(mode), updated)
			s.assertMetrics(name)
		}
	})
}

func versionLess(a, b string) bool {
	aa := strings.FieldsFunc(a, func(r rune) bool { return r == '.' || r == '-' })
	bb := strings.FieldsFunc(b, func(r rune) bool { return r == '.' || r == '-' })
	for i := 0; i < len(aa) && i < len(bb); i++ {
		x, _ := strconv.Atoi(aa[i])
		y, _ := strconv.Atoi(bb[i])
		if x != y {
			return x < y
		}
	}
	return len(aa) < len(bb)
}
func (s *suite) upgradeVersions(fromMajor, toMajor string) (string, string) {
	p := s.provider()
	var from, to string
	for _, v := range p.Versions {
		if v.Status != api.ProviderVersionStatusSupported {
			continue
		}
		if v.Major == fromMajor && (from == "" || versionLess(v.Version, from)) {
			from = v.Version
		}
		if v.Major == toMajor && (to == "" || versionLess(to, v.Version)) {
			to = v.Version
		}
	}
	if from == "" || to == "" || !versionLess(from, to) {
		s.t.Fatalf("catalog has no supported %s -> %s upgrade versions", fromMajor, toMajor)
	}
	allowed := false
	for _, path := range p.UpgradePaths {
		if (path.From == from || path.From == fromMajor) && (path.To == to || path.To == toMajor) {
			allowed = true
			break
		}
	}
	if !allowed {
		s.t.Fatalf("catalog does not advertise upgrade path %s -> %s", from, to)
	}
	s.t.Logf("upgrade catalog path=%s -> %s", from, to)
	return from, to
}
func (s *suite) assertVersions(name, want string) {
	for _, m := range s.members(name) {
		if m.Version == nil || !strings.HasPrefix(*m.Version, strings.Split(want, "-")[0]) {
			s.t.Fatalf("member=%s version=%v want=%s", m.Name, m.Version, want)
		}
	}
	s.snapshot(name+"-upgraded-members", s.members(name))
}

type liveWriter struct {
	s                          *suite
	stop                       chan struct{}
	done                       chan error
	once                       sync.Once
	success, retries, failures atomic.Int64
}

func (s *suite) startWriter(name string) *liveWriter {
	w := &liveWriter{s: s, stop: make(chan struct{}), done: make(chan error, 1)}
	initial := s.mongo(name)
	go func() {
		client := initial
		defer func() {
			if client != nil {
				_ = client.Disconnect(context.Background())
			}
		}()
		for id := int64(0); ; id++ {
			select {
			case <-w.stop:
				w.done <- nil
				return
			default:
			}
			// Public routing changes can delay primary connectivity independently
			// of database elections. Retry idempotent upserts within the shared
			// routing grace; assertWriterData checks every acknowledged write.
			deadline := time.Now().Add(publicRoutingGrace)
			for {
				ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
				var err error
				if client == nil {
					client, err = s.dial(ctx, name, "databaseAdmin", "", "", true)
				}
				if err == nil {
					_, err = client.Database("e2e").Collection("continuous").UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": bson.M{"value": "acknowledged"}}, options.UpdateOne().SetUpsert(true))
				}
				cancel()
				if err == nil {
					w.success.Add(1)
					break
				}
				if !retryable(err) {
					w.failures.Add(1)
					w.done <- fmt.Errorf("non-retryable continuous write: %s", s.redact(err.Error()))
					return
				}
				w.retries.Add(1)
				if time.Now().After(deadline) {
					w.done <- fmt.Errorf("continuous write exceeded %s retry deadline", publicRoutingGrace)
					return
				}
				if client != nil {
					_ = client.Disconnect(context.Background())
					client = nil
				}
				select {
				case <-s.ctx.Done():
					w.done <- s.ctx.Err()
					return
				case <-time.After(250 * time.Millisecond):
				}
			}
			select {
			case <-w.stop:
				w.done <- nil
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
	}()
	s.t.Cleanup(w.finish)
	return w
}
func retryable(err error) bool {
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return true
	}
	var server mongo.ServerError
	if errors.As(err, &server) {
		if server.HasErrorLabel("RetryableWriteError") {
			return true
		}
		for _, code := range []int{6, 7, 89, 91, 189, 262, 9001, 10107, 11600, 11602, 13435, 13436} {
			if server.HasErrorCode(code) {
				return true
			}
		}
	}
	return strings.Contains(err.Error(), "has no public endpoint for primary/router")
}
func (w *liveWriter) finish() {
	w.once.Do(func() {
		close(w.stop)
		err := <-w.done
		w.s.t.Logf("continuous writes acknowledged=%d retryableAttempts=%d nonRetryableFailures=%d", w.success.Load(), w.retries.Load(), w.failures.Load())
		if err != nil {
			w.s.t.Errorf("continuous writer: %v", err)
		}
		if w.success.Load() == 0 {
			w.s.t.Error("continuous writer acknowledged no writes")
		}
		if w.failures.Load() != 0 {
			w.s.t.Error("continuous writer had non-retryable failures")
		}
	})
}

func (s *suite) assertWriterData(name string, writer *liveWriter) {
	s.t.Helper()
	c := s.mongo(name)
	defer c.Disconnect(context.Background())
	ctx, cancel := context.WithTimeout(s.ctx, time.Minute)
	defer cancel()
	count, err := c.Database("e2e").Collection("continuous").CountDocuments(ctx, bson.M{})
	if err != nil {
		s.t.Fatal(err)
	}
	if count != writer.success.Load() {
		s.t.Fatalf("continuous-write count=%d acknowledged=%d", count, writer.success.Load())
	}
}

func (s *suite) rollingUpgrade(name, version string) {
	s.t.Helper()
	initial := s.members(name)
	primary := ""
	for _, member := range initial {
		if member.Role == api.MemberRolePrimary {
			primary = member.Name
		}
	}
	if len(initial) != 3 || primary == "" {
		s.t.Fatal("rolling upgrade requires healthy three-member replica set")
	}
	cluster := s.cluster(name)
	cluster.Spec.Version = version
	var updated api.Cluster
	s.expect(&updated)(s.client.ReplaceClusterWithBody(s.ctx, name, &api.ReplaceClusterParams{}, "application/json", s.clusterBody(clusterWriteEnvelope{Spec: cluster.Spec})))
	upgraded := map[string]bool{}
	order := []string{}
	s.poll("secondary-first rolling upgrade", operationTimeout, func() bool {
		members := s.members(name)
		healthy := 0
		for _, member := range members {
			if member.Health {
				healthy++
			}
			if member.Version == nil || !strings.HasPrefix(*member.Version, strings.Split(version, "-")[0]) || upgraded[member.Name] {
				continue
			}
			// A member is counted only after it is healthy on the new binary.
			if !member.Health {
				continue
			}
			if member.Name == primary && len(upgraded) != len(initial)-1 {
				s.t.Fatal("original primary upgraded before all original secondaries")
			}
			upgraded[member.Name] = true
			order = append(order, member.Name)
			s.snapshot(name+"-rolling-upgrade-transition", members)
		}
		if len(members) != 3 || healthy < 2 {
			s.t.Fatalf("rolling upgrade lost voting quorum: members=%d healthy=%d", len(members), healthy)
		}
		state := s.cluster(name)
		if state.Status.Phase == api.ClusterStatusPhaseError {
			s.t.Fatal("rolling upgrade entered Error")
		}
		return len(upgraded) == len(initial) && state.Status.Phase == api.ClusterStatusPhaseReady && state.Status.ObservedGeneration >= updated.Generation
	})
	s.snapshot(name+"-rolling-upgrade-order", order)
	s.assertReplicaMembers(name, 3)
}

func (s *suite) rollingRestart(name string) {
	s.t.Helper()
	// Confirm the captured three endpoints are voters before status stops
	// synchronizing during the restart operation.
	s.assertReplicaMembers(name, 3)
	members := s.members(name)
	if len(members) != 3 {
		s.t.Fatal("rolling restart requires three captured voting members")
	}
	clients := make([]*mongo.Client, len(members))
	for i, member := range members {
		ctx, cancel := context.WithTimeout(s.ctx, publicRoutingGrace)
		clients[i] = s.mongoMember(ctx, name, member, "databaseAdmin")
		cancel()
		defer clients[i].Disconnect(context.Background())
	}
	baseline := s.waitRestartMembers(name+"-rolling-restart-baseline", members, clients)
	s.snapshot(name+"-rolling-restart-baseline", baseline)
	var operation api.Operation
	s.expect(&operation, 202)(s.client.RestartCluster(s.ctx, name, &api.RestartClusterParams{}))
	latest, failed, stop := watchRestartQuorum(s.ctx, members, clients)
	defer stop()
	s.poll("quorum-safe rolling restart", operationTimeout, func() bool {
		select {
		case err := <-failed:
			s.t.Fatal(err)
		default:
		}
		select {
		case samples := <-latest:
			s.snapshot(name+"-rolling-restart-members", samples)
		default:
		}
		s.expect(&operation)(s.client.GetOperation(s.ctx, operation.Id, nil))
		if operation.Phase == api.OperationPhaseFailed {
			s.t.Fatal("rolling restart failed")
		}
		return operation.Phase == api.OperationPhaseSucceeded
	})
	summary := stop()
	s.snapshot(name+"-rolling-restart-probe-summary", summary)
	select {
	case err := <-failed:
		s.t.Fatal(err)
	default:
	}
	if err := summary.requireConclusive(); err != nil {
		s.t.Fatal(err)
	}
	after := s.waitRestartMembers(name+"-rolling-restart-uptimes", members, clients)
	for i, sample := range after {
		if !restartUptimeReset(baseline[i], sample) {
			s.t.Fatalf("rolling restart did not prove member=%s uptime reset: before=%+v after=%+v", sample.Name, baseline[i], sample)
		}
	}
	s.snapshot(name+"-rolling-restart-uptimes", after)
	s.snapshot(name+"-rolling-restart-operation", operation)
	s.ready(name)
}
