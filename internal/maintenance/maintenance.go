// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package maintenance evaluates permission to start new member disruptions.
package maintenance

import (
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/cronutil"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/robfig/cron/v3"
)

type window struct {
	schedule cron.Schedule
	duration time.Duration
	weekly   bool
}

func parse(p *domain.MaintenancePolicy) ([]window, error) {
	if p == nil {
		return nil, nil
	}
	zone := p.TimeZone
	if zone == "" {
		zone = "UTC"
	}
	loc, err := time.LoadLocation(zone)
	if err != nil {
		return nil, field("timeZone", "must name an IANA time zone")
	}
	if len(p.Windows) > 16 {
		return nil, field("windows", "must contain at most 16 windows")
	}
	out := make([]window, 0, len(p.Windows))
	for i, w := range p.Windows {
		prefix := fmt.Sprintf("windows[%d]", i)
		// A per-expression CRON_TZ would silently override the policy's zone.
		if strings.Contains(w.Schedule, "TZ=") {
			return nil, field(prefix+".schedule", "time zone belongs in timeZone")
		}
		s, err := cronutil.Parser().Parse(w.Schedule)
		if err != nil {
			return nil, field(prefix+".schedule", "must be a valid five-field cron schedule")
		}
		spec, ok := s.(*cron.SpecSchedule)
		if !ok {
			return nil, field(prefix+".schedule", "must be a calendar cron schedule, not an interval")
		}
		spec.Location = loc
		d, err := time.ParseDuration(w.Duration)
		if err != nil || d < 15*time.Minute || d > 168*time.Hour {
			return nil, field(prefix+".duration", "must be a Go duration from 15m through 168h")
		}
		// Unrestricted calendar fields repeat weekly (allowing for DST).
		const star = uint64(1) << 63
		fullDOM := spec.Dom&((1<<32)-2) == ((1 << 32) - 2)
		fullDOW := spec.Dow&127 == 127 && spec.Dow&star == 0
		weekly := spec.Month&((1<<13)-2) == ((1<<13)-2) && (spec.Dom&star != 0 || fullDOM || fullDOW)
		out = append(out, window{s, d, weekly})
	}
	return out, nil
}
func field(path, message string) error {
	return &domain.FieldError{Field: "spec.maintenance." + path, Reason: domain.ReasonInvalid, Message: message}
}

// Validate checks the shared cron grammar, time zone, window count and duration.
func Validate(p *domain.MaintenancePolicy) error { _, err := parse(p); return err }

// Open returns the merged window containing now and the next start after now.
// A zero currentEnd denotes continuous weekly coverage with no closing boundary.
// Durations are elapsed time, including across daylight-saving transitions.
func Open(p *domain.MaintenancePolicy, now time.Time) (open bool, currentEnd, nextStart time.Time, err error) {
	if p == nil || len(p.Windows) == 0 {
		return true, time.Time{}, time.Time{}, nil
	}
	windows, err := parse(p)
	if err != nil {
		return false, time.Time{}, time.Time{}, err
	}
	return evaluate(windows, now.UTC())
}

func evaluate(windows []window, now time.Time) (open bool, currentEnd, nextStart time.Time, err error) {
	allWeekly := true
	for _, w := range windows {
		allWeekly = allWeekly && w.weekly
		s := lastStart(w.schedule, now.Add(-w.duration), now)
		if !s.IsZero() {
			end := s.Add(w.duration)
			if end.After(now) && end.After(currentEnd) {
				open = true
				currentEnd = end
			}
		}
		next := w.schedule.Next(now)
		if !next.IsZero() && (nextStart.IsZero() || next.Before(nextStart)) {
			nextStart = next
		}
	}
	if !allWeekly {
		var subset [16]window
		count := 0
		for _, w := range windows {
			if w.weekly {
				subset[count] = w
				count++
			}
		}
		if count > 0 {
			weeklyOpen, end, _, weeklyErr := evaluate(subset[:count], now)
			if weeklyErr != nil {
				return false, time.Time{}, nextStart, weeklyErr
			}
			if weeklyOpen && end.IsZero() {
				return true, time.Time{}, nextStart, nil
			}
		}
	}
	if !open {
		return false, time.Time{}, nextStart, nil
	}
	// Only the latest start inside an already covered interval can extend it.
	// Binary search avoids enumerating every minute in long/dense windows.
	horizon := now.Add(16 * 24 * time.Hour)
	for range 524288 {
		before := currentEnd
		for _, w := range windows {
			s := lastStart(w.schedule, currentEnd.Add(-w.duration), currentEnd)
			if !s.IsZero() && s.Add(w.duration).After(currentEnd) {
				currentEnd = s.Add(w.duration)
			}
		}
		if currentEnd.Equal(before) {
			return true, currentEnd, nextStart, nil
		}
		if allWeekly && currentEnd.After(horizon) {
			return true, time.Time{}, nextStart, nil
		}
	}
	return false, time.Time{}, nextStart, fmt.Errorf("maintenance window recurrence exceeds evaluation bound")
}

// lastStart returns the last cron occurrence in [lo, hi]. The search stops below
// minute resolution: two five-field cron starts cannot fit in that interval.
func lastStart(schedule cron.Schedule, lo, hi time.Time) time.Time {
	first := schedule.Next(lo.Add(-time.Nanosecond))
	if first.IsZero() || first.After(hi) {
		return time.Time{}
	}
	left, right := first, hi
	for range 16 {
		if right.Sub(left) < time.Minute {
			break
		}
		mid := left.Add((right.Sub(left) + 1) / 2)
		candidate := schedule.Next(mid.Add(-time.Nanosecond))
		if !candidate.IsZero() && !candidate.After(hi) {
			first = candidate
			left = mid
		} else {
			right = mid.Add(-time.Nanosecond)
		}
	}
	if candidate := schedule.Next(left.Add(-time.Nanosecond)); !candidate.IsZero() && !candidate.After(hi) {
		return candidate
	}
	return first
}

// Delay bounds polling while still waking at the next opening.
func Delay(now, next time.Time) time.Duration {
	d := 5 * time.Minute
	if !next.IsZero() {
		if until := next.Sub(now); until > 0 && until < d {
			d = until
		}
	}
	return d
}

// Gated reports imperative operations that may start only inside a window.
func Gated(op *domain.Operation) bool {
	if op.IgnoreMaintenanceWindow {
		return false
	}
	switch op.Kind {
	case domain.OperationRestart, domain.OperationRotateTLS, domain.OperationUpgradeAgents, domain.OperationRaiseFCV:
		return true
	case domain.OperationReplaceMember:
		return op.Actor != "system"
	}
	return false
}

// Waiting reports the provider's durable maintenance wait condition.
func Waiting(c *domain.Cluster) bool {
	for _, v := range c.Status.Conditions {
		if v.Type == "MaintenanceWindow" {
			return v.Status == "False" && v.Reason == "WaitingForWindow"
		}
	}
	return false
}
