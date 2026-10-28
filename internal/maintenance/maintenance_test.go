// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package maintenance

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func at(s string) time.Time {
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return v
}
func TestOpen(t *testing.T) {
	tests := []struct {
		name, zone, now string
		windows         []domain.MaintenanceWindow
		open            bool
		end, next       string
	}{
		{name: "nil", now: "2026-01-01T00:00:00Z", open: true},
		{name: "before", now: "2026-01-01T01:59:59Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}, next: "2026-01-01T02:00:00Z"},
		{name: "start inclusive", now: "2026-01-01T02:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}, open: true, end: "2026-01-01T03:00:00Z", next: "2026-01-02T02:00:00Z"},
		{name: "end exclusive", now: "2026-01-01T03:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}, next: "2026-01-02T02:00:00Z"},
		{name: "overlap chain", now: "2026-01-01T02:15:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}, {Schedule: "30 2 * * *", Duration: "2h"}, {Schedule: "30 4 * * *", Duration: "1h"}}, open: true, end: "2026-01-01T05:30:00Z", next: "2026-01-01T02:30:00Z"},
		{name: "zone", zone: "Asia/Kathmandu", now: "2026-01-01T20:15:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}, open: true, end: "2026-01-01T21:15:00Z", next: "2026-01-02T20:15:00Z"},
		{name: "spring elapsed", zone: "America/New_York", now: "2026-03-08T07:30:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "2h"}}, open: true, end: "2026-03-08T08:00:00Z", next: "2026-03-09T05:00:00Z"},
		{name: "spring missing start", zone: "America/New_York", now: "2026-03-08T07:30:00Z", windows: []domain.MaintenanceWindow{{Schedule: "30 2 * * *", Duration: "1h"}}, next: "2026-03-09T06:30:00Z"},
		{name: "fall repeated hour", zone: "America/New_York", now: "2026-11-01T06:45:00Z", windows: []domain.MaintenanceWindow{{Schedule: "30 1 * * *", Duration: "2h"}}, open: true, end: "2026-11-01T08:30:00Z", next: "2026-11-02T06:30:00Z"},
		{name: "dense week", now: "2026-01-01T03:00:01Z", windows: []domain.MaintenanceWindow{{Schedule: "* * * * *", Duration: "168h"}}, open: true, next: "2026-01-01T03:01:00Z"},
		{name: "continuous weekly plus calendar", now: "2026-01-01T03:00:01Z", windows: []domain.MaintenanceWindow{{Schedule: "* * * * *", Duration: "168h"}, {Schedule: "0 0 1 * *", Duration: "1h"}}, open: true, next: "2026-01-01T03:01:00Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p *domain.MaintenancePolicy
			if tt.windows != nil {
				p = &domain.MaintenancePolicy{TimeZone: tt.zone, Windows: tt.windows}
			}
			open, end, next, err := Open(p, at(tt.now))
			if err != nil || open != tt.open {
				t.Fatalf("open=%v err=%v", open, err)
			}
			for _, v := range []struct {
				name, want string
				got        time.Time
			}{{"end", tt.end, end}, {"next", tt.next, next}} {
				if v.want == "" {
					if !v.got.IsZero() {
						t.Errorf("%s=%s, want zero", v.name, v.got)
					}
				} else if !v.got.Equal(at(v.want)) {
					t.Errorf("%s=%s, want %s", v.name, v.got, v.want)
				}
			}
		})
	}
}
func TestValidate(t *testing.T) {
	for _, tt := range []struct {
		name, zone, schedule, duration string
		count                          int
		valid                          bool
	}{
		{"valid", "", "0 2 * * *", "15m", 1, true}, {"max", "UTC", "@weekly", "168h", 16, true}, {"bad zone", "Mars/Base", "0 2 * * *", "1h", 1, false}, {"six fields", "UTC", "0 0 2 * * *", "1h", 1, false}, {"short", "UTC", "* * * * *", "14m", 1, false}, {"long", "UTC", "* * * * *", "169h", 1, false}, {"bad duration", "UTC", "* * * * *", "tomorrow", 1, false}, {"too many", "UTC", "* * * * *", "1h", 17, false}, {"interval", "UTC", "@every 1h", "1h", 1, false}, {"zone override", "UTC", "CRON_TZ=Europe/Prague 0 2 * * *", "1h", 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &domain.MaintenancePolicy{TimeZone: tt.zone}
			for range tt.count {
				p.Windows = append(p.Windows, domain.MaintenanceWindow{Schedule: tt.schedule, Duration: tt.duration})
			}
			if err := Validate(p); (err == nil) != tt.valid {
				t.Fatalf("error=%v valid=%v", err, tt.valid)
			}
		})
	}
}
func TestGated(t *testing.T) {
	for _, kind := range []string{domain.OperationRestart, domain.OperationRotateTLS, domain.OperationUpgradeAgents, domain.OperationRaiseFCV, domain.OperationReplaceMember, domain.OperationBackup, domain.OperationRestore, domain.OperationDelete, domain.OperationRotateSystemUser} {
		for _, actor := range []string{"operator", "system"} {
			op := &domain.Operation{Kind: kind, Actor: actor}
			want := kind == domain.OperationRestart || kind == domain.OperationRotateTLS || kind == domain.OperationUpgradeAgents || kind == domain.OperationRaiseFCV || (kind == domain.OperationReplaceMember && actor != "system")
			if Gated(op) != want {
				t.Errorf("kind=%s actor=%s", kind, actor)
			}
			op.IgnoreMaintenanceWindow = true
			if Gated(op) {
				t.Error("override gated")
			}
		}
	}
}

func TestOpenEmptyPolicyAlwaysOpen(t *testing.T) {
	open, end, next, err := Open(&domain.MaintenancePolicy{TimeZone: "not-a-time-zone"}, time.Time{})
	if err != nil || !open || !end.IsZero() || !next.IsZero() {
		t.Fatal(open, end, next, err)
	}
}

func TestObserveTransition(t *testing.T) {
	for _, tt := range []struct {
		name, zone, now, since string
		windows                []domain.MaintenanceWindow
	}{
		{name: "closed annual", now: "2026-10-28T13:35:00Z", since: "2026-01-01T00:15:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "15m"}}},
		{name: "annual reopening", now: "2027-01-01T00:05:00Z", since: "2027-01-01T00:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 0 1 1 *", Duration: "15m"}}},
		{name: "overlapping open", now: "2026-01-01T04:45:00Z", since: "2026-01-01T02:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}, {Schedule: "30 2 * * *", Duration: "2h"}, {Schedule: "30 4 * * *", Duration: "1h"}}},
		{name: "abutting open", now: "2026-01-01T03:30:00Z", since: "2026-01-01T02:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}, {Schedule: "0 3 * * *", Duration: "1h"}}},
		{name: "exact closing boundary", now: "2026-01-01T03:00:00Z", since: "2026-01-01T03:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 2 * * *", Duration: "1h"}}},
		{name: "century leap gap", now: "2103-06-01T00:00:00Z", since: "2096-02-29T01:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 0 29 2 *", Duration: "1h"}}},
		{name: "spring elapsed", zone: "America/New_York", now: "2026-03-08T07:30:00Z", since: "2026-03-08T06:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 1 * * *", Duration: "2h"}}},
		{name: "continuous", now: "2026-01-01T03:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "* * * * *", Duration: "168h"}}},
		{name: "impossible calendar", now: "2026-01-01T03:00:00Z", windows: []domain.MaintenanceWindow{{Schedule: "0 0 30 2 *", Duration: "1h"}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &domain.MaintenancePolicy{TimeZone: tt.zone, Windows: tt.windows}
			open, end, next, since, err := Observe(p, at(tt.now))
			wantOpen, wantEnd, wantNext, wantErr := Open(p, at(tt.now))
			if err != nil || wantErr != nil || open != wantOpen || !end.Equal(wantEnd) || !next.Equal(wantNext) {
				t.Fatal("observation differs from gate evaluation", open, end, next, err)
			}
			if tt.since == "" {
				if !since.IsZero() {
					t.Fatal("expected unknown boundary", since)
				}
			} else if !since.Equal(at(tt.since)) {
				t.Fatalf("since=%s, want %s", since, tt.since)
			}
		})
	}
}
