// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

func TestProxyDrainRetirementSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var actions []string
		queries := 0
		query := func(_ context.Context, sql string) ([]tableRow, error) {
			actions = append(actions, sql)
			switch sql {
			case "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers":
				return []tableRow{{"hostgroup_id": "10", "status": "ONLINE"}}, nil
			case "PROXYSQL PAUSE":
				return nil, nil
			default:
				if time.Since(start) < proxyDrainDetectionWindow {
					t.Fatal("session polling preceded LB detection window")
				}
				queries++
				count := "1"
				if queries == 3 {
					count = "0"
				}
				return []tableRow{{"Variable_Value": count}}, nil
			}
		}
		if err := drainProxyQueries(t.Context(), &pgspec.Proxy{Servers: []string{"db"}}, slog.Default(), query); err != nil {
			t.Fatal(err)
		}
		if actions[1] != "PROXYSQL PAUSE" || queries != 3 || time.Since(start) != 10500*time.Millisecond {
			t.Fatalf("wrong retirement sequence/time: %v %v", actions, time.Since(start))
		}
		for _, forbidden := range []string{"PROXYSQL STOP", "PROXYSQL KILL", "PROXYSQL RESUME"} {
			if slices.Contains(actions, forbidden) {
				t.Fatal("drain disrupted established sessions", forbidden)
			}
		}
	})
}

func TestProxyDrainBoundedWithPersistentSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var logs bytes.Buffer
		ctx, cancel := context.WithTimeout(t.Context(), proxyDrainTimeout)
		defer cancel()
		query := func(_ context.Context, sql string) ([]tableRow, error) {
			switch sql {
			case "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers":
				return []tableRow{{"hostgroup_id": "10", "status": "ONLINE"}}, nil
			case "PROXYSQL PAUSE":
				return nil, nil
			default:
				return []tableRow{{"Variable_Value": "1"}}, nil
			}
		}
		if err := drainProxyQueries(ctx, &pgspec.Proxy{Servers: []string{"db"}}, slog.New(slog.NewTextHandler(&logs, nil)), query); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if time.Since(start) != proxyDrainTimeout {
			t.Fatal("unbounded retirement", time.Since(start))
		}
		if !strings.Contains(logs.String(), "proxy drain bound reached") || !strings.Contains(logs.String(), "context deadline exceeded") {
			t.Fatal("bounded disconnect was not reported", logs.String())
		}
	})
}

func TestProxyDrainSkipsUnhealthyAndFailedPause(t *testing.T) {
	for _, mode := range []string{"unhealthy", "bootstrap", "pause-error", "status-error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				calls := 0
				failed := errors.New("native unavailable")
				query := func(_ context.Context, sql string) ([]tableRow, error) {
					calls++
					if calls == 1 {
						if mode == "status-error" {
							return nil, failed
						}
						status := "ONLINE"
						if mode == "unhealthy" {
							status = "SHUNNED"
						}
						return []tableRow{{"hostgroup_id": "10", "status": status}}, nil
					}
					if sql != "PROXYSQL PAUSE" || mode != "pause-error" {
						t.Fatal("unexpected native command", sql)
					}
					return nil, failed
				}
				p := &pgspec.Proxy{Servers: []string{"db"}}
				if mode == "bootstrap" {
					p.Servers = nil
				}
				err := drainProxyQueries(t.Context(), p, slog.Default(), query)
				if (mode == "pause-error" || mode == "status-error") != errors.Is(err, failed) {
					t.Fatal(err)
				}
				if !time.Now().Equal(start) {
					t.Fatal("failed/unhealthy proxy delayed termination")
				}
			})
		})
	}
}

func TestProxyDrainBudgetDoesNotExtendDataHandoff(t *testing.T) {
	if proxyDrainTimeout != 25*time.Second {
		t.Fatal("proxy hook must leave margin within default Nomad 30-second client grace")
	}
	e := New()
	for _, role := range []string{pgspec.RoleData, pgspec.RoleProxy} {
		e.mu.Lock()
		e.current = pgspec.Spec{Role: role}
		e.mu.Unlock()
		want := 4 * time.Second
		if role == pgspec.RoleProxy {
			want = proxyDrainTimeout
		}
		if got := e.ShutdownBudget(); got != want {
			t.Fatalf("role %s shutdown budget=%v want %v", role, got, want)
		}
	}
}

func TestProxyDrainStalledAdminBudgets(t *testing.T) {
	for _, stage := range []string{"health", "pause"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				ctx, cancel := context.WithTimeout(t.Context(), proxyDrainTimeout)
				defer cancel()
				query := func(ctx context.Context, sql string) ([]tableRow, error) {
					if stage == "health" || sql == "PROXYSQL PAUSE" {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return []tableRow{{"hostgroup_id": "10", "status": "ONLINE"}}, nil
				}
				err := drainProxyQueries(ctx, &pgspec.Proxy{Servers: []string{"db"}}, slog.Default(), query)
				want := proxyDrainTimeout
				if stage == "health" {
					want = 3 * time.Second
				}
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != want {
					t.Fatal("stalled admin exceeded stage budget", err, time.Since(start))
				}
			})
		})
	}
}

func TestProxyDrainPreservesTwentySecondSession(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var logs bytes.Buffer
		log := slog.New(slog.NewTextHandler(&logs, nil))
		query := func(ctx context.Context, sql string) ([]tableRow, error) {
			switch sql {
			case "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers":
				return []tableRow{{"hostgroup_id": "10", "status": "ONLINE"}}, nil
			case "PROXYSQL PAUSE":
				// Listener removal synchronously waits for native workers. An
				// acknowledgement after the health-query budget is still valid.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(4 * time.Second):
					return nil, nil
				}
			case "SELECT Variable_Value FROM stats_pgsql_global WHERE Variable_Name='Client_Connections_connected'":
				// Model an established session running a 20-second transaction,
				// committing and disconnecting. It remains connected, not zero,
				// while executing or idle in the transaction.
				count := "1"
				if time.Since(start) >= 20*time.Second {
					count = "0"
				}
				return []tableRow{{"Variable_Value": count}}, nil
			default:
				t.Fatal("unexpected native command", sql)
				return nil, nil
			}
		}
		if err := drainProxyQueries(t.Context(), &pgspec.Proxy{Servers: []string{"db"}}, log, query); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 20*time.Second {
			t.Fatal("terminated before established session finished", time.Since(start))
		}
		for _, want := range []string{"proxy drain paused", "connected=1", "connected=0", "proxy drain finished", "paused=true", "error=<nil>"} {
			if !strings.Contains(logs.String(), want) {
				t.Fatalf("missing drain evidence %q: %s", want, logs.String())
			}
		}
		if strings.Count(logs.String(), "proxy drain sessions") != 2 {
			t.Fatal("unchanged counts should not flood logs", logs.String())
		}
	})
}

func TestProxyDrainRejectsUnknownSessionCount(t *testing.T) {
	for _, value := range []string{"", "-1", "unknown"} {
		t.Run(value, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs bytes.Buffer
				query := func(_ context.Context, sql string) ([]tableRow, error) {
					switch sql {
					case "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers":
						return []tableRow{{"hostgroup_id": "10", "status": "ONLINE"}}, nil
					case "PROXYSQL PAUSE":
						return nil, nil
					default:
						return []tableRow{{"Variable_Value": value}}, nil
					}
				}
				err := drainProxyQueries(t.Context(), &pgspec.Proxy{Servers: []string{"db"}}, slog.New(slog.NewTextHandler(&logs, nil)), query)
				if err == nil || !strings.Contains(logs.String(), "proxy drain session count invalid") {
					t.Fatal("unknown count reported as successful drain", err, logs.String())
				}
			})
		})
	}
}

func TestProxyDrainReportsDeadlineDuringSanitizedCountQuery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), proxyDrainTimeout)
		defer cancel()
		var logs bytes.Buffer
		sanitized := errors.New("ProxySQL admin query failed")
		query := func(ctx context.Context, sql string) ([]tableRow, error) {
			switch sql {
			case "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers":
				return []tableRow{{"hostgroup_id": "10", "status": "ONLINE"}}, nil
			case "PROXYSQL PAUSE":
				return nil, nil
			default:
				<-ctx.Done()
				// queryTable deliberately hides the underlying native error.
				return nil, sanitized
			}
		}
		err := drainProxyQueries(ctx, &pgspec.Proxy{Servers: []string{"db"}}, slog.New(slog.NewTextHandler(&logs, nil)), query)
		if !errors.Is(err, sanitized) || !strings.Contains(logs.String(), "proxy drain bound reached") {
			t.Fatal("sanitized count-query timeout hid drain truncation", err, logs.String())
		}
	})
}
