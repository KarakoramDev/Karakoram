// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func (s *Server) eventAllowed(ctx context.Context, e domain.Event) bool {
	obj := "*"
	resource := "audit"
	namespace := e.Namespace
	if e.Cluster != "" {
		namespace = domain.NormalizedNamespace(namespace)
		obj = clusterObject(namespace, e.Cluster)
		resource = "events"
	} else if namespace != "" {
		obj = namespace
	}
	permission := s.allowed(ctx, resource, "read", obj)
	if resource == "events" {
		permission = permission || s.allowed(ctx, "audit", "read", obj)
	}
	q := request(ctx).URL.Query()
	return permission && (q.Get("namespace") == "" || q.Get("namespace") == namespace) && (q.Get("cluster") == "" || q.Get("cluster") == e.Cluster) && (q.Get("type") == "" || q.Get("type") == e.Type) && (q.Get("operationId") == "" || q.Get("operationId") == e.Operation) && (q.Get("actor") == "" || q.Get("actor") == e.Actor) && (q.Get("resource") == "" || q.Get("resource") == e.Resource)
}
func (s *Server) observability(ctx context.Context, name string, in input) response {
	q := request(ctx).URL.Query()
	if failure := s.validateObservationFilters(ctx, name); failure != nil {
		return *failure
	}
	switch name {
	case "ListOperations":
		items, err := s.Store.ListOperations(ctx, domain.ClusterRef{Namespace: namespaceFilter(ctx)})
		out := make([]api.Operation, 0, len(items))
		for _, o := range items {
			if (q.Get("cluster") == "" || q.Get("cluster") == o.Cluster) && s.allowed(ctx, "operations", "read", clusterObject(o.Namespace, o.Cluster)) && (q.Get("phase") == "" || q.Get("phase") == string(o.Phase)) {
				out = append(out, operationAPI(o))
			}
		}
		return listResponse(s, ctx, out, err)
	case "GetOperation":
		o, err := s.Store.GetOperation(ctx, in.Operation)
		if err != nil {
			return s.failure(err)
		}
		if !s.allowed(ctx, "operations", "read", clusterObject(o.Namespace, o.Cluster)) {
			return fail(403, "forbidden", "Permission denied")
		}
		return ok(operationAPI(*o))
	case "CancelOperation":
		op, err := s.Store.GetOperation(ctx, in.Operation)
		if err != nil {
			return s.failure(err)
		}
		if !s.allowed(ctx, "clusters", "update", clusterObject(op.Namespace, op.Cluster)) {
			return fail(403, "forbidden", "Cluster write permission is required to cancel operation "+op.ID)
		}
		if op.Terminal() {
			return failDetails(409, "conflict", "Operation "+op.ID+" is "+string(op.Phase)+" and cannot be cancelled", map[string]any{"resource": "operation", "name": op.ID, "reason": domain.ReasonInvalidState})
		}
		if op.Phase == domain.PhaseRunning {
			c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster})
			if err != nil {
				return s.failure(err)
			}
			p, err := s.Registry.Get(c.Spec.Provider)
			if err != nil {
				return s.failure(err)
			}
			canceller, ok := p.(engine.OperationCanceller)
			if !ok || domain.IsConvergenceOperation(op.Kind) {
				return failDetails(409, "conflict", "Operation "+op.ID+" ("+op.Kind+") cannot be cancelled safely once Running; accepted desired state and deletion are not rolled back", map[string]any{"resource": "operation", "name": op.ID, "reason": domain.ReasonInvalidState})
			}
			if err = canceller.CanCancelOperation(ctx, op); err != nil {
				return failDetails(409, "conflict", err.Error(), map[string]any{"resource": "operation", "name": op.ID, "reason": domain.ReasonInvalidState})
			}
		}
		cancelled, err := s.Store.RequestOperationCancellation(ctx, op.ID, op)
		if err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster})
		return response{status: 202, body: operationAPI(*cancelled), operationID: op.ID}
	case "ListEvents":
		limit := 100
		if raw := q.Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > 500 {
				return invalidField("limit", domain.ReasonOutOfRange, "limit must be an integer from 1 to 500")
			}
			limit = n
		}
		position, err := eventCursor(q.Get("cursor"))
		if err != nil {
			return invalidField("cursor", domain.ReasonInvalid, "cursor must be a cursor returned by the preceding page or a nonnegative event ID")
		}
		descending := false
		switch q.Get("order") {
		case "", "asc":
		case "desc":
			descending = true
		default:
			return invalidField("order", domain.ReasonInvalid, "order must be asc or desc")
		}
		out := page[api.Event]{Items: make([]api.Event, 0, limit)}
		for {
			filter := store.EventFilter{Namespace: namespaceFilter(ctx), Cluster: q.Get("cluster"), Type: q.Get("type"), Actor: q.Get("actor"), Resource: q.Get("resource"), Limit: 1000}
			if descending {
				filter.Descending, filter.BeforeID = true, position
			} else {
				filter.SinceID = position
			}
			items, err := s.Store.ListEvents(ctx, filter)
			if err != nil {
				return s.failure(err)
			}
			for _, e := range items {
				position = e.ID
				if !s.eventAllowed(ctx, e) {
					continue
				}
				if len(out.Items) == limit {
					cursor := base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(out.Items[len(out.Items)-1].Id, 10)))
					out.NextCursor = &cursor
					return ok(out)
				}
				out.Items = append(out.Items, eventAPI(e))
			}
			if len(items) < 1000 {
				return ok(out)
			}
		}
	case "StreamEvents":
		return s.stream(ctx)
	case "GetPrometheusMetrics":
		if s.Monitoring == nil {
			return fail(503, "unavailable", "Monitoring unavailable")
		}
		return response{noStore: true, serve: func(w http.ResponseWriter) error {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
			return s.Monitoring.WritePrometheus(ctx, w, func(ref domain.ClusterRef) bool {
				return (namespaceFilter(ctx) == "" || namespaceFilter(ctx) == domain.NormalizedNamespace(ref.Namespace)) && s.allowed(ctx, "monitoring", "read", clusterObject(ref.Namespace, ref.Name))
			})
		}}
	case "GetClusterMetrics":
		if !s.allowed(ctx, "monitoring", "read", clusterObject(clusterRef(ctx, in.Cluster).Namespace, in.Cluster)) {
			return fail(403, "forbidden", "Permission denied")
		}
		if _, err := s.Store.GetCluster(ctx, clusterRef(ctx, in.Cluster)); err != nil {
			return s.failure(err)
		}
		if s.Monitoring == nil {
			return fail(503, "unavailable", "Monitoring unavailable")
		}
		lookback, step, err := metricsDurations(q.Get("range"), q.Get("step"))
		if err != nil {
			return s.failure(err)
		}
		end := time.Now().UTC()
		start := end.Add(-lookback)
		metrics := []string{}
		if v := q.Get("metric"); v != "" {
			metrics = strings.Split(v, ",")
		}
		series, err := s.Monitoring.Series(ctx, clusterRef(ctx, in.Cluster), metrics, start, end, step)
		if err != nil {
			return s.failure(err)
		}
		a := api.ClusterMetrics{Start: start, End: end, Step: step.String(), Series: []api.MetricSeries{}}
		for _, v := range series {
			m := api.MetricSeries{Name: v.Name, Member: v.Member, Unit: v.Unit, Labels: &v.Labels, Points: make([]api.MetricPoint, 0, len(v.Points))}
			for _, p := range v.Points {
				m.Points = append(m.Points, api.MetricPoint{Timestamp: p.Timestamp, Value: float32(p.Value)})
			}
			a.Series = append(a.Series, m)
		}
		return ok(a)
	}
	return fail(500, "internal_error", "Unknown observability operation")
}
func eventCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil {
		raw, decodeErr := base64.RawURLEncoding.DecodeString(cursor)
		if decodeErr != nil {
			return 0, decodeErr
		}
		n, err = strconv.ParseInt(string(raw), 10, 64)
	}
	if n < 0 {
		return 0, fmt.Errorf("negative event cursor")
	}
	return n, err
}

// A large, incompressible comment encourages public proxies to flush even before
// the first event. Generated once, it carries no ID or durable replay cursor.
var sseConnectedComment = paddedSSEComment("connected")

// Cycling three distinct comments exceeds gzip's 32 KiB history without allocating
// fresh padding on every heartbeat. Padding never advances the event cursor.
var sseHeartbeatComments = [3]string{
	paddedSSEComment("heartbeat"),
	paddedSSEComment("heartbeat"),
	paddedSSEComment("heartbeat"),
}

func paddedSSEComment(name string) string {
	var padding [12 << 10]byte
	if _, err := rand.Read(padding[:]); err != nil {
		panic("generate SSE transport padding: " + err.Error())
	}
	return ": " + name + " " + base64.StdEncoding.EncodeToString(padding[:]) + "\n\n"
}

func (s *Server) stream(ctx context.Context) response {
	q := request(ctx).URL.Query()
	cursor := q.Get("cursor")
	if cursor == "" {
		cursor = request(ctx).Header.Get("Last-Event-ID")
	}
	since, err := eventCursor(cursor)
	if err != nil {
		return invalidField("cursor", domain.ReasonInvalid, "cursor must be a cursor returned by the preceding page or a nonnegative event ID")
	}
	return response{noStore: true, serve: func(w http.ResponseWriter) error {
		ch, cancel := s.Hub.Subscribe(namespaceFilter(ctx), q.Get("cluster"))
		defer cancel()
		controller := http.NewResponseController(w)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Accel-Buffering", "no")
		w.Header().Set("Cache-Control", "no-cache, no-transform")
		if _, err := fmt.Fprint(w, sseConnectedComment); err != nil {
			return err
		}
		if err := controller.Flush(); err != nil {
			return err
		}
		revalidate := func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			p, err := s.Auth.Authenticate(request(ctx))
			if err != nil {
				if canceled := ctx.Err(); canceled != nil {
					return canceled
				}
				return err
			}
			if p.MustChangePassword {
				return auth.ErrPasswordChangeRequired
			}
			ctx = context.WithValue(ctx, identityKey{}, p)
			return nil
		}
		send := func(e domain.Event) error {
			if err := revalidate(); err != nil {
				return err
			}
			if e.ID <= since {
				return nil
			}
			since = e.ID
			if !s.eventAllowed(ctx, e) {
				return nil
			}
			data, err := json.Marshal(eventAPI(e))
			if err != nil {
				return err
			}
			typ := strings.NewReplacer("\n", "", "\r", "").Replace(e.Type)
			if _, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.ID, typ, data); err != nil {
				return err
			}
			return controller.Flush()
		}
		// Hub notifications are only wakeups: durable IDs, not publication order,
		// define the cursor. Heartbeats also drain events dropped by a full hub.
		drain := func() error {
			if err := revalidate(); err != nil {
				return err
			}
			for {
				items, err := s.Store.ListEvents(ctx, store.EventFilter{Namespace: namespaceFilter(ctx), SinceID: since, Limit: 1000})
				if err != nil {
					return err
				}
				for _, e := range items {
					if err = send(e); err != nil {
						return err
					}
				}
				if len(items) < 1000 {
					return nil
				}
			}
		}
		if err := drain(); err != nil {
			return err
		}
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		heartbeat := 0
		for {
			select {
			case <-ctx.Done():
				return nil
			case _, open := <-ch:
				if !open {
					return nil
				}
				if err := drain(); err != nil {
					return err
				}
			case <-tick.C:
				if err := drain(); err != nil {
					return err
				}
				if _, err := fmt.Fprint(w, sseHeartbeatComments[heartbeat]); err != nil {
					return err
				}
				heartbeat = (heartbeat + 1) % len(sseHeartbeatComments)
				if err := controller.Flush(); err != nil {
					return err
				}
			}
		}
	}}
}
