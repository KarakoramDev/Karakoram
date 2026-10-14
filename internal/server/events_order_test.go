// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func listEventPage(t *testing.T, s *Server, token, query string) api.EventList {
	t.Helper()
	w := call(s, "GET", "/api/v1/events?"+query, "", token)
	if w.Code != 200 {
		t.Fatalf("GET /events?%s = %d %s", query, w.Code, w.Body.String())
	}
	var out api.EventList
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The Events page asks for the newest events; the default stays oldest-first
// so existing cursor-following clients keep working.
func TestListEventsOrder(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	base := listEventPage(t, s, token, "limit=500")
	before := len(base.Items)
	for i := range 5 {
		e := domain.Event{Cluster: "orders", Namespace: domain.DefaultNamespace, Type: "Normal", Reason: "OrderTest", Message: fmt.Sprintf("event %d", i)}
		if err := s.Store.AppendEvent(ctx, &e); err != nil {
			t.Fatal(err)
		}
	}
	ids := func(l api.EventList) []int64 {
		out := make([]int64, 0, len(l.Items))
		for _, e := range l.Items {
			out = append(out, e.Id)
		}
		return out
	}

	asc := listEventPage(t, s, token, "limit=500")
	if len(asc.Items) != before+5 {
		t.Fatalf("ascending page has %d items, want %d", len(asc.Items), before+5)
	}
	for i := 1; i < len(asc.Items); i++ {
		if asc.Items[i].Id <= asc.Items[i-1].Id {
			t.Fatalf("default order is not ascending: %v", ids(asc))
		}
	}
	newest := asc.Items[len(asc.Items)-1].Id

	first := listEventPage(t, s, token, "order=desc&limit=2")
	if len(first.Items) != 2 || first.Items[0].Id != newest || first.Items[1].Id >= first.Items[0].Id {
		t.Fatalf("first descending page = %v, want newest %d first", ids(first), newest)
	}
	if first.NextCursor == nil {
		t.Fatal("descending page without next cursor")
	}
	second := listEventPage(t, s, token, "order=desc&limit=2&cursor="+url.QueryEscape(*first.NextCursor))
	if len(second.Items) != 2 || second.Items[0].Id >= first.Items[1].Id || second.Items[1].Id >= second.Items[0].Id {
		t.Fatalf("second descending page = %v after %v", ids(second), ids(first))
	}

	if w := call(s, "GET", "/api/v1/events?order=sideways", "", token); w.Code != 400 {
		t.Fatalf("invalid order = %d, want 400", w.Code)
	}
}
