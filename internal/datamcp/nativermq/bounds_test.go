package nativermq

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestResponseBoundsAndRedirects(t *testing.T) {
	for _, kind := range []string{"large", "invalid", "redirect"} {
		t.Run(kind, func(t *testing.T) {
			requests := 0
			u := openTest(t, func(w http.ResponseWriter, r *http.Request) {
				requests++
				switch kind {
				case "large":
					_, _ = w.Write([]byte(`{"data":"` + strings.Repeat("x", 4<<20) + `"}`))
				case "invalid":
					_, _ = w.Write([]byte(`secret invalid JSON`))
				case "redirect":
					w.Header().Set("Location", "/credential-leak")
					w.WriteHeader(http.StatusFound)
				}
			}, engine.DataAccessRead)
			_, err := u.CallTool(context.Background(), "overview", map[string]any{"vhost": "/"})
			if err == nil || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
			if requests != 1 {
				t.Fatalf("followed redirect: %d requests", requests)
			}
		})
	}
}

func TestResourcePathValidation(t *testing.T) {
	u := openTest(t, func(w http.ResponseWriter, r *http.Request) { t.Error("unsafe resource reached API") }, engine.DataAccessReadWrite)
	for _, a := range []map[string]any{{"vhost": "..", "queue": "q"}, {"vhost": "v", "queue": ".."}, {"vhost": "v", "queue": "."}, {"vhost": "v", "queue": ""}} {
		if _, err := u.CallTool(context.Background(), "queue_get", a); err == nil {
			t.Fatal(a)
		}
	}
	if _, err := u.CallTool(context.Background(), "message_publish", map[string]any{"vhost": "v", "exchange": "..", "routing_key": "q", "payload": "p"}); err == nil {
		t.Fatal("unsafe exchange accepted")
	}
}
