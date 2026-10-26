package rabbitmq

import (
	"context"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestFormationObservationReadBudget(t *testing.T) {
	p, rc, _, _ := setup(t)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	defer server.Close()
	rc.Agents = coldObservationAgents{options: agentapi.ClientOptions{
		BaseURL: server.URL, Token: "test-token", CAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}),
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	start := time.Now()
	_, err := p.formationStatus(ctx, rc, "demom0")
	if time.Since(start) > 5*time.Second || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "RabbitMQ formation cluster_status read exceeded 4s budget") || !strings.Contains(err.Error(), "inspect member demom0 responsiveness/connectivity") {
		t.Fatalf("unbounded or undiagnosed formation read: elapsed=%s err=%v", time.Since(start), err)
	}
}
