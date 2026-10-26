package rabbitmq

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestManagedAccessListingReadBudget(t *testing.T) {
	for _, read := range []string{"list_vhosts", "list_users"} {
		t.Run(read, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("KARAKORAM_RABBITMQ_BIN", dir)
			fakeCLI(t, "rabbitmqctl", `printf '%s\n' "$1" >> "$CAPTURE/actions"
if [ "$1" = "$HANG" ]; then exec /bin/sleep 60; fi
case "$1" in
list_vhosts) printf '[{"name":"/"}]' ;;
list_users) printf '[]' ;;
*) exit 19 ;;
esac
`)
			runner := New()
			runner.env = []string{"CAPTURE=" + dir, "HANG=" + read}
			ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
			defer cancel()
			start := time.Now()
			err := runner.bootstrap(ctx, memberConfig(testSpec()))
			if time.Since(start) > 5*time.Second || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "RabbitMQ "+read+" read exceeded 4s budget") || !strings.Contains(err.Error(), "responsiveness/connectivity") {
				t.Fatalf("unbounded or undiagnosed %s: elapsed=%s err=%v", read, time.Since(start), err)
			}
			actions, err := os.ReadFile(filepath.Join(dir, "actions"))
			if err != nil {
				t.Fatal(err)
			}
			for _, action := range strings.Fields(string(actions)) {
				if action != "list_vhosts" && action != "list_users" {
					t.Fatalf("mutation after missing observation: %s", actions)
				}
			}
		})
	}
}

func TestFormationClusterStatusReadBudget(t *testing.T) {
	t.Setenv("KARAKORAM_RABBITMQ_BIN", t.TempDir())
	fakeCLI(t, "rabbitmqctl", "exec /bin/sleep 60\n")
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	start := time.Now()
	_, err := New().status(ctx, nil)
	if time.Since(start) > 5*time.Second || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "RabbitMQ cluster_status read exceeded 4s budget") || !strings.Contains(err.Error(), "responsiveness/connectivity") {
		t.Fatalf("unbounded or undiagnosed formation observation: elapsed=%s err=%v", time.Since(start), err)
	}
}

func TestManagedAccessMutationRetainsAdministrationBudget(t *testing.T) {
	t.Setenv("KARAKORAM_RABBITMQ_BIN", t.TempDir())
	fakeCLI(t, "rabbitmqctl", `case "$1" in
list_vhosts) printf '[{"name":"/"}]' ;;
list_users) printf '[{"user":"karakoram"},{"user":"admin"}]' ;;
change_password)
 read -r password
 if [ "$2" = karakoram ]; then /bin/sleep 5; fi
 ;;
esac
`)
	cfg := memberConfig(testSpec())
	cfg.SystemUsers = map[string]agentapi.Credentials{"admin": {Password: "test-admin"}, "karakoram": {Password: "test-control"}}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := New().bootstrap(ctx, cfg); err != nil {
		t.Fatalf("observation deadline leaked into credential mutation: %v", err)
	}
}
