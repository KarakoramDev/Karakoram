// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
)

func TestManagedBootstrapRestoresOnlyOwnedAccess(t *testing.T) {
	for _, vhostExists := range []bool{false, true} {
		t.Run(map[bool]string{false: "vhost-missing", true: "vhost-present"}[vhostExists], func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("KARAKORAM_RABBITMQ_BIN", dir)
			fakeCLI(t, "rabbitmqctl", `printf '%s\n' "$*" >> "$CAPTURE/actions"
case "$1" in
list_vhosts)
  if [ "$VHOST_EXISTS" = true ]; then printf '[{"name":"/"},{"name":"customer"}]'; else printf '[{"name":"customer"}]'; fi
  ;;
list_users) printf '[{"user":"admin"},{"user":"customer"}]' ;;
add_user|change_password)
  read -r password
  [ -n "$password" ] || exit 3
  ;;
esac
`)
			runner := New()
			value := "false"
			if vhostExists {
				value = "true"
			}
			runner.env = []string{"CAPTURE=" + dir, "VHOST_EXISTS=" + value}
			cfg := memberConfig(testSpec())
			cfg.SystemUsers = map[string]agentapi.Credentials{
				"admin":     {Username: "admin", Password: "desired-admin-credential"},
				"karakoram": {Username: "karakoram", Password: "desired-control-credential"},
			}
			if err := runner.bootstrap(t.Context(), cfg); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(dir, "actions"))
			if err != nil {
				t.Fatal(err)
			}
			actions := string(data)
			for _, want := range []string{"add_user karakoram", "change_password admin", "set_user_tags karakoram administrator", "set_user_tags admin administrator", "set_permissions --vhost / karakoram .* .* .*", "set_permissions --vhost / admin .* .* .*"} {
				if !strings.Contains(actions, want) {
					t.Fatalf("managed access not repaired: missing %s in %s", want, actions)
				}
			}
			if strings.Contains(actions, "add_vhost /") == vhostExists {
				t.Fatalf("managed vhost absence was not respected: %s", actions)
			}
			if strings.Contains(actions, "customer") || strings.Contains(actions, "desired-admin-credential") || strings.Contains(actions, "desired-control-credential") {
				t.Fatal("customer access changed or credentials exposed in command arguments")
			}
		})
	}
}

func TestManagedBootstrapNamesFailedResourceWithoutSecrets(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("KARAKORAM_RABBITMQ_BIN", dir)
	fakeCLI(t, "rabbitmqctl", `case "$1" in
list_vhosts) printf '[{"name":"/"}]' ;;
list_users) printf '[{"user":"karakoram"},{"user":"admin"}]' ;;
change_password) read -r password ;;
set_permissions) printf 'sensitive server diagnostic' >&2; exit 1 ;;
esac
`)
	cfg := memberConfig(testSpec())
	cfg.SystemUsers = map[string]agentapi.Credentials{
		"admin": {Password: "desired-admin-credential"}, "karakoram": {Password: "desired-control-credential"},
	}
	err := New().bootstrap(t.Context(), cfg)
	if err == nil || !strings.Contains(err.Error(), "managed user karakoram permissions on /") || strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "desired-control-credential") {
		t.Fatalf("native access failure lacked safe resource diagnosis: %v", err)
	}
}
