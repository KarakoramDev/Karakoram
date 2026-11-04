// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestCloneDiagnosticReachesControlPlaneClient(t *testing.T) {
	for _, mode := range []string{"request", "authorization", "recipient-preflight"} {
		t.Run(mode, func(t *testing.T) {
			s := testCloneSpec()
			d := &cloneTestDeps{rejectedGroupDeps: rejectedGroupDeps{s: s, dir: t.TempDir()}}
			req := myspec.CloneRequest{Receipt: "restore/clone/member", Version: s.Version, Donor: *s.GroupReplication.CloneDonor}
			want := "MySQL clone failed: stage=" + mode
			switch mode {
			case "request":
				req.Receipt = ""
			case "authorization":
				req.Version = "8.0.46-37"
			case "recipient-preflight":
				d.localErr = &mysqldriver.MySQLError{Number: 1045, Message: "Access denied PASSWORD='obsolete-unrecorded-secret' at private-host\nCLONE INSTANCE IDENTIFIED BY 'current-secret'"}
				want += "; MySQL 1045"
			}
			mux := http.NewServeMux()
			RegisterGroupRoutes(mux, d)
			server := httptest.NewTLSServer(mux)
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			client, err := agentapi.NewClient(agentapi.ClientOptions{BaseURL: server.URL, Token: "diagnostic-agent-token", CAPEM: ca})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			err = client.Engine(t.Context(), "mysql", myspec.ActionClone, req, nil)
			var remote *agentapi.HTTPError
			if !errors.As(err, &remote) || remote.StatusCode != http.StatusServiceUnavailable || remote.Message != want {
				t.Fatalf("missing categorized clone diagnosis: %v", err)
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatal("control-plane event error loses category")
			}
		})
	}
}

func TestCloneDiagnosticNeverIncludesUntrustedText(t *testing.T) {
	secret := "obsolete-password\nCLONE INSTANCE FROM user@host IDENTIFIED BY 'private-key-and-token'"
	for _, stage := range []string{"request", "authorization", "receipt", "receipt-recovery", "recipient-preflight", "donor-probe", "native-baseline", "donor-configuration", "receipt-write", "native-clone", "completion"} {
		t.Run(stage, func(t *testing.T) {
			native := &mysqldriver.MySQLError{Number: 3862, Message: secret}
			err := &cloneActionError{stage: stage, cause: fmt.Errorf("%s: %w", secret, native)}
			if got, want := cloneActionDiagnostic(err), "MySQL clone failed: stage="+stage+"; MySQL 3862"; got != want {
				t.Fatalf("diagnostic mismatch: %q", got)
			}
			var unwrapped *mysqldriver.MySQLError
			if !errors.As(err, &unwrapped) || unwrapped != native {
				t.Fatal("native error chain lost")
			}
		})
	}
	if got := cloneActionDiagnostic(&cloneActionError{stage: secret, cause: errors.New(secret)}); got != "MySQL clone failed: stage=unknown" {
		t.Fatal("untrusted stage escaped allowlist")
	}
	for _, test := range []struct {
		err  error
		want string
	}{
		{context.DeadlineExceeded, "deadline exceeded"},
		{context.Canceled, "canceled"},
		{errCloneCopyStarted, "native clone failed; file copy is not proven unstarted"},
	} {
		err := &cloneActionError{stage: "completion", cause: fmt.Errorf("%s: %w", secret, test.err)}
		if !errors.Is(err, test.err) || cloneActionDiagnostic(err) != "MySQL clone failed: stage=completion; "+test.want {
			t.Fatal("cancellation category or error chain lost")
		}
	}
}
