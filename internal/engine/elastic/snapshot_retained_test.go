// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestRetainedCatalogRequiresMatchingLastSnapshot(t *testing.T) {
	for _, tc := range []struct {
		catalog string
		allowed bool
	}{
		{`{"snapshots":[{"name":"snapshot-one","uuid":"unique"}]}`, true},
		{`{"snapshots":[{"name":"snapshot-one"},{"name":"snapshot-two"}]}`, false},
		{`{"snapshots":[{"name":"snapshot-two"}]}`, false},
		{`{"snapshots":[]}`, false},
		{`not-json`, false},
	} {
		if err := validateRetainedCatalog([]byte(tc.catalog), "snapshot-one"); (err == nil) != tc.allowed {
			t.Fatalf("catalog %s: %v", tc.catalog, err)
		}
	}
}

func TestRetainedDeletionRejectsBroadNamespaceBeforeS3(t *testing.T) {
	storage := domain.BackupStorage{Namespace: domain.DefaultNamespace, Name: "archive", Bucket: "backups", Prefix: "edited"}
	p := New(ProviderOptions{})
	for _, base := range []string{"saved/default", "saved/default/test", "saved/other/test/uid", "saved/default/other/uid", "saved/default/test/uid/extra", "saved/default/test/../uid", "saved/default/test/uid//"} {
		t.Run(base, func(t *testing.T) {
			b := domain.Backup{Namespace: domain.DefaultNamespace, Cluster: "test", Name: "saved", Storage: "archive", Provider: "elasticsearch", Location: snapshotLocation(&storage, base, "kk-archive", "snapshot-one")}
			for _, attempt := range []func(context.Context, domain.BackupStorage, domain.Backup) error{p.ValidateRetainedBackupDeletion, p.DeleteRetainedBackup} {
				err := attempt(t.Context(), storage, b)
				var refusal *engine.RetainedBackupDeletionRefusal
				var resource *domain.ResourceError
				if !errors.As(err, &refusal) || !errors.As(err, &resource) || !errors.Is(err, engine.ErrConflict) {
					t.Fatalf("unsafe prefix did not return a public deterministic refusal: %v", err)
				}
			}
		})
	}
}

func TestRetainedCatalogPreflightAndDeletionRefuseBeforeMutation(t *testing.T) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	address := ""
	for _, addr := range addresses {
		ip, _, _ := net.ParseCIDR(addr.String())
		if ip != nil && ip.To4() != nil && ip.IsPrivate() {
			address = ip.String()
			break
		}
	}
	if address == "" {
		t.Skip("private test interface unavailable")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(address, "0"))
	if err != nil {
		t.Fatal(err)
	}
	const base = "saved/default/source/immutable"
	catalog := `{"snapshots":[{"name":"one"},{"name":"two"}]}`
	mutations := 0
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			mutations++
			w.WriteHeader(500)
			return
		}
		if r.URL.Query().Has("list-type") {
			if r.URL.Query().Get("prefix") != base+"/" {
				t.Error("listing escaped immutable repository", r.URL.RawQuery)
			}
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%s/index-1</Key><Size>%d</Size></Contents><Contents><Key>%s/blob</Key><Size>1</Size></Contents></ListBucketResult>`, base, len(catalog), base)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/index-1") {
			w.Header().Set("Content-Length", fmt.Sprint(len(catalog)))
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
			fmt.Fprint(w, catalog)
			return
		}
		w.WriteHeader(404)
	}))
	server.Listener = listener
	server.Start()
	defer server.Close()
	t.Setenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST", listener.Addr().String())
	storage := domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: server.URL, Bucket: "backups", Prefix: "saved", Credentials: domain.StorageCredentials{AccessKeyID: "test-access", SecretAccessKey: "test-key"}}
	b := domain.Backup{Namespace: "default", Cluster: "source", Name: "saved", Storage: "archive", Provider: "elasticsearch", Phase: domain.PhaseSucceeded, Location: snapshotLocation(&storage, base, "kk-archive", "one")}
	p := New(ProviderOptions{})
	for _, prefix := range []string{"saved", "edited", "", "edited/nested"} {
		t.Run("storage-prefix="+prefix, func(t *testing.T) {
			current := storage
			current.Prefix = prefix
			for _, attempt := range []func(context.Context, domain.BackupStorage, domain.Backup) error{p.ValidateRetainedBackupDeletion, p.DeleteRetainedBackup} {
				err := attempt(t.Context(), current, b)
				var refusal *engine.RetainedBackupDeletionRefusal
				var resource *domain.ResourceError
				if !errors.As(err, &refusal) || !errors.As(err, &resource) || !errors.Is(err, engine.ErrConflict) || !strings.Contains(resource.Message, "other snapshots remain") {
					t.Fatal("catalog refusal lost its safe public contract after prefix edit", err)
				}
			}
		})
	}
	if mutations != 0 {
		t.Fatal("refused repository was mutated", mutations)
	}
}
