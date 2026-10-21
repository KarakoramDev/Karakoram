// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func repositoryEndpoint(s myspec.S3) (*url.URL, error) {
	endpoint := s.Endpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if s.Port != 0 {
		u.Host = net.JoinHostPort(u.Hostname(), fmt.Sprint(s.Port))
	}
	return u, nil
}
func s3Client(r myspec.Repository) (*minio.Client, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	u, err := repositoryEndpoint(r.S3)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !r.S3.VerifyTLS} // Explicit repository policy.
	if r.S3.CAPEM != "" {
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM([]byte(r.S3.CAPEM)) {
			return nil, errors.New("repository_ca")
		}
		tlsConfig.RootCAs = roots
	}
	transport.TLSClientConfig = tlsConfig
	lookup := minio.BucketLookupDNS
	if r.S3.PathStyle {
		lookup = minio.BucketLookupPath
	}
	return minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(r.S3.AccessKey, r.S3.SecretKey, ""), Secure: u.Scheme == "https", Region: r.S3.Region, BucketLookup: lookup, Transport: transport})
}

// cloudConfig uses xbcloud's documented defaults-file support. Credentials never
// enter argv or the process environment, including when native tools fail.
func cloudConfig(r myspec.Repository, dir string) (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	u, err := repositoryEndpoint(r.S3)
	if err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, ".xbcloud-*.cnf")
	if err != nil {
		return "", err
	}
	name := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(name)
		}
	}()
	text := "[xbcloud]\nstorage=s3\ns3-api-version=4\nparallel=2\n"
	for _, kv := range [][2]string{{"s3-endpoint", u.String()}, {"s3-bucket", r.S3.Bucket}, {"s3-region", r.S3.Region}, {"s3-access-key", r.S3.AccessKey}, {"s3-secret-key", r.S3.SecretKey}} {
		text += kv[0] + "=" + optionQuote(kv[1]) + "\n"
	}
	if r.S3.PathStyle {
		text += "s3-bucket-lookup=PATH\n"
	} else {
		text += "s3-bucket-lookup=DNS\n"
	}
	if !r.S3.VerifyTLS {
		text += "insecure\n"
	}
	if r.S3.CAPEM != "" {
		ca := name + ".pem"
		if err = os.WriteFile(ca, []byte(r.S3.CAPEM), 0600); err != nil {
			return "", err
		}
		text += "cacert=" + optionQuote(ca) + "\n"
	}
	if err = f.Chmod(0600); err == nil {
		_, err = f.WriteString(text)
	}
	if err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	ok = true
	return name, nil
}
func removeCloudConfig(name string) { _ = os.Remove(name); _ = os.Remove(name + ".pem") }
func downloadFile(ctx context.Context, c *minio.Client, r myspec.Repository, key, name string) error {
	return c.FGetObject(ctx, r.S3.Bucket, key, name, minio.GetObjectOptions{})
}
func metadataObject(prefix, name string) string {
	return strings.TrimSuffix(prefix, "/") + ".meta/" + filepath.Base(name)
}
