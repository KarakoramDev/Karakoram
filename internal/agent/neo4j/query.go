// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

var errCredentials = errors.New("Neo4j credentials rejected")

func tlsClient(s neo4jspec.Spec) (*http.Client, error) {
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(s.CAPEM)) {
		return nil, errors.New("invalid Neo4j CA")
	}
	pair, err := tls.X509KeyPair([]byte(s.Server.CertPEM), []byte(s.Server.KeyPEM))
	if err != nil {
		return nil, errors.New("invalid Neo4j certificate")
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return nil, errors.New("invalid Neo4j certificate")
	}
	name := "localhost"
	if leaf.VerifyHostname(name) != nil {
		if len(leaf.DNSNames) > 0 {
			name = leaf.DNSNames[0]
		} else if len(leaf.IPAddresses) > 0 {
			name = leaf.IPAddresses[0].String()
		} else {
			return nil, errors.New("Neo4j certificate has no SAN")
		}
	}
	tr := &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: name}, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second}
	return &http.Client{Transport: tr, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

type queryResult struct {
	Data struct {
		Fields []string `json:"fields"`
		Values [][]any  `json:"values"`
	} `json:"data"`
	Errors []struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

func request(ctx context.Context, s neo4jspec.Spec, port int, password, method, path string, body any, out any) error {
	client, err := tlsClient(s)
	if err != nil {
		return err
	}
	defer client.CloseIdleConnections()
	var encoded []byte
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return errors.New("invalid Neo4j request")
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, "https://127.0.0.1:"+strconv.Itoa(port)+path, bytes.NewReader(encoded))
	if err != nil {
		return errors.New("invalid Neo4j request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if password != "" {
		req.SetBasicAuth(neo4jspec.AdminUser, password)
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("Neo4j request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		return errCredentials
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if _, isQuery := out.(*queryResult); isQuery {
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
			var failure queryResult
			if readErr == nil && len(data) <= 64<<10 && json.Unmarshal(data, &failure) == nil && len(failure.Errors) > 0 {
				return classifyQueryError(failure.Errors[0].Code, failure.Errors[0].Message)
			}
		}
		return errors.New("Neo4j request rejected")
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if err != nil || len(data) > 4<<20 || json.Unmarshal(data, out) != nil {
		return errors.New("invalid Neo4j response")
	}
	return nil
}
func query(ctx context.Context, s neo4jspec.Spec, port int, password, db, statement string, parameters map[string]any) (queryResult, error) {
	var result queryResult
	err := request(ctx, s, port, password, http.MethodPost, "/db/"+db+"/query/v2", struct {
		Statement  string         `json:"statement"`
		Parameters map[string]any `json:"parameters,omitempty"`
	}{statement, parameters}, &result)
	if err != nil {
		return result, err
	}
	if len(result.Errors) > 0 {
		return queryResult{}, classifyQueryError(result.Errors[0].Code, result.Errors[0].Message)
	}
	return result, nil
}
func (r queryResult) rows() []map[string]any {
	rows := make([]map[string]any, 0, len(r.Data.Values))
	for _, values := range r.Data.Values {
		row := make(map[string]any, len(r.Data.Fields))
		for i, k := range r.Data.Fields {
			if i < len(values) {
				row[k] = values[i]
			}
		}
		rows = append(rows, row)
	}
	return rows
}
