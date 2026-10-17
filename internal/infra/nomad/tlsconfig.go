// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"crypto/tls"
	"crypto/x509"
	"os"
)

type tlsFiles struct{ CACert, ClientCert, ClientKey, ServerName string }

func loadTLS(getenv func(string) string) (*tls.Config, error) {
	files := tlsFiles{
		CACert:     getenv("KARAKORAM_NOMAD_CACERT"),
		ClientCert: getenv("KARAKORAM_NOMAD_CLIENT_CERT"),
		ClientKey:  getenv("KARAKORAM_NOMAD_CLIENT_KEY"),
		ServerName: getenv("KARAKORAM_NOMAD_TLS_SERVER_NAME"),
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: files.ServerName}
	if files.CACert != "" {
		pem, err := os.ReadFile(files.CACert)
		if err != nil {
			return nil, rejectedConfig("nomadTLSConfig", "cannot load KARAKORAM_NOMAD_CACERT")
		}
		config.RootCAs = x509.NewCertPool()
		if !config.RootCAs.AppendCertsFromPEM(pem) {
			return nil, rejectedConfig("nomadTLSConfig", "cannot load KARAKORAM_NOMAD_CACERT")
		}
	}
	if (files.ClientCert == "") != (files.ClientKey == "") {
		return nil, rejectedConfig("nomadTLSConfig", "KARAKORAM_NOMAD_CLIENT_CERT and KARAKORAM_NOMAD_CLIENT_KEY must be set together")
	}
	if files.ClientCert != "" {
		cert, err := tls.LoadX509KeyPair(files.ClientCert, files.ClientKey)
		if err != nil {
			return nil, rejectedConfig("nomadTLSConfig", "cannot load KARAKORAM_NOMAD_CLIENT_CERT and KARAKORAM_NOMAD_CLIENT_KEY")
		}
		config.Certificates = []tls.Certificate{cert}
	}
	return config, nil
}
