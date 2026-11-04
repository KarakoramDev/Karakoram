// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal"
)

func (s *Server) temporalFailure(err error) response {
	r := s.failure(err)
	if errors.Is(err, engine.ErrInvalid) {
		r.status = http.StatusUnprocessableEntity
	}
	return r
}

func (s *Server) temporal(ctx context.Context, name string, in input, rc *engine.Context, p engine.Provider) response {
	access, yes := p.(temporal.Access)
	if p.Name() != "temporal" || !yes {
		return fail(404, "not_found", "Temporal cluster not found")
	}
	switch name {
	case "IssueTemporalClientCertificate":
		b := in.Body.(*api.TemporalClientCertificateRequest)
		ttl := 720
		if b.TtlHours != nil {
			ttl = *b.TtlHours
		}
		if strings.TrimSpace(b.CommonName) == "" || ttl < 1 || ttl > 9528 {
			return fail(422, "invalid_request", "commonName is required and ttlHours must be between 1 and 9528")
		}
		issued, err := access.IssueClientCertificate(ctx, rc, b.CommonName, time.Duration(ttl)*time.Hour)
		if err != nil {
			return s.temporalFailure(err)
		}
		return response{status: http.StatusCreated, body: api.TemporalIssuedClientCertificate{
			Serial: issued.Serial, CommonName: issued.CommonName,
			FingerprintSha256: issued.FingerprintSHA256, CaFingerprintSha256: issued.CAFingerprintSHA256,
			CurrentCa: issued.CurrentCA, IssuedAt: issued.IssuedAt, NotAfter: issued.NotAfter,
			CertificatePem: issued.CertificatePEM, PrivateKeyPem: issued.PrivateKeyPEM, CaPem: issued.CAPEM,
			FrontendAddress: issued.FrontendAddress, ServerName: issued.ServerName,
		}, noStore: true}
	case "ListTemporalClientCertificates":
		items, err := access.ListClientCertificates(ctx, rc)
		if err != nil {
			return s.temporalFailure(err)
		}
		mapped := make([]api.TemporalClientCertificate, len(items))
		for i, c := range items {
			mapped[i] = api.TemporalClientCertificate{
				Serial: c.Serial, CommonName: c.CommonName,
				FingerprintSha256: c.FingerprintSHA256, CaFingerprintSha256: c.CAFingerprintSHA256,
				CurrentCa: c.CurrentCA, IssuedAt: c.IssuedAt, NotAfter: c.NotAfter,
			}
		}
		return ok(api.TemporalClientCertificateList{Items: mapped})
	case "ListTemporalNamespaces":
		items, err := access.ListNamespaces(ctx, rc)
		if err != nil {
			return s.temporalFailure(err)
		}
		return ok(page[temporal.Namespace]{Items: nonNil(items)})
	case "DeleteTemporalNamespace":
		if err := access.DeleteNamespace(ctx, rc, in.Name); err != nil {
			return s.temporalFailure(err)
		}
		return response{status: http.StatusAccepted}
	case "SetTemporalUIOIDCClientSecret":
		b := in.Body.(*api.TemporalUIOIDCClientSecret)
		if strings.TrimSpace(value(b.ClientSecret)) == "" {
			return fail(422, "invalid_request", "clientSecret is required")
		}
		if err := access.SetUIOIDCClientSecret(ctx, rc, value(b.ClientSecret)); err != nil {
			return s.temporalFailure(err)
		}
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: http.StatusNoContent, noStore: true}
	}
	return fail(500, "internal_error", "Unknown Temporal operation")
}

func (s *Server) IssueTemporalClientCertificate(ctx context.Context, r api.IssueTemporalClientCertificateRequestObject) (api.IssueTemporalClientCertificateResponseObject, error) {
	return s.handle(ctx, "IssueTemporalClientCertificate", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitIssueTemporalClientCertificateResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListTemporalClientCertificates(ctx context.Context, r api.ListTemporalClientCertificatesRequestObject) (api.ListTemporalClientCertificatesResponseObject, error) {
	return s.handle(ctx, "ListTemporalClientCertificates", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListTemporalClientCertificatesResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListTemporalNamespaces(ctx context.Context, r api.ListTemporalNamespacesRequestObject) (api.ListTemporalNamespacesResponseObject, error) {
	return s.handle(ctx, "ListTemporalNamespaces", input{Cluster: r.Cluster}), nil
}
func (r response) VisitListTemporalNamespacesResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteTemporalNamespace(ctx context.Context, r api.DeleteTemporalNamespaceRequestObject) (api.DeleteTemporalNamespaceResponseObject, error) {
	return s.handle(ctx, "DeleteTemporalNamespace", input{Cluster: r.Cluster, Name: r.TemporalNamespace}), nil
}
func (r response) VisitDeleteTemporalNamespaceResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) SetTemporalUIOIDCClientSecret(ctx context.Context, r api.SetTemporalUIOIDCClientSecretRequestObject) (api.SetTemporalUIOIDCClientSecretResponseObject, error) {
	return s.handle(ctx, "SetTemporalUIOIDCClientSecret", input{Cluster: r.Cluster, Body: r.Body}), nil
}
func (r response) VisitSetTemporalUIOIDCClientSecretResponse(w http.ResponseWriter) error {
	return r.write(w)
}
