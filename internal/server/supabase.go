// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
)

type supabaseAccess interface {
	Credentials(context.Context, *engine.Context) (supabase.Credentials, error)
	SetSMTPPassword(context.Context, *engine.Context, string) error
	ListFunctions(context.Context, *engine.Context) ([]string, error)
	GetFunction(context.Context, *engine.Context, string) (map[string]string, error)
	PutFunction(context.Context, *engine.Context, string, map[string]string) error
	DeleteFunction(context.Context, *engine.Context, string) error
}

var supabaseFunctionName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func validateSupabaseFunction(files map[string]string) error {
	if len(files) == 0 {
		return engine.InvalidField("body", domain.ReasonInvalid, "function must contain at least one source file")
	}
	total := 0
	for name, source := range files {
		if name == "" || strings.HasPrefix(name, "/") || strings.ContainsAny(name, "\\\x00:") {
			return engine.InvalidField("body", domain.ReasonInvalid, "function file paths must be relative")
		}
		for _, part := range strings.Split(name, "/") {
			if part == "" || part == "." || part == ".." {
				return engine.InvalidField("body", domain.ReasonInvalid, "function file paths must not contain empty, dot or parent components")
			}
		}
		total += len(source)
		if total > 1<<20 {
			return engine.InvalidField("body", domain.ReasonInvalid, "function source must not exceed 1 MiB")
		}
	}
	return nil
}

func (s *Server) supabase(ctx context.Context, name string, in input, rc *engine.Context, p engine.Provider) response {
	access, yes := p.(supabaseAccess)
	if p.Name() != "supabase" || !yes {
		return s.failure(engine.ErrNotSupported)
	}
	if in.Name != "" && !supabaseFunctionName.MatchString(in.Name) {
		return invalidField("name", domain.ReasonInvalid, "invalid function name")
	}
	switch name {
	case "GetSupabaseCredentials":
		credentials, err := access.Credentials(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		return response{status: http.StatusOK, body: credentials, noStore: true}
	case "SetSupabaseSMTPPassword":
		body := in.Body.(*api.SupabaseSMTPPassword)
		if err := access.SetSMTPPassword(ctx, rc, value(body.Password)); err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: http.StatusNoContent, noStore: true}
	case "ListSupabaseFunctions":
		names, err := access.ListFunctions(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		slices.Sort(names)
		return ok(page[string]{Items: nonNil(names)})
	case "GetSupabaseFunction":
		files, err := access.GetFunction(ctx, rc, in.Name)
		if err != nil {
			return s.failure(err)
		}
		return response{status: http.StatusOK, body: files, noStore: true}
	case "PutSupabaseFunction":
		files := map[string]string(*in.Body.(*api.SupabaseFunction))
		if err := validateSupabaseFunction(files); err != nil {
			return s.failure(err)
		}
		names, err := access.ListFunctions(ctx, rc)
		if err != nil {
			return s.failure(err)
		}
		if len(names) >= 100 && !slices.Contains(names, in.Name) {
			return invalidField("name", domain.ReasonInvalid, "cluster must not contain more than 100 functions")
		}
		if err := access.PutFunction(ctx, rc, in.Name, files); err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: http.StatusNoContent, noStore: true}
	case "DeleteSupabaseFunction":
		if err := access.DeleteFunction(ctx, rc, in.Name); err != nil {
			return s.failure(err)
		}
		s.trigger(ctx, rc.Cluster.Ref())
		return response{status: http.StatusNoContent}
	case "RotateSupabaseJWTSecret":
		return s.enqueue(ctx, rc.Cluster.Ref(), "rotate-jwt-secret", map[string]string{})
	}
	return fail(500, "internal_error", "Unknown Supabase operation")
}

func (s *Server) GetSupabaseCredentials(ctx context.Context, request api.GetSupabaseCredentialsRequestObject) (api.GetSupabaseCredentialsResponseObject, error) {
	return s.handle(ctx, "GetSupabaseCredentials", input{Cluster: request.Cluster}), nil
}
func (r response) VisitGetSupabaseCredentialsResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) SetSupabaseSMTPPassword(ctx context.Context, request api.SetSupabaseSMTPPasswordRequestObject) (api.SetSupabaseSMTPPasswordResponseObject, error) {
	return s.handle(ctx, "SetSupabaseSMTPPassword", input{Cluster: request.Cluster, Body: request.Body}), nil
}
func (r response) VisitSetSupabaseSMTPPasswordResponse(w http.ResponseWriter) error {
	return r.write(w)
}
func (s *Server) ListSupabaseFunctions(ctx context.Context, request api.ListSupabaseFunctionsRequestObject) (api.ListSupabaseFunctionsResponseObject, error) {
	return s.handle(ctx, "ListSupabaseFunctions", input{Cluster: request.Cluster}), nil
}
func (r response) VisitListSupabaseFunctionsResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) GetSupabaseFunction(ctx context.Context, request api.GetSupabaseFunctionRequestObject) (api.GetSupabaseFunctionResponseObject, error) {
	return s.handle(ctx, "GetSupabaseFunction", input{Cluster: request.Cluster, Name: request.Name}), nil
}
func (r response) VisitGetSupabaseFunctionResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) PutSupabaseFunction(ctx context.Context, request api.PutSupabaseFunctionRequestObject) (api.PutSupabaseFunctionResponseObject, error) {
	return s.handle(ctx, "PutSupabaseFunction", input{Cluster: request.Cluster, Name: request.Name, Body: request.Body}), nil
}
func (r response) VisitPutSupabaseFunctionResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) DeleteSupabaseFunction(ctx context.Context, request api.DeleteSupabaseFunctionRequestObject) (api.DeleteSupabaseFunctionResponseObject, error) {
	return s.handle(ctx, "DeleteSupabaseFunction", input{Cluster: request.Cluster, Name: request.Name}), nil
}
func (r response) VisitDeleteSupabaseFunctionResponse(w http.ResponseWriter) error { return r.write(w) }
func (s *Server) RotateSupabaseJWTSecret(ctx context.Context, request api.RotateSupabaseJWTSecretRequestObject) (api.RotateSupabaseJWTSecretResponseObject, error) {
	return s.handle(ctx, "RotateSupabaseJWTSecret", input{Cluster: request.Cluster}), nil
}
func (r response) VisitRotateSupabaseJWTSecretResponse(w http.ResponseWriter) error {
	return r.write(w)
}
