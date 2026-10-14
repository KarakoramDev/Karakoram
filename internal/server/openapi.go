// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"net/http"

	specification "github.com/KarakoramDev/Karakoram/api"
	"github.com/KarakoramDev/Karakoram/internal/api"
)

// GetOpenAPISpec serves exactly the document embedded in this executable.
func (s *Server) GetOpenAPISpec(context.Context, api.GetOpenAPISpecRequestObject) (api.GetOpenAPISpecResponseObject, error) {
	return response{serve: func(w http.ResponseWriter) error {
		w.Header().Set("Content-Type", "application/yaml")
		w.Header().Set("Cache-Control", "public, max-age=300")
		_, err := w.Write(specification.YAML)
		return err
	}}, nil
}
func (r response) VisitGetOpenAPISpecResponse(w http.ResponseWriter) error { return r.write(w) }
