// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/apispec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/gorillamux"
)

var requestRouter = sync.OnceValues(func() (routers.Router, error) {
	doc, err := apispec.Load()
	if err != nil {
		return nil, err
	}
	// The stdlib dispatcher serves /healthz and /metrics at the installation root.
	// Strip the API prefix on routing only; keep the embedded document immutable.
	copy := *doc
	copy.Servers = nil
	return gorillamux.NewRouter(&copy)
})

var saneRequestID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func requestID(r *http.Request) string {
	values := r.Header.Values("X-Request-ID")
	if len(values) == 1 && saneRequestID.MatchString(values[0]) {
		return values[0]
	}
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		panic("generate request identifier: " + err.Error())
	}
	return hex.EncodeToString(data[:])
}

func (s *Server) validationMiddleware(next http.Handler) http.Handler {
	router, err := requestRouter()
	if err != nil {
		panic("load request validation schema: " + err.Error())
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isAPI := strings.HasPrefix(r.URL.Path, "/api/v1/")
		if !isAPI && r.URL.Path != "/healthz" && r.URL.Path != "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		routed := r.Clone(r.Context())
		if isAPI {
			routed.URL.Path = strings.TrimPrefix(r.URL.Path, "/api/v1")
			routed.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, "/api/v1")
		}
		if routed.Method == http.MethodHead {
			routed.Method = http.MethodGet
		}
		route, params, routeErr := router.FindRoute(routed)
		if routeErr != nil {
			methods := []string{}
			for _, method := range []string{"DELETE", "GET", "OPTIONS", "PATCH", "POST", "PUT"} {
				probe := routed.Clone(routed.Context())
				probe.Method = method
				if found, _, e := router.FindRoute(probe); e == nil {
					methods = append(methods, method)
					if method == "GET" {
						methods = append(methods, "HEAD")
					}
					_ = found
				}
			}
			if len(methods) > 0 {
				sort.Strings(methods)
				w.Header().Set("Allow", strings.Join(methods, ", "))
				_ = fail(405, "method_not_allowed", "Method "+r.Method+" is not allowed for this route; use "+strings.Join(methods, ", ")).write(w)
				return
			}
			// Permit separately registered API endpoints such as the OpenAPI document;
			// the dispatcher's API fallback remains a JSON 404 for unknown paths.
			next.ServeHTTP(w, r)
			return
		}
		// Gorilla routes encoded paths; validate the same decoded segment values
		// that net/http supplies to the strict handlers through PathValue.
		for name, encoded := range params {
			decoded, err := url.PathUnescape(encoded)
			if err != nil {
				_ = invalidField(name, domain.ReasonInvalid, name+" must use valid URL path escaping").write(w)
				return
			}
			params[name] = decoded
		}
		if route.Operation.RequestBody != nil {
			kind, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if e == nil && (route.Operation.OperationID == "patchCluster" || route.Operation.OperationID == "dryRunPatchCluster") && kind == "application/json" {
				// Older clients encoded merge patches as JSON. Normalize only this
				// operation; the wire format and merge semantics are identical.
				kind = "application/merge-patch+json"
				r.Header.Set("Content-Type", kind)
				routed.Header.Set("Content-Type", kind)
			}
			allowed := route.Operation.RequestBody.Value.Content
			// Absence of a body is a required-field error, not a media-type error.
			hasBody := r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0
			if hasBody {
				if e != nil || allowed[kind] == nil {
					types := make([]string, 0, len(allowed))
					for k := range allowed {
						types = append(types, k)
					}
					sort.Strings(types)
					_ = failDetails(415, "unsupported_media_type", "Content-Type must be "+strings.Join(types, " or "), map[string]any{"field": "Content-Type", "reason": domain.ReasonUnsupported}).write(w)
					return
				}
			}
		}
		if r.Body != nil && r.Body != http.NoBody {
			data, e := io.ReadAll(r.Body)
			if e != nil {
				_ = decoderFailure(e).write(w)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(data))
			routed.Body = io.NopCloser(bytes.NewReader(data))
			if len(bytes.TrimSpace(data)) > 0 {
				if route.Operation.RequestBody == nil {
					_ = invalidField("body", domain.ReasonInvalid, "body is not accepted by this operation").write(w)
					return
				}
				if !json.Valid(data) {
					_ = invalidField("body", domain.ReasonInvalid, "body must contain exactly one valid JSON value with no trailing data").write(w)
					return
				}
			}
		}
		if e := validateQuery(r, route); e != nil {
			_ = s.failure(e).write(w)
			return
		}
		input := &openapi3filter.RequestValidationInput{Request: routed, PathParams: params, Route: route, Options: &openapi3filter.Options{
			AuthenticationFunc:  openapi3filter.NoopAuthenticationFunc,
			SkipSettingDefaults: true, ExcludeReadOnlyValidations: true,
			SchemaValidationOptions: []openapi3.SchemaValidationOption{openapi3.EnableFormatValidation()},
		}}
		if e := openapi3filter.ValidateRequest(r.Context(), input); e != nil {
			result := schemaFailure(e)
			if strings.Contains(route.Operation.OperationID, "Temporal") {
				result.status = http.StatusUnprocessableEntity
			}
			_ = result.write(w)
			return
		}
		// Generated strict handlers decode JSON even when OpenAPI makes the
		// entire body optional. Preserve absence through schema validation, then
		// give that decoder the empty object represented by its zero-value input.
		if body := route.Operation.RequestBody; body != nil && !body.Value.Required && r.ContentLength == 0 && body.Value.Content["application/json"] != nil {
			r.Body = io.NopCloser(strings.NewReader("{}"))
			r.ContentLength = 2
			r.Header.Set("Content-Type", "application/json")
		}
		next.ServeHTTP(w, r)
	})
}

func schemaFailure(err error) response {
	field, reason, rule := "body", domain.ReasonInvalid, "does not match the request schema"
	var req *openapi3filter.RequestError
	if errors.As(err, &req) && req.Parameter != nil {
		field = req.Parameter.Name
	}
	if errors.Is(err, openapi3filter.ErrInvalidRequired) {
		reason = domain.ReasonRequired
		rule = "is required"
	}
	var schema *openapi3.SchemaError
	if errors.As(err, &schema) {
		if schema.SchemaField == "not" && schema.Schema != nil && schema.Schema.Not != nil &&
			strings.Join(schema.Schema.Not.Value.Required, ",") == "password,passwordHash" {
			return invalidField("password", domain.ReasonConflict, "password and passwordHash are mutually exclusive")
		}
		if schema.SchemaField == "oneOf" && strings.Join(schema.JSONPointer(), ".") == "source" {
			return invalidField("source", domain.ReasonConflict, "source must set exactly one of source.backup or source.pitrTime")
		}
		// Unwrap composition errors to preserve the innermost JSON field path.
		for schema.SchemaField == "oneOf" || schema.SchemaField == "allOf" || schema.SchemaField == "anyOf" {
			var nested *openapi3.SchemaError
			if schema.Origin == nil || !errors.As(schema.Origin, &nested) || nested == schema {
				break
			}
			schema = nested
		}
		path := schema.JSONPointer()
		if len(path) > 0 && field == "body" {
			field = strings.Join(path, ".")
		}
		rule = schema.Reason
		switch schema.SchemaField {
		case "required":
			reason = domain.ReasonRequired
		case "properties":
			reason = domain.ReasonUnknownField
			// kin-openapi reports the parent path for additionalProperties violations.
			if strings.HasPrefix(rule, "property ") {
				quoted := strings.TrimSuffix(strings.TrimPrefix(rule, "property "), " is unsupported")
				if property, e := strconv.Unquote(quoted); e == nil {
					if field == "body" {
						field = property
					} else {
						field += "." + property
					}
				}
			}
			rule = "is not a recognized field"
		case "enum":
			reason = domain.ReasonUnsupported
			values := make([]string, 0, len(schema.Schema.Enum))
			for _, value := range schema.Schema.Enum {
				values = append(values, fmt.Sprint(value))
			}
			rule = "must be one of: " + strings.Join(values, ", ")
		case "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "minLength", "maxLength", "minItems", "maxItems":
			reason = domain.ReasonOutOfRange
			if schema.SchemaField == "minLength" && schema.Value == "" {
				reason = domain.ReasonRequired
				rule = "is required"
			}
		case "pattern":
			if schema.Schema.Pattern == `\S` {
				reason = domain.ReasonRequired
				rule = "must not be blank"
			}
		}
		if schema.Schema != nil && schema.Schema.Description != "" {
			rule += "; " + schema.Schema.Description
		}
	}
	if field == "spec.components.configServer.replicas" && schema != nil && schema.SchemaField != "type" {
		rule = "must be 1, 3, 5 or 7"
		reason = domain.ReasonOutOfRange
	}
	if field == "limit" {
		rule = "must be an integer from 1 to 500"
	}
	if field == "order" {
		rule = "must be asc or desc"
	}
	if field == "expiresAt" {
		rule = "must be an RFC 3339 date-time"
	}
	if field == "spec.provider" && reason == domain.ReasonRequired {
		rule = "is required; list providers with GET /api/v1/providers"
	}
	return invalidField(field, reason, field+" "+rule)
}

func decoderFailure(err error) response {
	var large *http.MaxBytesError
	if errors.As(err, &large) {
		return fail(413, "payload_too_large", fmt.Sprintf("body exceeds the %d MiB size limit", large.Limit>>20))
	}
	var typed *json.UnmarshalTypeError
	if errors.As(err, &typed) {
		field := typed.Field
		if field == "" {
			field = "body"
		}
		return invalidField(field, domain.ReasonInvalid, field+" must be "+typed.Type.String())
	}
	var date *time.ParseError
	if errors.As(err, &date) {
		return invalidField("expiresAt", domain.ReasonInvalid, "expiresAt must be an RFC 3339 date-time")
	}
	return invalidField("body", domain.ReasonInvalid, "body must contain valid JSON matching the request schema")
}

func parameterFailure(err error) response {
	field, reason := "parameters", domain.ReasonInvalid
	switch e := err.(type) {
	case *api.InvalidParamFormatError:
		field = e.ParamName
	case *api.TooManyValuesForParamError:
		field = e.ParamName
	case *api.RequiredParamError:
		field = e.ParamName
		reason = domain.ReasonRequired
	}
	rule := "must have one valid value matching its documented type"
	if reason == domain.ReasonRequired {
		rule = "is required"
	}
	if field == "limit" {
		rule = "must be an integer from 1 to 500"
	}
	return invalidField(field, reason, field+" "+rule)
}

// Cursor formats differ: events also accept their numeric SSE event IDs.
func validateQuery(r *http.Request, route *routers.Route) error {
	q := r.URL.Query()
	for _, parameters := range []openapi3.Parameters{route.PathItem.Parameters, route.Operation.Parameters} {
		for _, param := range parameters {
			p := param.Value
			if p.In != "query" {
				continue
			}
			values, present := q[p.Name]
			if present && (len(values) != 1 || (values[0] == "" && p.Name != "actor" && p.Name != "resource")) {
				rule := "must have one nonempty value"
				switch p.Name {
				case "limit":
					rule = "must be a single integer from 1 to 500"
				case "order":
					rule = "must be asc or desc"
				case "cursor":
					rule = "must be one cursor returned by the preceding page"
				case "range":
					rule = "must be a positive duration no greater than 24h"
				case "step":
					rule = "must be a positive duration no greater than range"
				}
				return domain.InvalidField(p.Name, domain.ReasonInvalid, "%s %s", p.Name, rule)
			}
		}
	}
	if cursor := q.Get("cursor"); cursor != "" {
		var err error
		switch route.Operation.OperationID {
		case "listEvents", "streamEvents":
			_, err = eventCursor(cursor)
		case "getMemberLogs": // Provider-owned log cursor, not a list offset.
		default:
			b, e := base64.RawURLEncoding.DecodeString(cursor)
			err = e
			if err == nil {
				var n int64
				n, err = strconv.ParseInt(string(b), 10, 64)
				if n < 0 {
					err = fmt.Errorf("negative cursor")
				}
			}
		}
		if err != nil {
			return domain.InvalidField("cursor", domain.ReasonInvalid, "cursor must be a cursor returned by the preceding page (events also accept nonnegative event IDs)")
		}
	}
	if route.Operation.OperationID == "getClusterMetrics" {
		_, _, err := metricsDurations(q.Get("range"), q.Get("step"))
		return err
	}
	return nil
}

func metricsDurations(rawRange, rawStep string) (time.Duration, time.Duration, error) {
	lookback, step := time.Hour, 30*time.Second
	var err error
	if rawRange != "" {
		lookback, err = time.ParseDuration(rawRange)
		if err != nil || lookback <= 0 || lookback > 24*time.Hour {
			return 0, 0, domain.InvalidField("range", domain.ReasonOutOfRange, "range must be a positive duration no greater than 24h (for example 30m, 1h or 24h; d is not a duration unit)")
		}
	}
	if rawStep != "" {
		step, err = time.ParseDuration(rawStep)
		if err != nil || step <= 0 || step > lookback {
			return 0, 0, domain.InvalidField("step", domain.ReasonOutOfRange, "step must be a positive duration no greater than range (%s); use duration units such as s, m or h", lookback)
		}
	} else if step > lookback {
		step = lookback
	}
	return lookback, step, nil
}
