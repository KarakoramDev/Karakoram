// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package enginetest

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

// New Backend methods default to mutations: forgetting to intercept a new
// mutation must fail this test rather than silently weaken every idle guardrail.
func TestRecordingBackendInterceptsEveryMutation(t *testing.T) {
	reads := map[string]bool{"Name": true, "Capabilities": true, "Layout": true, "ProjectInfo": true, "GetService": true, "ListServices": true, "ListPublicPorts": true, "Containers": true, "Logs": true, "OpStatus": true, "SubdomainURLs": true}
	for _, contract := range []reflect.Type{reflect.TypeFor[infra.Backend](), reflect.TypeFor[infra.SubdomainAccess]()} {
		for i := range contract.NumMethod() {
			method := contract.Method(i)
			if reads[method.Name] {
				continue
			}
			t.Run(method.Name, func(t *testing.T) {
				backend := fake.New(fake.Options{})
				backend.FailNext(method.Name, errors.New("injected mutation rejection"))
				recorder := &RecordingBackend{Backend: backend}
				args := []reflect.Value{reflect.ValueOf(context.Background())}
				for j := 1; j < method.Type.NumIn(); j++ {
					args = append(args, reflect.Zero(method.Type.In(j)))
				}
				reflect.ValueOf(recorder).MethodByName(method.Name).Call(args)
				if calls := recorder.Calls(); len(calls) != 1 || calls[0] != method.Name+":" {
					t.Fatalf("mutation escaped recorder: %v", calls)
				}
			})
		}
	}
}
