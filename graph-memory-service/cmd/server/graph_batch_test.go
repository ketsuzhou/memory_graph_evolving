package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"river2.dev/graph-memory-service/internal/httpapi"
)

func TestNewGraphBatchHandlerAlwaysWiresIndependentRoutes(t *testing.T) {
	handler, err := newGraphBatchHandler("test-token")
	if err != nil {
		t.Fatalf("new graph batch handler: %v", err)
	}
	if handler == nil {
		t.Fatal("newGraphBatchHandler returned nil handler")
	}
	routes := httpapi.GraphBatchRoutes()
	if len(routes) != 4 {
		t.Fatalf("GraphBatchRoutes() = %#v, want 4 always-mounted paths", routes)
	}
	for _, route := range routes {
		req := httptest.NewRequest(route.Method, route.Path, bytes.NewReader([]byte(`{}`)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s %s without token status = %d, want 401", route.Method, route.Path, rec.Code)
		}
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil || envelope.Error.Code != "UNAUTHORIZED" {
			t.Fatalf("%s unauthorized body = %s", route.Path, rec.Body.String())
		}
	}
}
