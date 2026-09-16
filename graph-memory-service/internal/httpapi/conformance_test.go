package httpapi_test

import (
	"bufio"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/httpapi"
)

func TestMemoryProtocolV1RoutesMatchOpenAPI(t *testing.T) {
	want := contractOpenAPIRoutes(t)
	got := httpapi.Routes()
	if len(got) == 0 {
		t.Fatal("Routes() returned no routes")
	}

	wantSet := contractRouteSet(t, want, "OpenAPI")
	gotSet := contractRouteSet(t, got, "Routes()")
	if fmt.Sprint(gotSet) != fmt.Sprint(wantSet) {
		t.Errorf("Routes() =\n  %s\nOpenAPI routes =\n  %s", strings.Join(gotSet, "\n  "), strings.Join(wantSet, "\n  "))
	}
}

// TestConsolidationCutRoutesMatchOpenAPI pins the independent cut route
// inventory to the SEPARATE consolidated-cut OpenAPI contract. The cut
// surface is intentionally not part of Routes() (which is pinned to the
// Memory Protocol spec above); it is conformance-checked here on its own
// against openapi/consolidation-cuts.yaml.
func TestConsolidationCutRoutesMatchOpenAPI(t *testing.T) {
	want := contractOpenAPIRoutesFrom(t, "consolidation-cuts.yaml")
	got := httpapi.ConsolidationCutRoutes()
	if len(got) == 0 {
		t.Fatal("ConsolidationCutRoutes() returned no routes")
	}

	wantSet := contractRouteSet(t, want, "consolidation-cuts.yaml")
	gotSet := contractRouteSet(t, got, "ConsolidationCutRoutes()")
	if fmt.Sprint(gotSet) != fmt.Sprint(wantSet) {
		t.Errorf("ConsolidationCutRoutes() =\n  %s\nOpenAPI routes =\n  %s", strings.Join(gotSet, "\n  "), strings.Join(wantSet, "\n  "))
	}
}

func contractOpenAPIRoutes(t *testing.T) []httpapi.Route {
	return contractOpenAPIRoutesFrom(t, "memory-protocol.yaml")
}

func contractOpenAPIRoutesFrom(t *testing.T, specFile string) []httpapi.Route {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate conformance test source")
	}
	protocolPath := filepath.Join(filepath.Dir(filename), "..", "..", "openapi", specFile)
	file, err := os.Open(protocolPath)
	if err != nil {
		t.Fatalf("open frozen OpenAPI protocol: %v", err)
	}
	defer file.Close()

	methods := map[string]bool{
		"delete":  true,
		"get":     true,
		"head":    true,
		"options": true,
		"patch":   true,
		"post":    true,
		"put":     true,
		"trace":   true,
	}
	var routes []httpapi.Route
	inPaths := false
	currentPath := ""
	serverPath := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "servers:" {
			continue
		}
		// The server base may be a bare path (consolidation-cuts.yaml uses
		// url: /v1) or a full origin with no path (memory-protocol.yaml uses
		// url: http://localhost). Prepend the base path so the server-relative
		// OpenAPI paths equal the codebase's fully-qualified /v1 routes.
		if strings.HasPrefix(strings.TrimSpace(line), "- url: ") {
			raw := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "- url: "))
			raw = strings.Trim(raw, `"`)
			if !strings.Contains(raw, "//") {
				serverPath = raw
			} else if u, err := url.Parse(raw); err == nil {
				serverPath = u.Path
			}
			continue
		}
		if line == "paths:" {
			inPaths = true
			continue
		}
		if !inPaths {
			continue
		}
		if line == "components:" {
			break
		}
		if strings.HasPrefix(line, "  /") && !strings.HasPrefix(line, "    ") {
			currentPath = serverPath + strings.TrimSuffix(strings.TrimSpace(line), ":")
			continue
		}
		if currentPath == "" || !strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "      ") {
			continue
		}
		word := strings.TrimSuffix(strings.TrimSpace(line), ":")
		if methods[word] {
			routes = append(routes, httpapi.Route{Method: strings.ToUpper(word), Path: currentPath})
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan frozen OpenAPI protocol: %v", err)
	}
	if len(routes) == 0 {
		t.Fatal("standard-library OpenAPI scanner found no routes")
	}
	return routes
}

func contractRouteSet(t *testing.T, routes []httpapi.Route, source string) []string {
	t.Helper()
	allowedMethods := map[string]bool{
		http.MethodDelete:  true,
		http.MethodGet:     true,
		http.MethodHead:    true,
		http.MethodOptions: true,
		http.MethodPatch:   true,
		http.MethodPost:    true,
		http.MethodPut:     true,
		http.MethodTrace:   true,
	}
	seen := make(map[string]bool, len(routes))
	result := make([]string, 0, len(routes))
	for _, route := range routes {
		if !allowedMethods[route.Method] {
			t.Errorf("%s has non-canonical HTTP method %q for path %q", source, route.Method, route.Path)
		}
		if !strings.HasPrefix(route.Path, "/v1/") && route.Path != "/v1/tenant" {
			t.Errorf("%s route %s %s is outside /v1", source, route.Method, route.Path)
		}
		key := route.Method + " " + route.Path
		if seen[key] {
			t.Errorf("%s contains duplicate route %s", source, key)
			continue
		}
		seen[key] = true
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
