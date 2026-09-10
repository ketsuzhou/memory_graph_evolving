package httpapi_test

import (
	"net/http"
	"strings"
	"testing"

	"river2.dev/graph-memory-service/internal/httpapi"
)

func TestOperationalPayloadCannotSelectTenantPrincipalOrGrant(t *testing.T) {
	server := newContractServer(t)
	contractInitializeTenant(t, server)

	selectors := []struct {
		name  string
		field string
		value any
	}{
		{name: "tenant", field: "tenant_id", value: "attacker-tenant"},
		{name: "principal", field: "principal_id", value: "attacker-principal"},
		{name: "acting principal", field: "acting_principal_id", value: "attacker-principal"},
		{name: "grant", field: "grant_id", value: "attacker-grant"},
	}
	for _, selector := range selectors {
		t.Run(selector.name, func(t *testing.T) {
			request := map[string]any{
				"request_id":  "authority-check",
				"query":       "must not select authority",
				"space_ids":   []string{"room-shared"},
				"max_results": 1,
				"deadline_ms": 100,
			}
			request[selector.field] = selector.value
			status, header, payload := contractJSONRequest(t, server, http.MethodPost, "/v1/recalls", request)
			contractRequireError(t, status, header, payload, http.StatusBadRequest, "INVALID_REQUEST")
		})
	}
}

func TestProtocolHasNoHostDatabaseOrDAGMutationRoute(t *testing.T) {
	routes := httpapi.Routes()
	if len(routes) == 0 {
		t.Fatal("Routes() returned no protocol routes")
	}
	for _, route := range routes {
		path := strings.ToLower(route.Path)
		for _, forbidden := range []string{"/host", "/rooms", "/deliveries", "/interactions", "/dag", "database", "sql"} {
			if strings.Contains(path, forbidden) {
				t.Errorf("protocol route %s %s exposes forbidden Host database or DAG authority (%q)", route.Method, route.Path, forbidden)
			}
		}
		if route.Method == http.MethodPatch || route.Method == http.MethodDelete {
			t.Errorf("protocol route %s %s exposes an unsupported mutation method", route.Method, route.Path)
		}
	}
}
