package authz_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"testing"

	"river2.dev/graph-memory-service/internal/authz"
)

func TestAuthenticatorRequiresExactBearerToken(t *testing.T) {
	authenticator := authz.NewAuthenticator("correct-token")
	for _, authorization := range []string{
		"",
		"correct-token",
		"Basic correct-token",
		"Bearer wrong-token",
		"Bearer correct-token extra",
		"bearer correct-token",
	} {
		if _, err := authenticator.Authenticate(authorization); err == nil {
			t.Errorf("Authenticate(%q) succeeded, want rejection", authorization)
		}
	}
	if _, err := authenticator.Authenticate("Bearer correct-token"); err != nil {
		t.Errorf("Authenticate(valid bearer token) error = %v", err)
	}
}

func TestAuthenticatorUsesConstantTimeTokenComparison(t *testing.T) {
	_, testFilename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test source")
	}
	serviceFilename := filepath.Join(filepath.Dir(testFilename), "service.go")
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, serviceFilename, nil, 0)
	if err != nil {
		t.Fatalf("parse authz source: %v", err)
	}

	importsCryptoSubtle := false
	for _, spec := range parsed.Imports {
		if spec.Path.Value == `"crypto/subtle"` {
			importsCryptoSubtle = true
			break
		}
	}
	usesConstantTimeCompare := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "ConstantTimeCompare" {
			return true
		}
		packageName, ok := selector.X.(*ast.Ident)
		if ok && packageName.Name == "subtle" {
			usesConstantTimeCompare = true
		}
		return true
	})
	if !importsCryptoSubtle || !usesConstantTimeCompare {
		t.Errorf("Authenticator must compare bearer-token bytes with crypto/subtle.ConstantTimeCompare (import=%t call=%t)", importsCryptoSubtle, usesConstantTimeCompare)
	}
}
