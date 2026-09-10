// Package roombridge embeds the Host-owned Pi extension that registers the
// Room tool surface for real Pi RPC processes. Evaluation runners materialize
// it next to their work directories and pass the path through the launcher.
package roombridge

import (
	_ "embed"
	"os"
	"path/filepath"
)

//go:embed room_bridge.js
var extensionSource string

// WriteTo writes the extension as <dir>/room_bridge.js and returns its path.
// The file is world-readable so the Pi child process can load it.
func WriteTo(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "room_bridge.js")
	if err := os.WriteFile(path, []byte(extensionSource), 0o644); err != nil {
		return "", err
	}
	return path, nil
}
