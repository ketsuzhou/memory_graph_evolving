// Command integrationtracer runs one PG-40/PG-41 Host scenario and prints
// its factual outcome as a single JSON object on stdout. It is a tracer
// driver, not a composition root: PG-50B remains the sole writer of
// cmd/bench-runner and the runtime composition.
//
// Usage:
//
//	go run ./internal/integrationtracer/main freeze <outputFreezePath>
//	go run ./internal/integrationtracer/main benchmark-barrier <batchManifestPath> <outputBarrierFactsPath>
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"river2.dev/pi-group-chat-host/internal/integrationtracer"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: integrationtracer <freeze <outputFreezePath>|benchmark-barrier <batchManifestPath> <outputBarrierFactsPath>>")
		os.Exit(2)
	}
	var (
		facts map[string]any
		err   error
	)
	switch os.Args[1] {
	case "freeze":
		facts, err = integrationtracer.Freeze(os.Args[2])
	case "benchmark-barrier":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "benchmark-barrier requires <batchManifestPath> <outputBarrierFactsPath>")
			os.Exit(2)
		}
		facts, err = integrationtracer.BenchmarkBarrier(os.Args[2], os.Args[3])
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", os.Args[1])
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "scenario %s failed: %v\n", os.Args[1], err)
		os.Exit(1)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(facts); err != nil {
		fmt.Fprintf(os.Stderr, "encode facts: %v\n", err)
		os.Exit(1)
	}
}
