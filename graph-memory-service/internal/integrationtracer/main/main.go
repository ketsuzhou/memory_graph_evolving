// Command integrationtracer runs one PG-40..43 GMS scenario and prints its
// factual outcome as a single JSON object on stdout. It is a tracer driver,
// not a composition root: PG-50A remains the sole writer of cmd/server.
//
// Usage:
//
//	go run ./internal/integrationtracer/main production-cut <hostFreezePath>
//	go run ./internal/integrationtracer/main batch-diagnosis <hostBarrierFactsPath> <batchManifestPath>
//	go run ./internal/integrationtracer/main replay-activation
//	go run ./internal/integrationtracer/main revocation
package main

import (
	"encoding/json"
	"fmt"
	"os"

	"river2.dev/graph-memory-service/internal/integrationtracer"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: integrationtracer <production-cut <hostFreezePath>|batch-diagnosis <hostBarrierFactsPath> <batchManifestPath>|replay-activation|revocation>")
		os.Exit(2)
	}
	var (
		facts map[string]any
		err   error
	)
	switch os.Args[1] {
	case "production-cut":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "production-cut requires <hostFreezePath>")
			os.Exit(2)
		}
		facts, err = integrationtracer.ProductionCut(os.Args[2])
	case "batch-diagnosis":
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "batch-diagnosis requires <hostBarrierFactsPath> <batchManifestPath>")
			os.Exit(2)
		}
		facts, err = integrationtracer.BatchDiagnosis(os.Args[2], os.Args[3])
	case "replay-activation":
		facts, err = integrationtracer.ReplayActivation()
	case "revocation":
		facts, err = integrationtracer.Revocation()
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
