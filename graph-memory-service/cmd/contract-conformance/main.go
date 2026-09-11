// Command contract-conformance runs the GMS (Go) adapter of the FND-001
// shared JCS/SHA-256 golden conformance corpus (Contract §16) and emits the
// machine-readable S1 report consumed by the INT-001 cross-language harness.
//
// The runner derives accept/reject, the reason code, canonical bytes and the
// digest for every manifest case independently of the golden expectations,
// and only then compares against expected.json. It never writes or rewrites
// any expected value.
//
// Usage:
//
//	contract-conformance --fixtures <dir> --report <out.json>
//
// Exit codes: 0 all cases pass; 1 at least one case fails; 2 corpus/manifest
// integrity error. The fixtures directory defaults to RSIH_CONFORMANCE_DIR or
// the repository-relative conformance directory.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"river2.dev/graph-memory-service/internal/contract"
)

// reportSchemaVersion is the frozen INT-001 report envelope.
const reportSchemaVersion = "rsih-s1-report.v1"

type reportAdapter struct {
	Language string `json:"language"`
	Runtime  string `json:"runtime"`
	Repo     string `json:"repo"`
}

type reportCase struct {
	ID             string  `json:"id"`
	Category       string  `json:"category"`
	SourcePath     string  `json:"source_path"`
	DerivedAccept  bool    `json:"derived_accept"`
	DerivedReason  *string `json:"derived_reason"`
	DerivedDigest  *string `json:"derived_digest"`
	DerivedLength  *int    `json:"derived_length"`
	DerivedBase64  string  `json:"derived_base64"`
	MatchesExpecte bool    `json:"matches_expected"`
}

type report struct {
	SchemaVersion  string        `json:"schema_version"`
	Adapter        reportAdapter `json:"adapter"`
	ManifestDigest string        `json:"manifest_digest"`
	CaseCount      int           `json:"case_count"`
	AllPassed      bool          `json:"all_passed"`
	Cases          []reportCase  `json:"cases"`
}

func main() {
	os.Exit(run())
}

func run() int {
	fixtures := flag.String("fixtures", "", "conformance fixtures directory (default: $RSIH_CONFORMANCE_DIR or repo-relative lookup)")
	reportPath := flag.String("report", "", "write the rsih-s1-report.v1 JSON report to this path")
	flag.Parse()

	dir := *fixtures
	if dir == "" {
		resolved, err := contract.DefaultConformanceDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			return 2
		}
		dir = resolved
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		fmt.Fprintf(os.Stderr, "ERROR: --fixtures %s is not a directory\n", dir)
		return 2
	}

	res := contract.RunCorpus(dir)

	allPassed := len(res.CorpusErrors) == 0
	cases := make([]reportCase, 0, len(res.Cases))
	for _, c := range res.Cases {
		if !c.OK {
			allPassed = false
		}
		var lengthPtr *int
		if c.DerivedDigest != nil {
			lengthVal := c.DerivedLength
			lengthPtr = &lengthVal
		}
		cases = append(cases, reportCase{
			ID:             c.CaseID,
			Category:       c.Category,
			SourcePath:     c.SourcePath,
			DerivedAccept:  c.DerivedAccept,
			DerivedReason:  c.DerivedReason,
			DerivedDigest:  c.DerivedDigest,
			DerivedLength:  lengthPtr,
			DerivedBase64:  c.DerivedBase64,
			MatchesExpecte: c.OK,
		})
	}
	// Report cases are ordered by id in byte order (INT-001 contract).
	sort.Slice(cases, func(i, j int) bool { return cases[i].ID < cases[j].ID })

	out := report{
		SchemaVersion: reportSchemaVersion,
		Adapter: reportAdapter{
			Language: "go",
			Runtime:  runtime.Version(),
			Repo:     "gms",
		},
		ManifestDigest: res.ManifestDigest,
		CaseCount:      len(cases),
		AllPassed:      allPassed,
		Cases:          cases,
	}

	if *reportPath != "" {
		data, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: marshal report: %v\n", err)
			return 2
		}
		data = append(data, '\n')
		if err := os.WriteFile(*reportPath, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: write report %s: %v\n", *reportPath, err)
			return 2
		}
	}

	for _, e := range res.CorpusErrors {
		fmt.Printf("ERROR: %s\n", e)
	}
	for _, c := range res.Cases {
		if c.OK {
			digest := "-"
			if c.DerivedDigest != nil {
				digest = *c.DerivedDigest
			}
			reason := "-"
			if c.DerivedReason != nil {
				reason = *c.DerivedReason
			}
			fmt.Printf("PASS %s accept=%v digest=%s length=%d reason=%s\n", c.CaseID, c.DerivedAccept, digest, c.DerivedLength, reason)
		} else {
			fmt.Printf("FAIL %s: %s\n", c.CaseID, joinProblems(c.Problems))
		}
	}
	failed := 0
	for _, c := range res.Cases {
		if !c.OK {
			failed++
		}
	}
	fmt.Printf("SUMMARY: cases=%d passed=%d failed=%d corpus_errors=%d\n", len(res.Cases), len(res.Cases)-failed, failed, len(res.CorpusErrors))
	fmt.Printf("report: schema=%s all_passed=%v manifest_digest=%s fixtures=%s\n", reportSchemaVersion, allPassed, res.ManifestDigest, filepath.Clean(dir))

	if len(res.CorpusErrors) > 0 {
		return 2
	}
	if !allPassed {
		return 1
	}
	return 0
}

func joinProblems(problems []string) string {
	out := ""
	for i, p := range problems {
		if i > 0 {
			out += "; "
		}
		out += p
	}
	return out
}
