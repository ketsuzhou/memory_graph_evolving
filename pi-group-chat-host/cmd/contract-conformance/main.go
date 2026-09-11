// Command contract-conformance runs the Host Go adapter against the shared
// FND-001 JCS/SHA-256 golden corpus and emits the machine-readable S1 report
// consumed by the INT-001 cross-language orchestrator.
//
// Usage:
//
//	contract-conformance [--fixtures <dir>] [--report <out.json>]
//
// --fixtures defaults to RSIH_CONFORMANCE_DIR, then to the frozen location
// of the corpus relative to this repository's module root.
//
// Exit codes: 0 all cases pass; 1 at least one case mismatch; 2 corpus or
// reason-policy integrity error (fail closed).
package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"

	"river2.dev/pi-group-chat-host/internal/contract"
)

const reportSchemaVersion = "rsih-s1-report.v1"

func main() {
	os.Exit(run())
}

func run() int {
	fixtures := flag.String("fixtures", "", "conformance fixtures directory (default: RSIH_CONFORMANCE_DIR, else the frozen relative location)")
	reportPath := flag.String("report", "", "write the JSON report to this path (default: stdout)")
	flag.Parse()

	dir := *fixtures
	if dir == "" {
		var err error
		dir, err = contract.ConformanceDir()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
			return 2
		}
	}

	// Fail closed on reason-registry drift before evaluating anything: the
	// system and Host proxy policies must be digest-verified (Contract
	// §13.7.1 receiver_precondition).
	if _, err := contract.LoadReasonBundle(filepath.Join(dir, "policy")); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: reason policy verification failed: %v\n", err)
		return 2
	}

	corpus, err := contract.RunCorpus(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 2
	}
	for _, ce := range corpus.CorpusErrors {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", ce)
	}
	failed := 0
	for _, c := range corpus.Cases {
		if c.OK {
			reason := c.Derived.ReasonCode
			if reason == "" {
				reason = "-"
			}
			length := 0
			if c.Derived.Canonical != nil {
				length = len(c.Derived.Canonical)
			}
			fmt.Printf("PASS %s accept=%t digest=%s length=%d reason=%s\n",
				c.CaseID, c.Derived.Accept, orDash(c.Derived.Digest), length, reason)
		} else {
			failed++
			fmt.Printf("FAIL %s: %s\n", c.CaseID, joinProblems(c.Problems))
		}
	}
	fmt.Printf("SUMMARY: cases=%d passed=%d failed=%d corpus_errors=%d\n",
		len(corpus.Cases), len(corpus.Cases)-failed, failed, len(corpus.CorpusErrors))

	report := buildReport(corpus)
	if *reportPath == "" || *reportPath == "-" {
		os.Stdout.Write(report)
	} else if err := os.WriteFile(*reportPath, report, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: write report %s: %v\n", *reportPath, err)
		return 2
	}

	switch {
	case len(corpus.CorpusErrors) > 0:
		return 2
	case failed > 0:
		return 1
	}
	return 0
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
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

// buildReport renders the rsih-s1-report.v1 document. The report itself is
// emitted as JCS plus a trailing newline, so cross-language orchestrators
// can compare it byte for byte.
func buildReport(corpus *contract.CorpusResult) []byte {
	cases := append([]contract.CaseResult(nil), corpus.Cases...)
	sort.Slice(cases, func(i, j int) bool { return cases[i].CaseID < cases[j].CaseID })

	allPassed := len(corpus.CorpusErrors) == 0 && len(cases) > 0
	caseArray := make(contract.Array, 0, len(cases))
	for _, c := range cases {
		if !c.OK {
			allPassed = false
		}
		entry := contract.NewObject()
		entry.Set("id", contract.String(c.CaseID))
		entry.Set("category", contract.String(c.Category))
		entry.Set("source_path", contract.String(c.SourcePath))
		entry.Set("derived_accept", contract.Bool(c.Derived.Accept))
		if c.Derived.ReasonCode == "" {
			entry.Set("derived_reason", contract.Null{})
		} else {
			entry.Set("derived_reason", contract.String(c.Derived.ReasonCode))
		}
		if c.Derived.Canonical != nil {
			entry.Set("derived_digest", contract.String(c.Derived.Digest))
			entry.Set("derived_length", contract.Number(strconv.Itoa(len(c.Derived.Canonical))))
		} else {
			entry.Set("derived_digest", contract.Null{})
			entry.Set("derived_length", contract.Null{})
		}
		entry.Set("derived_base64", contract.String(base64.StdEncoding.EncodeToString(c.CanonicalFileBytes)))
		entry.Set("matches_expected", contract.Bool(c.OK))
		caseArray = append(caseArray, entry)
	}

	adapter := contract.NewObject()
	adapter.Set("language", contract.String("go"))
	adapter.Set("runtime", contract.String(runtime.Version()))
	adapter.Set("repo", contract.String("host"))

	root := contract.NewObject()
	root.Set("schema_version", contract.String(reportSchemaVersion))
	root.Set("adapter", adapter)
	root.Set("manifest_digest", contract.String(corpus.ManifestDigest))
	root.Set("case_count", contract.Number(strconv.Itoa(len(corpus.Cases))))
	root.Set("all_passed", contract.Bool(allPassed))
	root.Set("cases", caseArray)

	report, err := contract.JCS(root)
	if err != nil {
		// The report only contains canonicalizable scalars; unreachable.
		panic(fmt.Sprintf("report canonicalization failed: %v", err))
	}
	return append(report, '\n')
}
