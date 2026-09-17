package memoryclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSkillEvolutionArmBClientUsesExactHTTPContracts(t *testing.T) {
	profile := SkillEvolutionContextProfile{
		SchemaVersion: "context-profile/1.0", TaskFamily: "code_implementation", RuntimeClass: "pi-group-chat-host/go-linux",
		WorkspaceFeatureTags: []string{"bench-runner", "warm-skill"}, ObservableGuardFacts: []string{"runner-local-ledger-primary"},
		ToolPolicyRef:    SkillEvolutionVersionedRef{ID: "tool-policy", Version: 1, Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		EnvironmentClass: "sandboxed-linux",
	}
	subject := SkillEvolutionSubject{CandidateRef: &SkillEvolutionCandidateRef{
		SchemaVersion: "gms.candidate-artifact-ref.v2", CandidateID: "runner-skill-a", Kind: "human_procedure", BodyDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", OriginType: "skill_proposal",
		OriginRef: SkillEvolutionVersionedRef{ID: "runner-proposal-a", Version: 1, Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
	}}
	interaction := SkillEvolutionInteractionRecordRequest{RequestID: "interaction-1", Subject: subject, ContextProfile: profile, Stage: "selected", EvidenceRefs: []SkillEvolutionEvidenceRef{}}
	diagnosis := SkillEvolutionDiagnosisRecordRequest{
		RequestID: "diagnosis-1", AssessmentID: "assessment-1", Subject: subject, ContextProfile: profile, ReturnedPathID: "path-1", AddressedAgentID: "agent-primary",
		ContributionScoreMicros: 0, ConfidenceMicros: 0, Rationale: "observational only", EvidenceRefs: []SkillEvolutionEvidenceRef{}, AdoptionEvidenceRefs: []SkillEvolutionEvidenceRef{}, CounterevidenceRefs: []SkillEvolutionEvidenceRef{},
		RubricRef: SkillEvolutionVersionedRef{ID: "runner-diagnosis-rubric", Version: 1, Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"}, EvaluationBatchID: "eval-held-out-1", DiagnosticOnly: true,
	}

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if got := r.Header.Get("Authorization"); got != "Bearer graph-token" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		switch r.URL.Path {
		case SkillEvolutionInteractionRecordPath:
			if r.Method != http.MethodPost || got["stage"] != "selected" {
				t.Fatalf("interaction = %s", body)
			}
			if _, exists := got["source_lineage_id"]; exists {
				t.Fatalf("client must not set server-owned writer identity: %s", body)
			}
			_, _ = io.WriteString(w, `{"schema_version":"gms.arm-b-write.v1","request_id":"interaction-1","recorded":true,"non_authoritative":true,"writer_identity":"server-arm-b"}`)
		case SkillEvolutionDiagnosisRecordPath:
			if r.Method != http.MethodPost || got["evaluation_batch_id"] != "eval-held-out-1" || got["diagnostic_only"] != true {
				t.Fatalf("diagnosis = %s", body)
			}
			_, _ = io.WriteString(w, `{"schema_version":"gms.arm-b-write.v1","request_id":"diagnosis-1","recorded":true,"non_authoritative":true,"writer_identity":"server-arm-b"}`)
		case SkillEvolutionAdvisoryReadPath:
			if r.Method != http.MethodPost || got["request_id"] != "advisory-1" {
				t.Fatalf("advisory request = %s", body)
			}
			_, _ = io.WriteString(w, `{"schema_version":"gms.advisory-candidates.v1","request_id":"advisory-1","non_authoritative":true,"usage_summary":{"policy_version":"usage-ranking-v1","entries":[]},"candidates":[{"candidate_id":"candidate-procedure","candidate_ref":{"schema_version":"gms.candidate-artifact-ref.v2","candidate_id":"candidate-procedure","kind":"human_procedure","body_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","origin_type":"skill_proposal","origin_ref":{"id":"proposal-procedure","version":1,"digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},"kind":"human_procedure","guidance":"Inspect the smallest failing input.","non_authoritative":true}]}`)
		case SkillEvolutionCandidateOutcomesReadPath:
			if r.Method != http.MethodPost || got["request_id"] != "outcomes-1" {
				t.Fatalf("outcomes request = %s", body)
			}
			_, _ = io.WriteString(w, `{"schema_version":"gms.candidate-outcomes-read.v1","request_id":"outcomes-1","outcomes":[{"candidate_ref":{"schema_version":"gms.candidate-artifact-ref.v2","candidate_id":"candidate-procedure","kind":"human_procedure","body_digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","origin_type":"skill_proposal","origin_ref":{"id":"proposal-procedure","version":1,"digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},"status":"activated"}]}`)
		default:
			t.Fatalf("unexpected path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "graph-token", server.Client(), 1<<20)
	if response, err := client.RecordSkillEvolutionInteraction(context.Background(), interaction); err != nil || !response.Recorded || !response.NonAuthoritative {
		t.Fatalf("interaction response=%+v err=%v", response, err)
	}
	if response, err := client.RecordSkillEvolutionDiagnosis(context.Background(), diagnosis); err != nil || !response.Recorded || !response.NonAuthoritative {
		t.Fatalf("diagnosis response=%+v err=%v", response, err)
	}
	advisory, err := client.ReadSkillEvolutionAdvisoryCandidates(context.Background(), SkillEvolutionAdvisoryReadRequest{RequestID: "advisory-1", ContextProfile: profile})
	if err != nil {
		t.Fatal(err)
	}
	if len(advisory.Candidates) != 1 || advisory.Candidates[0].CandidateID != "candidate-procedure" || advisory.Candidates[0].CandidateRef == nil || advisory.Candidates[0].CandidateRef.BodyDigest != "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" || advisory.Candidates[0].Guidance != "Inspect the smallest failing input." {
		t.Fatalf("advisory = %+v", advisory)
	}
	outcomes, err := client.ReadSkillEvolutionCandidateOutcomes(context.Background(), SkillEvolutionCandidateOutcomesReadRequest{RequestID: "outcomes-1", CandidateRefs: []SkillEvolutionCandidateRef{*advisory.Candidates[0].CandidateRef}})
	if err != nil || len(outcomes.Outcomes) != 1 || outcomes.Outcomes[0].Status != "activated" {
		t.Fatalf("outcomes=%+v err=%v", outcomes, err)
	}
	if calls != 4 {
		t.Fatalf("HTTP calls = %d, want 4", calls)
	}
}
