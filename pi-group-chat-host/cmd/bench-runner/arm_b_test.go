package main

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"river2.dev/pi-group-chat-host/internal/memoryclient"
)

type fakeArmBClient struct {
	interactions []memoryclient.SkillEvolutionInteractionRecordRequest
	diagnoses    []memoryclient.SkillEvolutionDiagnosisRecordRequest
	advisory     memoryclient.SkillEvolutionAdvisoryReadResponse
	outcomes     memoryclient.SkillEvolutionCandidateOutcomesReadResponse
	err          error
	unrecorded   bool
}

func (f *fakeArmBClient) RecordSkillEvolutionInteraction(_ context.Context, request memoryclient.SkillEvolutionInteractionRecordRequest) (memoryclient.SkillEvolutionWriteResponse, error) {
	f.interactions = append(f.interactions, request)
	return memoryclient.SkillEvolutionWriteResponse{Recorded: !f.unrecorded, NonAuthoritative: true}, f.err
}
func (f *fakeArmBClient) RecordSkillEvolutionDiagnosis(_ context.Context, request memoryclient.SkillEvolutionDiagnosisRecordRequest) (memoryclient.SkillEvolutionWriteResponse, error) {
	f.diagnoses = append(f.diagnoses, request)
	return memoryclient.SkillEvolutionWriteResponse{Recorded: !f.unrecorded, NonAuthoritative: true}, f.err
}
func (f *fakeArmBClient) ReadSkillEvolutionAdvisoryCandidates(_ context.Context, _ memoryclient.SkillEvolutionAdvisoryReadRequest) (memoryclient.SkillEvolutionAdvisoryReadResponse, error) {
	return f.advisory, f.err
}

func TestArmBReporterRetrievalStagesNeverClaimAdoption(t *testing.T) {
	proposal := skillProposal{Sequence: 3, EpisodeID: "train-3", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Text: "inspect the failing input"}
	fake := &fakeArmBClient{}
	reporter := newArmBReporter(fake, func(string, ...any) {})
	record := attemptRecord{EvaluationID: "eval", EpisodeID: "test-4", FamilyID: "code", Arm: "warm-skill"}

	reporter.reportRetrievedSkills(context.Background(), &record, []skillProposal{proposal}, "@task-agent @memory-agent\n[skill aaaaaaaaaaaa from episode train-3]\ninspect the failing input", true)

	if got := interactionStages(fake.interactions); !reflect.DeepEqual(got, []string{"selected", "exposed"}) {
		t.Fatalf("reported stages = %v, want selected then exposed", got)
	}
	for _, interaction := range fake.interactions {
		if interaction.Stage == "adopted" || interaction.Stage == "matched" {
			t.Fatalf("retrieval output must not overclaim %q: %+v", interaction.Stage, interaction)
		}
	}
	if record.SkillEvolutionInteractionReports != 2 || record.SkillEvolutionInteractionFailures != 0 {
		t.Fatalf("interaction telemetry = %+v", record)
	}
}

func TestArmBReporterFailuresAreCountedAndDoNotTouchGradingFields(t *testing.T) {
	fake := &fakeArmBClient{err: errors.New("GMS unavailable")}
	reporter := newArmBReporter(fake, func(string, ...any) {})
	record := attemptRecord{EvaluationID: "eval", EpisodeID: "test-4", FamilyID: "code", Arm: "warm-skill", Status: "completed"}
	proposal := skillProposal{EpisodeID: "train-1", SHA256: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Text: "use a checklist"}

	reporter.reportRetrievedSkills(context.Background(), &record, []skillProposal{proposal}, "@task-agent @memory-agent\n[skill bbbbbbbbbbbb from episode train-1]", true)
	reporter.reportDiagnosis(context.Background(), &record, proposal, "held-out-eval")
	_, err := reporter.advisoryLedger(context.Background(), &record)
	if err != nil {
		t.Fatalf("advisory failures must be fail-open, got %v", err)
	}

	if record.SkillEvolutionInteractionFailures != 2 || record.SkillEvolutionDiagnosisFailures != 1 || record.SkillEvolutionAdvisoryFailures != 1 {
		t.Fatalf("failure counters = %+v", record)
	}
	if record.Status != "completed" || record.FailureKind != "" || record.Error != "" || record.FinalOutput != nil {
		t.Fatalf("reporting failure changed grading record: %+v", record)
	}
}

func TestArmBReporterAdvisoryLedgerSupplementsWithoutMutatingLocalLedger(t *testing.T) {
	local := []skillProposal{{EpisodeID: "train-1", SHA256: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", Text: "local ledger guidance"}}
	fake := &fakeArmBClient{advisory: memoryclient.SkillEvolutionAdvisoryReadResponse{Candidates: []memoryclient.SkillEvolutionAdvisoryCandidate{
		{CandidateID: "candidate-1", CandidateRef: &memoryclient.SkillEvolutionCandidateRef{SchemaVersion: "gms.candidate-artifact-ref.v2", CandidateID: "candidate-1", Kind: "human_procedure", BodyDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", OriginType: "skill_proposal", OriginRef: memoryclient.SkillEvolutionVersionedRef{ID: "proposal-1", Version: 1, Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}, Kind: "human_procedure", Guidance: "gms advisory guidance"},
		{CandidateID: "candidate-tool", Kind: "tool", Guidance: "must never enter the text ledger"},
	}}}
	reporter := newArmBReporter(fake, func(string, ...any) {})
	record := attemptRecord{EvaluationID: "eval", EpisodeID: "test-4", FamilyID: "code", Arm: "warm-skill"}
	advisory, err := reporter.advisoryLedger(context.Background(), &record)
	if err != nil {
		t.Fatal(err)
	}
	combined := supplementLedger(local, advisory)
	if len(local) != 1 || local[0].Text != "local ledger guidance" {
		t.Fatalf("local ledger mutated: %+v", local)
	}
	if len(advisory) != 1 || advisory[0].EpisodeID != "gms-advisory-candidate-1" {
		t.Fatalf("advisory parse = %+v", advisory)
	}
	if len(combined) != 2 || combined[0].Text != "local ledger guidance" || combined[1].Text != "gms advisory guidance" {
		t.Fatalf("combined ledger = %+v", combined)
	}
	if record.SkillEvolutionAdvisoryCandidates != 1 || record.SkillEvolutionAdvisoryFailures != 0 {
		t.Fatalf("advisory telemetry = %+v", record)
	}
}

func interactionStages(items []memoryclient.SkillEvolutionInteractionRecordRequest) []string {
	stages := make([]string, 0, len(items))
	for _, item := range items {
		stages = append(stages, item.Stage)
	}
	return stages
}

func TestReduceBatchDiagnosisReportsOnlyAcceptedProposal(t *testing.T) {
	proposal := "SKILL PROPOSAL\nname: retry carefully\ntrigger: transient failure\nsteps: 1. inspect 2. retry\npitfalls: blind retry"
	fake := &fakeArmBClient{}
	config := armConfig{arm: "warm-skill-batch", outDir: t.TempDir(), armBReporter: newArmBReporter(fake, func(string, ...any) {})}
	ledger := map[string][]skillProposal{}
	first, duplicate := attemptRecord{EvaluationID: "eval", EpisodeID: "first", FamilyID: "family", Arm: "warm-skill-batch", Split: "train"}, attemptRecord{EvaluationID: "eval", EpisodeID: "duplicate", FamilyID: "family", Arm: "warm-skill-batch", Split: "train"}
	if err := reduceBatchDiagnosis(config, "family", batchDiagnosisResult{sequence: 1, episodeID: "first", reply: &proposal}, &ledger, &first); err != nil {
		t.Fatal(err)
	}
	if err := reduceBatchDiagnosis(config, "family", batchDiagnosisResult{sequence: 2, episodeID: "duplicate", reply: &proposal}, &ledger, &duplicate); err != nil {
		t.Fatal(err)
	}
	if len(fake.diagnoses) != 1 || first.SkillEvolutionDiagnosisReports != 1 || duplicate.SkillEvolutionDiagnosisReports != 0 {
		t.Fatalf("diagnosis reports=%d, first=%+v duplicate=%+v", len(fake.diagnoses), first, duplicate)
	}
	if fake.diagnoses[0].EvaluationBatchID != "training:eval:warm-skill-batch:first" {
		t.Fatalf("batch id = %q", fake.diagnoses[0].EvaluationBatchID)
	}
}

func TestArmBReporterCountsUnrecordedWriteAsFailure(t *testing.T) {
	fake := &fakeArmBClient{unrecorded: true}
	reporter := newArmBReporter(fake, func(string, ...any) {})
	record := attemptRecord{EvaluationID: "eval", EpisodeID: "test-4", FamilyID: "code", Arm: "warm-skill", Status: "completed"}
	proposal := skillProposal{EpisodeID: "train-1", SHA256: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", Text: "use a checklist"}

	reporter.reportDiagnosis(context.Background(), &record, proposal, "held-out-eval")
	if record.SkillEvolutionDiagnosisReports != 0 || record.SkillEvolutionDiagnosisFailures != 1 {
		t.Fatalf("unrecorded diagnosis must count as failure: %+v", record)
	}
	if record.Status != "completed" || record.Error != "" {
		t.Fatalf("unrecorded report changed grading record: %+v", record)
	}
}

func TestReduceBatchDiagnosisDedupesIdenticalBlocksWithinOneReply(t *testing.T) {
	proposal := "SKILL PROPOSAL\nname: retry carefully\ntrigger: transient failure\nsteps: 1. inspect 2. retry\npitfalls: blind retry"
	fake := &fakeArmBClient{}
	config := armConfig{arm: "warm-skill-batch", outDir: t.TempDir(), armBReporter: newArmBReporter(fake, func(string, ...any) {})}
	ledger := map[string][]skillProposal{}
	record := attemptRecord{EvaluationID: "eval", EpisodeID: "first", FamilyID: "family", Arm: "warm-skill-batch", Split: "train"}
	reply := proposalWithDuplicateBlock(proposal)
	if err := reduceBatchDiagnosis(config, "family", batchDiagnosisResult{sequence: 1, episodeID: "first", reply: &reply}, &ledger, &record); err != nil {
		t.Fatal(err)
	}
	if len(ledger["family"]) != 1 || record.SkillProposalCount != 1 || len(fake.diagnoses) != 1 {
		t.Fatalf("same-turn duplicate bypassed dedupe: ledger=%+v record=%+v reports=%d", ledger["family"], record, len(fake.diagnoses))
	}
}

func proposalWithDuplicateBlock(proposal string) string { return proposal + "\n\n" + proposal }

func (f *fakeArmBClient) ReadSkillEvolutionCandidateOutcomes(_ context.Context, _ memoryclient.SkillEvolutionCandidateOutcomesReadRequest) (memoryclient.SkillEvolutionCandidateOutcomesReadResponse, error) {
	return f.outcomes, f.err
}
