package main

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCoordinateParallelBatchEnforcesBarriersAndSingleMerge(t *testing.T) {
	trainStarted := make(chan int, 3)
	diagnosisStarted := make(chan int, 3)
	trainRelease := map[int]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	diagnosisRelease := map[int]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}

	var mu sync.Mutex
	var events []string
	consolidations := 0
	hooks := parallelBatchHooks[int, int, int, int, int]{
		RunTrain: func(_ context.Context, job parallelBatchJob[int]) int {
			trainStarted <- job.Sequence
			<-trainRelease[job.Sequence]
			return job.Sequence
		},
		ReduceTrain: func(results []parallelBatchResult[int]) ([]parallelBatchJob[int], error) {
			mu.Lock()
			events = append(events, "reduce-train")
			mu.Unlock()
			jobs := make([]parallelBatchJob[int], 0, len(results))
			for _, result := range results {
				jobs = append(jobs, parallelBatchJob[int]{Sequence: result.Sequence, Value: result.Value})
			}
			return jobs, nil
		},
		RunDiagnosis: func(_ context.Context, job parallelBatchJob[int]) int {
			diagnosisStarted <- job.Sequence
			<-diagnosisRelease[job.Sequence]
			return job.Sequence
		},
		ReduceDiagnosis: func(_ []parallelBatchResult[int]) error {
			mu.Lock()
			events = append(events, "reduce-diagnosis")
			mu.Unlock()
			return nil
		},
		Consolidate: func() error {
			mu.Lock()
			defer mu.Unlock()
			consolidations++
			events = append(events, "consolidate")
			return nil
		},
		RunTest: func(_ context.Context, job parallelBatchJob[int]) error {
			mu.Lock()
			events = append(events, "test-"+string(rune('0'+job.Sequence)))
			mu.Unlock()
			return nil
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- coordinateParallelBatch(context.Background(), 3,
			[]parallelBatchJob[int]{{Sequence: 3}, {Sequence: 1}, {Sequence: 2}},
			[]parallelBatchJob[int]{{Sequence: 5}, {Sequence: 4}}, hooks)
	}()

	awaitStarts(t, trainStarted, 3)
	mu.Lock()
	if len(events) != 0 || consolidations != 0 {
		t.Fatalf("train reducer or later phase ran before all train workers completed: events=%v merges=%d", events, consolidations)
	}
	mu.Unlock()
	// Deliberately complete out of order. The reducer must still observe 1,2,3.
	close(trainRelease[3])
	close(trainRelease[2])
	close(trainRelease[1])

	awaitStarts(t, diagnosisStarted, 3)
	mu.Lock()
	if !reflect.DeepEqual(events, []string{"reduce-train"}) || consolidations != 0 {
		t.Fatalf("diagnosis reducer/merge/test crossed diagnosis barrier: events=%v merges=%d", events, consolidations)
	}
	mu.Unlock()
	close(diagnosisRelease[2])
	close(diagnosisRelease[3])
	close(diagnosisRelease[1])

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not complete")
	}
	mu.Lock()
	defer mu.Unlock()
	// Tests run concurrently, so their completion order is not deterministic;
	// the barriers before them are.
	if len(events) < 3 || !reflect.DeepEqual(events[:3], []string{"reduce-train", "reduce-diagnosis", "consolidate"}) {
		t.Fatalf("phase order prefix = %v, want reduce-train, reduce-diagnosis, consolidate first", events)
	}
	testEvents := append([]string(nil), events[3:]...)
	sort.Strings(testEvents)
	if !reflect.DeepEqual(testEvents, []string{"test-4", "test-5"}) {
		t.Fatalf("test events = %v, want exactly test-4 and test-5", events)
	}
	if consolidations != 1 {
		t.Fatalf("consolidation calls = %d, want exactly 1", consolidations)
	}
}

func TestCoordinateParallelBatchTestStageRunsToCompletionOnFailure(t *testing.T) {
	testStarted := make(chan int, 2)
	testRelease := map[int]chan struct{}{4: make(chan struct{}), 5: make(chan struct{})}
	var mu sync.Mutex
	ran := []int{}
	hooks := parallelBatchHooks[int, int, int, int, int]{
		RunTrain:        func(_ context.Context, job parallelBatchJob[int]) int { return job.Sequence },
		ReduceTrain:     func(results []parallelBatchResult[int]) ([]parallelBatchJob[int], error) { return nil, nil },
		RunDiagnosis:    func(_ context.Context, job parallelBatchJob[int]) int { return job.Sequence },
		ReduceDiagnosis: func([]parallelBatchResult[int]) error { return nil },
		Consolidate:     func() error { return nil },
		RunTest: func(_ context.Context, job parallelBatchJob[int]) error {
			testStarted <- job.Sequence
			<-testRelease[job.Sequence]
			mu.Lock()
			ran = append(ran, job.Sequence)
			mu.Unlock()
			if job.Sequence == 4 {
				return fmt.Errorf("test 4 infrastructure failure")
			}
			return nil
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- coordinateParallelBatch(context.Background(), 2,
			nil, []parallelBatchJob[int]{{Sequence: 4}, {Sequence: 5}}, hooks)
	}()
	awaitStarts(t, testStarted, 2)
	// Both tests must be running before either finishes: one failure must not
	// starve the remaining held-out episodes.
	close(testRelease[4])
	close(testRelease[5])
	err := <-done
	if err == nil || !strings.Contains(err.Error(), "test 4") {
		t.Fatalf("coordinator error = %v, want the sequence-4 failure", err)
	}
	mu.Lock()
	defer mu.Unlock()
	completed := append([]int(nil), ran...)
	sort.Ints(completed)
	if !reflect.DeepEqual(completed, []int{4, 5}) {
		t.Fatalf("tests that ran = %v, want both 4 and 5", ran)
	}
}

func TestCoordinateParallelBatchReducersReceiveStableSequenceOrder(t *testing.T) {
	var trainOrder, diagnosisOrder []int
	hooks := parallelBatchHooks[int, int, int, int, int]{
		RunTrain: func(_ context.Context, job parallelBatchJob[int]) int { return job.Sequence * 10 },
		ReduceTrain: func(results []parallelBatchResult[int]) ([]parallelBatchJob[int], error) {
			for _, result := range results {
				trainOrder = append(trainOrder, result.Sequence)
				if result.Value != result.Sequence*10 {
					t.Fatalf("result lost sequence association: %+v", result)
				}
			}
			return []parallelBatchJob[int]{{Sequence: 9}, {Sequence: 7}, {Sequence: 8}}, nil
		},
		RunDiagnosis: func(_ context.Context, job parallelBatchJob[int]) int { return job.Sequence },
		ReduceDiagnosis: func(results []parallelBatchResult[int]) error {
			for _, result := range results {
				diagnosisOrder = append(diagnosisOrder, result.Sequence)
			}
			return nil
		},
		Consolidate: func() error { return nil },
		RunTest:     func(context.Context, parallelBatchJob[int]) error { return nil },
	}
	if err := coordinateParallelBatch(context.Background(), 3, []parallelBatchJob[int]{{Sequence: 30}, {Sequence: 10}, {Sequence: 20}}, nil, hooks); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(trainOrder, []int{10, 20, 30}) {
		t.Fatalf("train reducer order = %v", trainOrder)
	}
	if !reflect.DeepEqual(diagnosisOrder, []int{7, 8, 9}) {
		t.Fatalf("diagnosis reducer order = %v", diagnosisOrder)
	}
}

func TestPreassignFamilyEpisodesAndDiagnosisRoomsAreStable(t *testing.T) {
	episodes := []manifestEpisode{
		{EpisodeID: "late", FamilyID: "f", Split: "test", Order: intPtr(3)},
		{EpisodeID: "first", FamilyID: "f", Split: "train", Order: intPtr(1)},
		{EpisodeID: "second", FamilyID: "f", Split: "train", Order: intPtr(2)},
	}
	next := 4
	assigned := preassignFamilyEpisodes(episodes, "f", &next)
	if got := []int{assigned[0].sequence, assigned[1].sequence, assigned[2].sequence}; !reflect.DeepEqual(got, []int{5, 6, 7}) {
		t.Fatalf("assigned sequences = %v", got)
	}
	if assigned[0].episode.EpisodeID != "first" || assigned[2].episode.EpisodeID != "late" {
		t.Fatalf("assignment ignored manifest order: %+v", assigned)
	}
	roomA, sharedA, privateA, memoryA := warmSkillDiagnosisRoom("f", 5)
	roomB, sharedB, privateB, memoryB := warmSkillDiagnosisRoom("f", 6)
	if roomA == roomB || sharedA == sharedB || privateA == privateB || memoryA == memoryB {
		t.Fatal("parallel diagnosis jobs must have unique rooms and spaces")
	}
}

func awaitStarts(t *testing.T, started <-chan int, count int) {
	t.Helper()
	seen := map[int]bool{}
	for len(seen) < count {
		select {
		case sequence := <-started:
			seen[sequence] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d/%d workers started", len(seen), count)
		}
	}
}

func TestReduceBatchDiagnosisDedupesAndStampsInSequenceOrder(t *testing.T) {
	proposal := "SKILL PROPOSAL\nname: retry carefully\ntrigger: transient failure\nsteps: 1. inspect 2. retry\npitfalls: blind retry"
	ledger := map[string][]skillProposal{}
	config := armConfig{arm: "warm-skill-batch", outDir: t.TempDir()}
	first, second := attemptRecord{}, attemptRecord{}
	results := []parallelBatchResult[batchDiagnosisResult]{
		{Sequence: 1, Value: batchDiagnosisResult{sequence: 1, episodeID: "first", reply: &proposal}},
		{Sequence: 2, Value: batchDiagnosisResult{sequence: 2, episodeID: "second", reply: &proposal}},
	}
	for index, result := range results {
		record := []*attemptRecord{&first, &second}[index]
		if err := reduceBatchDiagnosis(config, "family", result.Value, &ledger, record); err != nil {
			t.Fatal(err)
		}
	}
	if len(ledger["family"]) != 1 || ledger["family"][0].Sequence != 1 || ledger["family"][0].EpisodeID != "first" {
		t.Fatalf("stable first proposal did not win dedupe: %+v", ledger["family"])
	}
	if first.SkillProposalStatus != "published" || first.SkillProposalCount != 1 || first.SkillProposalsTotal != 1 {
		t.Fatalf("first record not stamped as publisher: %+v", first)
	}
	if second.SkillProposalStatus != "duplicate" || second.SkillProposalCount != 0 || second.SkillProposalsTotal != 1 {
		t.Fatalf("second record not stamped as duplicate: %+v", second)
	}
}
