package usageprojection

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// MemoryStore is the durable-in-process adapter for UsageProjectionStore. Its
// Snapshot and Restore methods provide a versioned persistence boundary for
// server runtime composition without coupling observations to the normative
// graph store.
type MemoryStore struct {
	mu           sync.Mutex
	interactions []Interaction
	assessments  []DiagnosisUtilityAssessment
	checkpoint   func([]byte) error
}

var _ UsageProjectionStore = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// SetCheckpoint installs the server-owned persistence callback used after a
// successful append. It is deliberately not part of UsageProjectionStore: the
// service only needs observation storage, while runtime composition owns file
// durability policy.
func (s *MemoryStore) SetCheckpoint(checkpoint func([]byte) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint = checkpoint
}

func (s *MemoryStore) AppendInteraction(ctx context.Context, interaction Interaction) error {
	s.mu.Lock()
	s.interactions = append(s.interactions, cloneInteraction(interaction))
	checkpoint := s.checkpoint
	s.mu.Unlock()
	return s.checkpointAfterAppend(ctx, checkpoint)
}

func (s *MemoryStore) AppendDiagnosis(ctx context.Context, assessment DiagnosisUtilityAssessment) error {
	s.mu.Lock()
	s.assessments = append(s.assessments, cloneAssessment(assessment))
	checkpoint := s.checkpoint
	s.mu.Unlock()
	return s.checkpointAfterAppend(ctx, checkpoint)
}

func (s *MemoryStore) Read(_ context.Context) (UsageProjectionSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return UsageProjectionSnapshot{
		SchemaVersion: UsageProjectionSnapshotSchemaV1,
		Interactions:  cloneInteractions(s.interactions),
		Assessments:   cloneAssessments(s.assessments),
	}, nil
}

func (s *MemoryStore) Snapshot() ([]byte, error) {
	snapshot, err := s.Read(context.Background())
	if err != nil {
		return nil, err
	}
	return json.Marshal(snapshot)
}

// Restore validates the complete image before replacing state. A malformed or
// unauthorized record cannot partially overwrite an existing projection.
func (s *MemoryStore) Restore(image []byte) error {
	var snapshot UsageProjectionSnapshot
	if err := json.Unmarshal(image, &snapshot); err != nil {
		return fmt.Errorf("usage projection snapshot is not valid JSON: %w", err)
	}
	if snapshot.SchemaVersion != UsageProjectionSnapshotSchemaV1 {
		return fmt.Errorf("usage projection snapshot schema %q is unsupported", snapshot.SchemaVersion)
	}
	for index, interaction := range snapshot.Interactions {
		if err := validateInteraction(interaction); err != nil {
			return fmt.Errorf("usage projection snapshot interaction %d: %w", index, err)
		}
	}
	for index, assessment := range snapshot.Assessments {
		if err := validateDiagnosis(assessment); err != nil {
			return fmt.Errorf("usage projection snapshot assessment %d: %w", index, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.interactions = cloneInteractions(snapshot.Interactions)
	s.assessments = cloneAssessments(snapshot.Assessments)
	return nil
}

func cloneInteractions(values []Interaction) []Interaction {
	out := make([]Interaction, len(values))
	for index, value := range values {
		out[index] = cloneInteraction(value)
	}
	return out
}

func cloneAssessments(values []DiagnosisUtilityAssessment) []DiagnosisUtilityAssessment {
	out := make([]DiagnosisUtilityAssessment, len(values))
	for index, value := range values {
		out[index] = cloneAssessment(value)
	}
	return out
}

func (s *MemoryStore) checkpointAfterAppend(_ context.Context, checkpoint func([]byte) error) error {
	if checkpoint == nil {
		return nil
	}
	image, err := s.Snapshot()
	if err != nil {
		return err
	}
	return checkpoint(image)
}

// PersistToFile writes a snapshot atomically, so a process crash cannot leave
// a truncated usage projection image.
func (s *MemoryStore) PersistToFile(path string) error {
	image, err := s.Snapshot()
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	if _, err := temporary.Write(image); err != nil {
		temporary.Close()
		_ = os.Remove(name)
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		_ = os.Remove(name)
		return err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(name)
		return err
	}
	return os.Rename(name, path)
}

// LoadFromFile restores the projection sidecar. A missing file is a first
// boot; malformed content leaves the existing in-memory projection unchanged.
func (s *MemoryStore) LoadFromFile(path string) error {
	image, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.Restore(image)
}
