package armc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
)

// MemoryStore is a server-owned Arm C plan authority. It is not HTTP-facing;
// when composed with a stateful server it supports an atomic sidecar snapshot
// so pending plans survive restart before CompleteArmC.
type MemoryStore struct {
	mu         sync.Mutex
	contracts  map[string]ValidationContract
	manifests  map[string]TaskFamilyManifest
	fixtures   map[string]PairedFixture
	plans      map[string]EvaluationPlan
	checkpoint func([]byte) error
}
type snapshot struct {
	Contracts map[string]ValidationContract `json:"contracts"`
	Manifests map[string]TaskFamilyManifest `json:"manifests"`
	Fixtures  map[string]PairedFixture      `json:"fixtures"`
	Plans     map[string]EvaluationPlan     `json:"plans"`
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{contracts: map[string]ValidationContract{}, manifests: map[string]TaskFamilyManifest{}, fixtures: map[string]PairedFixture{}, plans: map[string]EvaluationPlan{}}
}
func (s *MemoryStore) SetCheckpoint(checkpoint func([]byte) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoint = checkpoint
}
func (s *MemoryStore) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(snapshot{Contracts: s.contracts, Manifests: s.manifests, Fixtures: s.fixtures, Plans: s.plans})
}
func (s *MemoryStore) Restore(image []byte) error {
	var data snapshot
	if err := json.Unmarshal(image, &data); err != nil {
		return fmt.Errorf("armc: snapshot invalid: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if data.Contracts == nil {
		data.Contracts = map[string]ValidationContract{}
	}
	if data.Manifests == nil {
		data.Manifests = map[string]TaskFamilyManifest{}
	}
	if data.Fixtures == nil {
		data.Fixtures = map[string]PairedFixture{}
	}
	if data.Plans == nil {
		data.Plans = map[string]EvaluationPlan{}
	}
	s.contracts, s.manifests, s.fixtures, s.plans = data.Contracts, data.Manifests, data.Fixtures, data.Plans
	return nil
}
func (s *MemoryStore) PersistToFile(path string) error {
	if path == "" {
		return nil
	}
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
func (s *MemoryStore) LoadFromFile(path string) error {
	if path == "" {
		return nil
	}
	image, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.Restore(image)
}
func (s *MemoryStore) changed(checkpoint func([]byte) error) error {
	if checkpoint == nil {
		return nil
	}
	image, err := s.Snapshot()
	if err != nil {
		return err
	}
	return checkpoint(image)
}
func (s *MemoryStore) PutValidationContract(_ context.Context, c ValidationContract) (bool, error) {
	s.mu.Lock()
	if e, ok := s.contracts[c.ContractID]; ok {
		s.mu.Unlock()
		if reflect.DeepEqual(e, c) {
			return false, nil
		}
		return false, fmt.Errorf("armc: contract conflict")
	}
	s.contracts[c.ContractID] = c
	checkpoint := s.checkpoint
	s.mu.Unlock()
	if err := s.changed(checkpoint); err != nil {
		return false, err
	}
	return true, nil
}
func (s *MemoryStore) ValidationContract(_ context.Context, r ValidationContractRef) (ValidationContract, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.contracts[r.ContractID]
	if !ok || c.Ref() != r {
		return ValidationContract{}, fmt.Errorf("armc: validation contract not found")
	}
	return c, nil
}
func (s *MemoryStore) PutTaskFamilyManifest(_ context.Context, m TaskFamilyManifest) (bool, error) {
	s.mu.Lock()
	if e, ok := s.manifests[m.ManifestID]; ok {
		s.mu.Unlock()
		if e == m {
			return false, nil
		}
		return false, fmt.Errorf("armc: manifest conflict")
	}
	s.manifests[m.ManifestID] = m
	checkpoint := s.checkpoint
	s.mu.Unlock()
	if err := s.changed(checkpoint); err != nil {
		return false, err
	}
	return true, nil
}
func (s *MemoryStore) TaskFamilyManifest(_ context.Context, r TaskFamilyManifestRef) (TaskFamilyManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.manifests[r.ManifestID]
	if !ok || m.Ref() != r {
		return TaskFamilyManifest{}, fmt.Errorf("armc: task family manifest not found")
	}
	return m, nil
}
func (s *MemoryStore) PutFixture(_ context.Context, f PairedFixture) (bool, error) {
	s.mu.Lock()
	if e, ok := s.fixtures[f.FixtureID]; ok {
		s.mu.Unlock()
		if reflect.DeepEqual(e, f) {
			return false, nil
		}
		return false, fmt.Errorf("armc: fixture conflict")
	}
	s.fixtures[f.FixtureID] = f
	checkpoint := s.checkpoint
	s.mu.Unlock()
	if err := s.changed(checkpoint); err != nil {
		return false, err
	}
	return true, nil
}
func (s *MemoryStore) Fixture(_ context.Context, id string) (PairedFixture, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, ok := s.fixtures[id]
	if !ok {
		return PairedFixture{}, fmt.Errorf("armc: fixture not found")
	}
	return f, nil
}
func (s *MemoryStore) PutPlan(_ context.Context, p EvaluationPlan) (bool, error) {
	s.mu.Lock()
	if e, ok := s.plans[p.CandidateID]; ok {
		s.mu.Unlock()
		if reflect.DeepEqual(e, p) {
			return false, nil
		}
		return false, fmt.Errorf("armc: plan conflict")
	}
	s.plans[p.CandidateID] = p
	checkpoint := s.checkpoint
	s.mu.Unlock()
	if err := s.changed(checkpoint); err != nil {
		return false, err
	}
	return true, nil
}
func (s *MemoryStore) Plan(_ context.Context, id string) (EvaluationPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.plans[id]
	if !ok {
		return EvaluationPlan{}, fmt.Errorf("armc: plan not found")
	}
	return p, nil
}
