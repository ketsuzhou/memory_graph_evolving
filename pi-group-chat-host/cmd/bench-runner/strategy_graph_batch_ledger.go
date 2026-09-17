package main

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"

	"river2.dev/pi-group-chat-host/internal/skillfence"
)

//go:embed testdata/pinned-0917-ledger.md
var pinned0917LedgerFS embed.FS

const (
	pinned0917ExpectedCount = 16
	pinned0917Namespace     = skillfence.EvaluationNamespace
	pinned0917LineagePrefix = "pin0917-"
	pinned0917Revision      = 1
)

// pinnedSkill is one fail-closed snapshot entry from the 0917 freeze.
type pinnedSkill struct {
	ID             string
	Name           string
	Trigger        string
	SHA256         string
	Body           string
	BodyDigest     string
	SkillReference string
}

type pinnedLedger struct {
	Skills   []pinnedSkill
	ByRef    map[string]pinnedSkill
	BySHA256 map[string]pinnedSkill
}

func loadPinned0917Ledger() (*pinnedLedger, error) {
	raw, err := pinned0917LedgerFS.ReadFile("testdata/pinned-0917-ledger.md")
	if err != nil {
		return nil, fmt.Errorf("pinned 0917 ledger: embed missing: %w", err)
	}
	return parsePinnedLedger(raw)
}

func loadPinnedLedgerFromPath(path string) (*pinnedLedger, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("pinned ledger %s: %w", path, err)
	}
	return parsePinnedLedger(raw)
}

func parsePinnedLedger(raw []byte) (*pinnedLedger, error) {
	blocks := splitConsolidatedSkills(string(raw))
	if len(blocks) != pinned0917ExpectedCount {
		return nil, fmt.Errorf("pinned 0917 ledger: got %d CONSOLIDATED SKILL blocks, want %d (fail-closed)", len(blocks), pinned0917ExpectedCount)
	}
	// splitBlocks strips the header; restore it so body bytes are stable.
	ledger := &pinnedLedger{
		Skills:   make([]pinnedSkill, 0, len(blocks)),
		ByRef:    map[string]pinnedSkill{},
		BySHA256: map[string]pinnedSkill{},
	}
	for i, block := range blocks {
		name := fieldLine(block, "name:")
		trigger := fieldLine(block, "trigger:")
		if name == "" || trigger == "" {
			return nil, fmt.Errorf("pinned 0917 ledger: skill %d missing name or trigger", i+1)
		}
		body := "CONSOLIDATED SKILL\n" + block
		sum := sha256.Sum256([]byte(body))
		hexDigest := hex.EncodeToString(sum[:])
		lineage := pinned0917LineagePrefix + fmt.Sprintf("%02d", i+1)
		ref := fmt.Sprintf("skill://%s/%s@%d", pinned0917Namespace, lineage, pinned0917Revision)
		if _, err := skillfence.ParseExactSkillReference(ref); err != nil {
			return nil, fmt.Errorf("pinned 0917 ledger: skill %d reference %s: %w", i+1, ref, err)
		}
		skill := pinnedSkill{
			ID:             lineage,
			Name:           name,
			Trigger:        trigger,
			SHA256:         hexDigest,
			Body:           body,
			BodyDigest:     "sha256:" + hexDigest,
			SkillReference: ref,
		}
		if _, exists := ledger.ByRef[ref]; exists {
			return nil, fmt.Errorf("pinned 0917 ledger: duplicate reference %s", ref)
		}
		if _, exists := ledger.BySHA256[hexDigest]; exists {
			return nil, fmt.Errorf("pinned 0917 ledger: duplicate sha256 %s", hexDigest)
		}
		ledger.Skills = append(ledger.Skills, skill)
		ledger.ByRef[ref] = skill
		ledger.BySHA256[hexDigest] = skill
	}
	return ledger, nil
}

func fieldLine(block, prefix string) string {
	for _, line := range strings.Split(block, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(trimmed), strings.ToLower(prefix)) {
			return strings.TrimSpace(trimmed[len(prefix):])
		}
	}
	return ""
}

func (l *pinnedLedger) asSkillProposals() []skillProposal {
	if l == nil {
		return nil
	}
	out := make([]skillProposal, 0, len(l.Skills))
	for i, skill := range l.Skills {
		out = append(out, skillProposal{
			Sequence:  i + 1,
			EpisodeID: skill.ID,
			SHA256:    skill.SHA256,
			Text:      skill.Body,
		})
	}
	return out
}

func (l *pinnedLedger) lookup(refOrSHA string) (pinnedSkill, bool) {
	if l == nil {
		return pinnedSkill{}, false
	}
	if skill, ok := l.ByRef[strings.TrimSpace(refOrSHA)]; ok {
		return skill, true
	}
	key := strings.TrimPrefix(strings.TrimSpace(refOrSHA), "sha256:")
	if skill, ok := l.BySHA256[key]; ok {
		return skill, true
	}
	if n, err := strconv.Atoi(strings.TrimSpace(refOrSHA)); err == nil && n >= 1 && n <= len(l.Skills) {
		return l.Skills[n-1], true
	}
	return pinnedSkill{}, false
}
