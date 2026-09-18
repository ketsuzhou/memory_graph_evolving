package main

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
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
	skill, ok, _ := l.resolveNomination(refOrSHA)
	return skill, ok
}

// resolveNomination maps one memory-authored citation onto the pinned 16-skill
// ledger. Priority: exact skill_reference, unique sha256 prefix (≥8 hex),
// unique entry id (03 / pin0917-03), unique normalized name. Ambiguous or
// zero matches stay unresolved so the Host can audit the raw token.
func (l *pinnedLedger) resolveNomination(raws ...string) (pinnedSkill, bool, string) {
	if l == nil {
		return pinnedSkill{}, false, firstNonEmptyRaw(raws)
	}
	tokens := compactNominationRaws(raws)
	if len(tokens) == 0 {
		return pinnedSkill{}, false, ""
	}
	if skill, ok, amb := l.matchExactReference(tokens); ok {
		return skill, true, ""
	} else if amb {
		return pinnedSkill{}, false, strings.Join(tokens, " ")
	}
	if skill, ok, amb := l.matchUniqueSHA256Prefix(tokens); ok {
		return skill, true, ""
	} else if amb {
		return pinnedSkill{}, false, strings.Join(tokens, " ")
	}
	if skill, ok, amb := l.matchUniqueEntry(tokens); ok {
		return skill, true, ""
	} else if amb {
		return pinnedSkill{}, false, strings.Join(tokens, " ")
	}
	if skill, ok, amb := l.matchUniqueName(tokens); ok {
		return skill, true, ""
	} else if amb {
		return pinnedSkill{}, false, strings.Join(tokens, " ")
	}
	return pinnedSkill{}, false, strings.Join(tokens, " ")
}

func (l *pinnedLedger) matchExactReference(tokens []string) (pinnedSkill, bool, bool) {
	var hit pinnedSkill
	found := 0
	for _, token := range tokens {
		if skill, ok := l.ByRef[strings.TrimSpace(token)]; ok {
			if found == 0 {
				hit = skill
			} else if skill.SkillReference != hit.SkillReference {
				return pinnedSkill{}, false, true
			}
			found++
		}
	}
	return hit, found > 0, false
}

func (l *pinnedLedger) matchUniqueSHA256Prefix(tokens []string) (pinnedSkill, bool, bool) {
	var hit pinnedSkill
	found := 0
	for _, hexDigest := range nominationHexTokens(tokens) {
		if len(hexDigest) < 8 {
			continue
		}
		var matched []pinnedSkill
		if skill, ok := l.BySHA256[hexDigest]; ok {
			matched = append(matched, skill)
		} else {
			for _, skill := range l.Skills {
				if strings.HasPrefix(skill.SHA256, hexDigest) {
					matched = append(matched, skill)
				}
			}
		}
		if len(matched) > 1 {
			return pinnedSkill{}, false, true
		}
		if len(matched) == 1 {
			if found == 0 {
				hit = matched[0]
			} else if matched[0].SkillReference != hit.SkillReference {
				return pinnedSkill{}, false, true
			}
			found++
		}
	}
	return hit, found > 0, false
}

var pin0917EntryRE = regexp.MustCompile(`(?i)pin0917-0*([0-9]{1,2})`)

func (l *pinnedLedger) matchUniqueEntry(tokens []string) (pinnedSkill, bool, bool) {
	seen := map[int]bool{}
	var indexes []int
	add := func(n int) {
		if n < 1 || n > len(l.Skills) || seen[n] {
			return
		}
		seen[n] = true
		indexes = append(indexes, n)
	}
	for _, token := range tokens {
		for _, match := range pin0917EntryRE.FindAllStringSubmatch(token, -1) {
			n, err := strconv.Atoi(match[1])
			if err == nil {
				add(n)
			}
		}
		if trimmed := strings.TrimSpace(token); pin0917EntryRE.FindString(trimmed) == "" {
			if n, err := strconv.Atoi(trimmed); err == nil {
				add(n)
			}
		}
	}
	if len(indexes) > 1 {
		return pinnedSkill{}, false, true
	}
	if len(indexes) == 1 {
		return l.Skills[indexes[0]-1], true, false
	}
	return pinnedSkill{}, false, false
}

func (l *pinnedLedger) matchUniqueName(tokens []string) (pinnedSkill, bool, bool) {
	seen := map[string]pinnedSkill{}
	for _, token := range tokens {
		candidates := []string{token, lastSkillPathSegment(token)}
		for _, candidate := range candidates {
			key := normalizeSkillName(candidate)
			if key == "" {
				continue
			}
			for _, skill := range l.Skills {
				if normalizeSkillName(skill.Name) == key {
					seen[skill.SkillReference] = skill
				}
			}
		}
	}
	if len(seen) > 1 {
		return pinnedSkill{}, false, true
	}
	for _, skill := range seen {
		return skill, true, false
	}
	return pinnedSkill{}, false, false
}

func compactNominationRaws(raws []string) []string {
	out := make([]string, 0, len(raws))
	seen := map[string]bool{}
	for _, raw := range raws {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || seen[trimmed] {
			continue
		}
		seen[trimmed] = true
		out = append(out, trimmed)
	}
	return out
}

func firstNonEmptyRaw(raws []string) string {
	for _, raw := range raws {
		if trimmed := strings.TrimSpace(raw); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func nominationHexTokens(tokens []string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(value string) {
		hexDigest := nominationHex(value)
		if hexDigest == "" || seen[hexDigest] {
			return
		}
		seen[hexDigest] = true
		out = append(out, hexDigest)
	}
	for _, token := range tokens {
		add(token)
		if at := strings.LastIndex(token, "@"); at >= 0 && at+1 < len(token) {
			add(token[at+1:])
		}
	}
	return out
}

func nominationHex(raw string) string {
	value := strings.ToLower(strings.TrimPrefix(strings.TrimSpace(raw), "sha256:"))
	if len(value) < 8 {
		return ""
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return ""
		}
	}
	return value
}

func lastSkillPathSegment(raw string) string {
	value := strings.TrimSpace(raw)
	if i := strings.Index(value, "://"); i >= 0 {
		value = value[i+3:]
	}
	if at := strings.LastIndex(value, "@"); at >= 0 {
		value = value[:at]
	}
	if slash := strings.LastIndex(value, "/"); slash >= 0 {
		value = value[slash+1:]
	}
	return value
}

func normalizeSkillName(value string) string {
	var builder strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			builder.WriteRune(r)
		}
	}
	return builder.String()
}
