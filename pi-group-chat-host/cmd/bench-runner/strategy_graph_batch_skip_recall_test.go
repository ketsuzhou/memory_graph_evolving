package main

import (
	"testing"

	"river2.dev/pi-group-chat-host/internal/runtime"
)

func TestLegacyTurnRequestZeroValueDoesNotSkipRecall(t *testing.T) {
	t.Parallel()
	var request runtime.TurnRequest
	if request.SkipRecall {
		t.Fatal("zero-value TurnRequest must keep pre-turn recall (legacy arms)")
	}
	if request.SkillProtocol != nil {
		t.Fatal("zero-value TurnRequest must not attach a skill protocol")
	}
	authority := runtime.ExecutionAuthority{ProfileKind: "ordinary", ExtraOrdinaryTools: []string{skillGetToolName, skillFeedbackToolName}}
	if len(authority.ExtraOrdinaryTools) != 2 {
		t.Fatal("ExtraOrdinaryTools must be recordable without restricting coding tools")
	}
}
