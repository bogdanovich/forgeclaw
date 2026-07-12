package agent

import (
	"encoding/json"
	"fmt"

	runtimeevents "github.com/sipeed/picoclaw/pkg/events"
	"github.com/sipeed/picoclaw/pkg/tools"
	"github.com/sipeed/picoclaw/pkg/tools/loopguard"
)

func (p *Pipeline) beforeToolLoopDecision(
	ts *turnState,
	exec *turnExecution,
	tool string,
	args map[string]any,
) (loopguard.Decision, loopguard.Semantics) {
	semantics := loopguard.SemanticsUnknown
	if ts != nil && ts.agent != nil && ts.agent.Tools != nil {
		semantics = ts.agent.Tools.LoopSemantics(tool)
	}
	if exec == nil || exec.loopGuard == nil {
		return loopguard.Decision{Action: loopguard.ActionAllow, Tool: tool}, semantics
	}
	return exec.loopGuard.Before(tool, args, semantics), semantics
}

func (p *Pipeline) afterToolLoopDecision(
	ts *turnState,
	exec *turnExecution,
	tool string,
	args map[string]any,
	result *tools.ToolResult,
	modelContent string,
	semantics loopguard.Semantics,
) loopguard.Decision {
	if exec == nil || exec.loopGuard == nil || result == nil {
		return loopguard.Decision{Action: loopguard.ActionAllow, Tool: tool}
	}
	decision := exec.loopGuard.After(loopguard.Observation{
		Tool: tool, Args: args, ResultText: modelContent,
		Failed: result.IsError, Semantics: semantics,
	})
	if decision.Action != loopguard.ActionAllow {
		p.emitToolLoopDecision(ts, decision)
	}
	return decision
}

func (p *Pipeline) emitToolLoopDecision(ts *turnState, decision loopguard.Decision) {
	if p == nil || ts == nil || decision.Action == loopguard.ActionAllow {
		return
	}
	p.emitEvent(
		runtimeevents.KindAgentToolLoopDecision,
		ts.eventMeta("runTurn", "turn.tool.loop_decision"),
		ToolLoopDecisionPayload{
			Tool: decision.Tool, ArgsHash: decision.ArgsHash,
			Action: string(decision.Action), Code: decision.Code,
			Count: decision.Count, Threshold: decision.Threshold,
		},
	)
}

func blockedToolLoopResult(decision loopguard.Decision) *tools.ToolResult {
	payload := struct {
		Error string `json:"error"`
		Loop  struct {
			Action    string `json:"action"`
			Code      string `json:"code"`
			Tool      string `json:"tool"`
			ArgsHash  string `json:"args_hash"`
			Count     int    `json:"count"`
			Threshold int    `json:"threshold"`
		} `json:"loop_guard"`
	}{Error: decision.Message}
	payload.Loop.Action = string(decision.Action)
	payload.Loop.Code = decision.Code
	payload.Loop.Tool = decision.Tool
	payload.Loop.ArgsHash = decision.ArgsHash
	payload.Loop.Count = decision.Count
	payload.Loop.Threshold = decision.Threshold
	data, err := json.Marshal(payload)
	if err != nil {
		return tools.ErrorResult("tool execution blocked by loop protection")
	}
	return tools.ErrorResult(string(data))
}

func appendToolLoopGuidance(content string, decision loopguard.Decision) string {
	if decision.Action != loopguard.ActionWarn && decision.Action != loopguard.ActionHalt {
		return content
	}
	label := "Tool loop warning"
	if decision.Action == loopguard.ActionHalt {
		label = "Tool loop hard stop"
	}
	return content + fmt.Sprintf(
		"\n\n[%s: %s; count=%d; %s]",
		label, decision.Code, decision.Count, decision.Message,
	)
}
