package integrationtools

import toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"

func (*WebSearchTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*WebFetchTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*FindSkillsTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*MessageTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*ReactionTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*InstallSkillTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*SendTTSTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

// ToolSteeringSafety keeps MCP calls unknown because remote annotations are
// untrusted hints; continuing requires an explicit trusted wrapper.
func (*MCPTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyUnknown
}
