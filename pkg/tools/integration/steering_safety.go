package integrationtools

import toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"

func (*WebSearchTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*WebFetchTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*FindSkillsTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*MessageTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*ReactionTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*InstallSkillTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*SendTTSTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}
