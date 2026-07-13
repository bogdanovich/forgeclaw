package seahorse

import toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"

func (*ExpandTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*GrepTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}
