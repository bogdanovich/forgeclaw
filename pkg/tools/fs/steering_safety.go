package fstools

import toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"

func (*LoadImageTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*WriteFileTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*AppendFileTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*ApplyPatchTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*SendFileTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}
