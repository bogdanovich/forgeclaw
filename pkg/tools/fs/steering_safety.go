package fstools

import toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"

func (*LoadImageTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyReadOnly
}

func (*WriteFileTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*AppendFileTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*ApplyPatchTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*SendFileTool) ToolSteeringSafety() toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}
