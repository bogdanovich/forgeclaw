package hardwaretools

import toolshared "github.com/sipeed/picoclaw/pkg/tools/shared"

func (*I2CTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*SPITool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}

func (*SerialTool) ToolSteeringSafety(map[string]any) toolshared.SteeringSafety {
	return toolshared.SteeringSafetyCancellable
}
