package tools

import "strings"

func (*RegexSearchTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyReadOnly
}

func (*BM25SearchTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyReadOnly
}

func (*TaskStatusTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyReadOnly
}

func (*SpawnStatusTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyReadOnly
}
func (*GetGoalTool) ToolSteeringSafety(map[string]any) SteeringSafety { return SteeringSafetyReadOnly }

func (*CreateGoalTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (*UpdateGoalTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (*UpdatePlanTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}
func (*CronTool) ToolSteeringSafety(map[string]any) SteeringSafety { return SteeringSafetyCancellable }
func (*DelegateTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}
func (*SpawnTool) ToolSteeringSafety(map[string]any) SteeringSafety { return SteeringSafetyCancellable }
func (*SubagentTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (*ImageGenerateTool) ToolSteeringSafety(map[string]any) SteeringSafety {
	return SteeringSafetyCancellable
}

func (*ExecTool) ToolSteeringSafety(args map[string]any) SteeringSafety {
	action, _ := args["action"].(string)
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "list", "poll", "read":
		return SteeringSafetyReadOnly
	default:
		return SteeringSafetyCancellable
	}
}
