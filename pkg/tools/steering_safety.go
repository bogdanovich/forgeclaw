package tools

func (*RegexSearchTool) ToolSteeringSafety() SteeringSafety { return SteeringSafetyReadOnly }
func (*BM25SearchTool) ToolSteeringSafety() SteeringSafety  { return SteeringSafetyReadOnly }
func (*TaskStatusTool) ToolSteeringSafety() SteeringSafety  { return SteeringSafetyReadOnly }
func (*SpawnStatusTool) ToolSteeringSafety() SteeringSafety { return SteeringSafetyReadOnly }
func (*GetGoalTool) ToolSteeringSafety() SteeringSafety     { return SteeringSafetyReadOnly }

func (*CreateGoalTool) ToolSteeringSafety() SteeringSafety { return SteeringSafetyCancellable }
func (*UpdateGoalTool) ToolSteeringSafety() SteeringSafety { return SteeringSafetyCancellable }
func (*UpdatePlanTool) ToolSteeringSafety() SteeringSafety { return SteeringSafetyCancellable }
func (*CronTool) ToolSteeringSafety() SteeringSafety       { return SteeringSafetyCancellable }
func (*DelegateTool) ToolSteeringSafety() SteeringSafety   { return SteeringSafetyCancellable }
func (*SpawnTool) ToolSteeringSafety() SteeringSafety      { return SteeringSafetyCancellable }
