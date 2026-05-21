import { createFileRoute } from "@tanstack/react-router"

import { TasksPage } from "@/components/agent/tasks/tasks-page"

export const Route = createFileRoute("/agent/tasks")({
  component: AgentTasksRoute,
})

function AgentTasksRoute() {
  return <TasksPage />
}
