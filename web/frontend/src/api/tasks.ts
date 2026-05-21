import { launcherFetch } from "@/api/http"

export interface TaskCompletionMedia {
  ref: string
  type?: string
  filename?: string
  content_type?: string
}

export interface TaskCompletion {
  text?: string
  media?: TaskCompletionMedia[]
}

export interface TaskRecord {
  task_id: string
  runtime: string
  task_kind?: string
  requester_session_key?: string
  owner_key?: string
  scope_kind?: string
  channel?: string
  chat_id?: string
  topic_id?: string
  agent_id?: string
  label?: string
  task: string
  status: string
  delivery_status: string
  notify_policy?: string
  delivery_mode?: string
  created_at: number
  started_at?: number
  ended_at?: number
  last_event_at?: number
  cleanup_after?: number
  error?: string
  progress_summary?: string
  terminal_summary?: string
  completion?: TaskCompletion
}

export interface TasksResponse {
  workspace: string
  store_path: string
  count: number
  tasks: TaskRecord[]
  counts: Record<string, number>
}

async function request<T>(path: string): Promise<T> {
  const res = await launcherFetch(path)
  if (!res.ok) {
    const detail = await res.text()
    throw new Error(detail || `API error: ${res.status} ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

export async function getTasks(params?: {
  limit?: number
  taskKind?: string
}): Promise<TasksResponse> {
  const query = new URLSearchParams()
  if (params?.limit !== undefined) {
    query.set("limit", String(params.limit))
  }
  if (params?.taskKind) {
    query.set("task_kind", params.taskKind)
  }
  const qs = query.toString()
  return request<TasksResponse>(`/api/tasks${qs ? `?${qs}` : ""}`)
}
