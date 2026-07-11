# 🔄 Spawn & Async Tasks

> Back to [README](../README.md)

PicoClaw supports **asynchronous task execution** via the `spawn` tool. This is primarily used by the **Heartbeat** system to run long-running tasks without blocking the main agent loop.

## Heartbeat

The heartbeat system periodically checks `<workspace>/HEARTBEAT.md` for scheduled tasks. On first run, a default template is auto-generated. You can customize it to define quick tasks (handled inline) and long tasks (delegated via `spawn`).

**Example `HEARTBEAT.md`:**

```markdown
## Quick Tasks (respond directly)

- Report current time

## Long Tasks (use spawn for async)

- Search the web for AI news and summarize
- Check email and report important messages
```

**Key behaviors:**

| Feature                 | Description                                                                 |
| ----------------------- | --------------------------------------------------------------------------- |
| **spawn**               | Creates an async task/subagent and records it in the durable task registry  |
| **Independent context** | Subagent has its own context, no session history                            |
| **Delivery mode**       | Completion can be delivered to the user, the parent agent, or both          |
| **Non-blocking**        | After spawning, heartbeat continues to next task                            |
| **Status**              | Use `task_status` for durable status; `spawn_status` is a spawn-only view   |

#### How Subagent Communication Works

```
Heartbeat triggers
    ↓
Agent reads HEARTBEAT.md
    ↓
For long task: spawn subagent
    ↓                           ↓
Continue to next task      Subagent works independently
    ↓                           ↓
All tasks done            Subagent completes task record
    ↓                           ↓
Respond HEARTBEAT_OK      Delivery coordinator routes result
```

The subagent has access to its configured tools, but completion delivery is owned by the async task delivery path. A terminal background task usually uses user delivery. A compositional task can route the completion back to the parent so the parent can synthesize the final user-facing answer. See [Async Task Delivery](../architecture/async-task-delivery.md) for the registry and delivery model.

Use `task_status` and `spawn_status` to inspect durable child-task execution.

**Configuration:**

```json
{
  "heartbeat": {
    "enabled": true,
    "interval": 30
  }
}
```

| Option     | Default | Description                        |
| ---------- | ------- | ---------------------------------- |
| `enabled`  | `true`  | Enable/disable heartbeat           |
| `interval` | `30`    | Check interval in minutes (min: 5) |

**Environment variables:**

* `PICOCLAW_HEARTBEAT_ENABLED=false` to disable
* `PICOCLAW_HEARTBEAT_INTERVAL=60` to change interval
