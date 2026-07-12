# Session Goals

Session goals are a small, durable user/operator contract for one routed conversation. They help an agent keep working toward an explicit objective across normal turns without creating a workflow runtime.

## Scope And Storage

There is at most one goal per canonical routed session key. Goal data is persisted in the existing workspace state file and survives process restarts. A goal stores its objective, current status, optional note, creation/update times, and the most recent blocked/completed transition times.

The goal is separate from chat history. A history reset does not implicitly change the goal unless the command semantics below say so.

## Commands

- `/goal` or `/goal status` shows the current goal.
- `/goal start <objective>` creates a goal. `/goal <objective>` is the implicit form.
- `/goal edit <objective>` changes the objective while preserving status.
- `/goal pause [note]`, `/goal resume [note]`, `/goal complete [note]`, `/goal block [note]`, and `/goal clear` manage it explicitly.
- `/new` starts fresh history and clears the current goal.
- `/reset` starts fresh history but preserves the goal.
- `/reset clear` clears the routed session override and the current goal.
- `/clear` clears stored chat history but preserves the goal.

Top-level command definitions are shared with Telegram registration, so `/goal` and `/new` are registered alongside other supported commands.

## Model Behavior

For an active goal, normal LLM turns receive a compact dynamic reminder:

```text
Active goal: <objective> - advance it or update its status (get_goal/update_goal).
```

Paused, blocked, and complete goals are not injected. The reminder is not saved to chat history. It is also excluded from no-history turns, including subturns and `/btw` side questions.

The model-facing tools are:

- `get_goal` reads the goal for the current routed session.
- `create_goal` creates a goal only for an explicit user or system request.
- `update_goal` can only mark the current goal `complete` or `blocked`.

Tool calls use the canonical routed session key rather than a temporary history key, keeping them aligned with `/goal` across `/new` and `/reset`.

## Non-Goals

Session goals do not create tasks, schedule work, execute background actions, or replace task/workflow systems. They are intentionally independent from task boards, task packets, task registries, and task execution.

Token budgets and automated retries are future work only if they can use existing accounting and policy boundaries without turning session goals into a workflow engine.
