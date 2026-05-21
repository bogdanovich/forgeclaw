# Async Task Delivery

PicoClaw background work now uses an explicit task/completion/delivery shape:

1. A tool or child runtime records a durable task in the task registry.
2. When the async result completes, the runtime builds a typed `AsyncCompletionInput`.
3. The delivery coordinator applies the requested delivery mode: `user_only`, `parent_only`, or `user_and_parent`.
4. User delivery goes through normal outbound text/media delivery.
5. Parent synthesis calls `processAsyncCompletion` directly. It must not publish a synthetic `system` inbound message.
6. The task registry records delivery status, completion id, delivery timestamp, and delivery error if one occurs.

## Status Tools

Use `task_status` for durable task history across spawn, delegate, and future background runtimes. It is the source of truth for completed tasks and restart-persistent state.

`spawn_status` is kept as a compatibility/debug view for tasks started specifically by the `spawn` tool. It is backed by the same durable registry but intentionally remains spawn-only.

## Legacy System Messages

Older async completion paths used synthetic inbound messages with `channel=system` and `kind=async_completion`. That path is now an adapter only, so queued or stored legacy messages can still be processed.

New producers must not enqueue async completions through `PublishInbound(system)`. They should use `AsyncCompletionInput` and the delivery coordinator instead.

## Runtime Smoke Checklist

- Run a simple media task that only sends a video.
- Run a composite media task that sends a video and returns text for parent synthesis.
- Check `task_status` after completion.
- Restart the service.
- Check `task_status` again and confirm completed tasks are still visible.
- Confirm no completed task replays user-visible text or media after restart.
