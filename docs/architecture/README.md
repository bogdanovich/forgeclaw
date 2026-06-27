# Architecture

Internal architecture notes for major runtime mechanisms and subsystem design.

- [Steering](steering.md): injecting messages into a running agent loop between tool calls.
- [Async Task Delivery](async-task-delivery.md): durable task/completion/delivery model, task boards, deliverables, and current source-of-truth boundaries.
- [SubTurn Mechanism](subturn.md): sub-agent coordination, concurrency control, and lifecycle handling.
- [Subagent Model Policy](subagent-model-policy.md): child-run model selection, inherited session override modes, and precedence.
- [Session System](session-system.md): session scope allocation, JSONL persistence, alias compatibility, and migration.
- [Routing System](routing-system.md): agent dispatch, session policy selection, and light/heavy model routing.
- [Durable Ingress](durable-ingress.md): normalized inbound message spool and restart replay semantics.
- [Runtime Events](runtime-events.md): runtime event envelope, centralized event logging, filters, and examples.
- [Channel Lifecycle](channel-lifecycle.md): target channel supervision model, runtime state, inbound activation, and readiness invariants.
- [Workspace Temp Directory](workspace-temp.md): standard scratch path, `PICOCLAW_WORKSPACE_TMP`, and where temporary files should go.
- [Shellguard](shellguard.md): reusable shell command validation, command classification, permission modes, and path-scope limits.
- [Agent Self-Evolution](agent-self-evolution.md): learning records, draft generation, application modes, and state layout.
- [Hook System Guide](hooks/README.md): current hook architecture and protocol details.
