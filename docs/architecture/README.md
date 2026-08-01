# Architecture

Internal architecture notes for major runtime mechanisms and subsystem design.

- [Steering](steering.md): injecting messages into a running agent loop between tool calls.
- [AgentLoop Runtime Host](agentloop-runtime.md): AgentLoop/Pipeline split, inbound scheduling, session claims, recovery, and intentional coupling.
- [Async Task Delivery](async-task-delivery.md): durable task/completion/delivery model, deliverables, and current source-of-truth boundaries.
- [SubTurn Mechanism](subturn.md): sub-agent coordination, concurrency control, and lifecycle handling.
- [Subagent Model Policy](subagent-model-policy.md): child-run model selection, inherited session override modes, and precedence.
- [Session System](session-system.md): session scope allocation, JSONL persistence, alias compatibility, and migration.
- [Seahorse Reconciliation](seahorse-reconciliation.md): canonical JSONL history, derived Seahorse state, revision watermarks, and recovery invariants.
- [Memory System](memory-system.md): memory layers, source-of-truth boundaries, prompt budgets, mutation semantics, privacy policy, and evaluation contract.
- [Session Goals](session-goals.md): durable per-conversation objectives, command and tool interfaces, prompt injection, and reset semantics.
- [Routing System](routing-system.md): agent dispatch, session policy selection, and light/heavy model routing.
- [Durable Ingress](durable-ingress.md): normalized inbound message spool and restart replay semantics.
- [Safe Restart And Deploy](safe-restart-and-deploy.md): bounded restart/deploy handoff, shared binary targets, and durability boundaries.
- [Node Companion](node-companion.md): outbound paired capability hosts, transport and identity boundaries, remote execution policy, and the Linux/macOS MVP.
- [Node Companion Post-MVP Roadmap](node-companion-roadmap.md): ordered future milestones for explicit owner/root shell and PTY access, administrator file transfer, service management, fleet operations, executors, transports, and additional capabilities.
- [Node Companion P0 Capability Contracts](node-companion-p0-contracts.md): admitted scope, bounded discovery schema, effective-policy projection, freshness, redaction, and completion gates for model-visible node capabilities.
- [Node Companion P1 Owner-Control Admission](node-companion-p1-admission.md): admitted owner shell, cancellation, Linux root broker, and interactive terminal contracts with disabled production defaults and exact completion gates.
- [Node Companion P2 File Transfer Admission](node-companion-p2-admission.md): admitted regular-file transfer, gateway spool, path safety, Linux administrator helper, approval, replay, deployment, and mandatory completion gates.
- [Inbound Message Relations](inbound-message-relations.md): explicit relation typing for replies, adjacent follow-ups, media-only turns, and platform-native grouping.
- [Runtime Events](runtime-events.md): runtime event envelope, centralized event logging, filters, and examples.
- [Channel Lifecycle](channel-lifecycle.md): conservative channel reload policy, delivery ownership invariants, and the roadmap for any future hot-replacement work.
- [Workspace Temp Directory](workspace-temp.md): standard scratch path, `MINTCLAW_WORKSPACE_TMP`, and where temporary files should go.
- [Media Store Durability](media-store.md): workspace-local media reference recovery, retention semantics, and migration limits.
- [Shellguard](shellguard.md): reusable shell command validation, command classification, permission modes, and path-scope limits.
- [Tool-Loop Stagnation Protection](tool-loop-stagnation.md): warning-first repeated failure and read-only no-progress detection with hash-safe state and events.
- [Passive Diagnostics](passive-diagnostics.md): bounded redacted execution traces for direct human and Codex debugging without runtime coupling.
- [Current Refactoring Audit](current-refactoring-audit.md): near-term architecture risks around metadata, delivery, turn state, context manager migration, and provider contracts.
- [Reliability and Refactoring Roadmap](reliability-refactoring-roadmap.md): prioritized durability, security, ownership, provider-contract, and cross-platform verification work with explicit completion criteria.
- [Hook System Guide](hooks/README.md): current hook architecture and protocol details.
