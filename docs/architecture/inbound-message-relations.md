# Inbound Message Relations

ForgeClaw currently preserves normalized inbound messages through the durable
ingress spool, but it still leaves too much conversational boundary inference
to prompt-building heuristics.

This document records the target architecture for handling adjacent follow-up
messages, media-only turns, replies, albums, and other inbound continuations in
a way that keeps message boundaries explicit while still supporting good chat
UX.

## Problem

Two different concerns are getting mixed together:

1. preserving the real inbound event boundary
2. deciding whether a later event should be interpreted as related to an
   earlier user turn

If the runtime physically merges inbound events too early, it becomes harder
to:

- preserve attachment provenance
- explain why a turn was interpreted a certain way
- replay/debug transcripts deterministically
- extract memory or facts correctly
- support stricter agents that should not infer adjacency

If the runtime does nothing and leaves the decision entirely to prompt prose,
human chat UX suffers:

- text, then photo, then voice often belongs to one thought
- users do not always press Reply
- platforms emit albums and bursty messages in different shapes

## Design Goal

Use a strict event model and a flexible relation model.

- Every inbound platform event remains a distinct normalized message.
- The runtime computes explicit typed relations between adjacent messages.
- Downstream prompt assembly, queueing, memory extraction, and agent policy use
  those typed relations instead of re-guessing from raw history.

This keeps OpenClaw-style boundary discipline while recovering the practical
follow-up UX that systems like Hermes try to provide through pending-message
merging.

## Non-Goals

The relation layer should not depend on semantic text interpretation.

In particular, the classifier should not:

- use an LLM
- compare text meaning or topic similarity
- look for continuation by matching specific words or phrases
- guess relation from vague linguistic cues like "also", "here", "this", or
  "and then"

That kind of inference is too brittle for a core boundary layer and would make
the runtime harder to reason about, test, and audit.

## Target Model

Normalized inbound events should gain a relation descriptor that answers:

- is this event standalone?
- is it a direct reply?
- is it part of a burst or album?
- is it likely a media follow-up to the previous user turn?
- should it be grouped into the same conversational ask for this agent?

Suggested relation kinds:

- `standalone`
- `reply_to_message`
- `album_member`
- `same_burst_text`
- `adjacent_followup`
- `adjacent_followup_media`
- `clarification_answer`

The first version does not need every kind above, but the model should make
room for them rather than hard-coding only one heuristic.

## Invariants

### 1. Do not erase raw event boundaries

The runtime may group related events for interpretation, but it should not
pretend they were originally one message.

Keep:

- original message ids
- timestamps
- sender identity
- media paths / attachment lists
- explicit reply ids

### 2. Explicit reply always wins

If a platform says the inbound message is a reply, that relation is stronger
than any adjacency heuristic.

### 3. Platform-native groupings are first-class

Telegram media groups/albums and similar native constructs should map to an
explicit grouping relation rather than an incidental merge hidden inside one
adapter.

### 4. Adjacency inference must be bounded

Adjacency is useful, but only under controlled conditions.

For example, `adjacent_followup_media` may require:

- same session key
- same sender
- within a bounded time window
- no assistant reply after the prior user turn
- no explicit conflicting reply target

### 5. Relation computation belongs before prompt assembly

The prompt builder should consume relation metadata. It should not be the
primary place where these boundaries are inferred.

## Recommended Runtime Shape

### Stage 1: classify at ingress/runtime boundary

Add a small relation classifier after normalized inbound message construction
and before prompt assembly.

Inputs:

- current inbound message
- recent unambiguous session-local message metadata
- active-run state, when relevant
- platform-native reply/group ids

Outputs:

- relation kind
- optional anchor message id
- optional grouped message ids
- optional confidence / reason code for diagnostics

The classifier should be:

- structural
- deterministic
- non-LLM
- cheap to run
- explainable in tests

It should rely on runtime facts such as:

- explicit reply id
- sender identity
- session identity
- timestamp distance
- presence of assistant reply after the prior user turn
- presence of media
- platform-native album/media-group ids

It should not rely on text similarity, keyword continuation rules, or semantic
judgment about what the user "probably meant".

### Stage 2: preserve relation metadata through routing

Carry relation metadata through:

- durable ingress replay
- session claiming
- steering / follow-up queueing
- prompt build request
- memory / artifact extraction surfaces where useful

### Stage 3: assemble the effective user ask from typed relations

Prompt assembly may then apply policy like:

- `reply_to_message`: include quoted target context
- `album_member`: assemble one visual set
- `adjacent_followup_media`: treat current media as context for the previous
  user ask
- `standalone`: do not inherit prior ask context

This is where agent-specific policy can differ without changing the inbound
truth model.

## Policy Layer

Not every agent should interpret adjacency the same way.

Examples:

- personal assistant: permissive adjacency
- family archive: moderate adjacency
- code reviewer: strict, almost no inferred follow-ups
- ops/release bot: strict unless explicit reply

So relation classification should be core runtime behavior, but relation usage
should be policy-driven.

Important distinction:

- classification remains structural and deterministic
- policy decides how much weight to give a classified relation

That keeps the core runtime truthful while still letting agents differ in how
strictly they interpret adjacent follow-ups.

Suggested configuration direction:

- per-agent or per-workspace relation policy
- bounded adjacency window
- enable/disable inferred media follow-ups
- enable/disable same-burst text grouping

## Rollout Plan

### PR 1: document and isolate current heuristic

- land this design note
- isolate current media-follow-up heuristic behind one helper / classifier seam
- no behavior change other than code movement if needed

### PR 2: introduce typed relation metadata

- add relation fields to the normalized inbound/prompt-build path
- keep existing behavior by mapping current heuristics to the new fields
- add focused tests for relation classification

### PR 3: move prompt logic to consume relation metadata

- remove prompt-local guessing where possible
- keep prompt assembly responsible only for rendering context, not inferring it

### PR 4: normalize platform-native grouping

- lift Telegram album/photo-burst semantics into explicit relation metadata
- avoid adapter-local hidden merge behavior where a relation record is better

### PR 5: add policy controls

- make adjacency behavior configurable by workspace/agent
- default conservatively

## Desired End State

At the end of this refactor:

- inbound event boundaries remain explicit
- reply and album semantics are first-class
- adjacent follow-up behavior is typed and explainable
- prompt assembly is simpler
- memory extraction becomes less error-prone
- different agents can adopt different adjacency policies without changing core
  runtime semantics

This should be treated as a boundary-cleanup effort, not as a broad rewrite of
session routing, outbound delivery, or the existing durable ingress model.
