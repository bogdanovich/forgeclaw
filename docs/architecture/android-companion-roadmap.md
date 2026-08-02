# Android Companion Roadmap

## Status

Proposed product and architecture roadmap. This document defines ordering,
boundaries, and completion evidence. It does not admit every milestone for
implementation.

The Android companion is distinct from running the MintClaw Linux binary in
Termux. Termux provides a phone-local gateway and agent runtime. The companion
is a native Android application connected to a remote MintClaw gateway.

This roadmap extends the existing
[`node-companion.md`](node-companion.md) architecture. It does not replace the
node protocol, durable interaction system, session store, media store, or
gateway AgentLoop.

## Decision Summary

- Build one native Kotlin application with Jetpack Compose.
- Give the application two separately authorized roles: an operator client and
  a device-capability node.
- Maintain independent authenticated logical connections for those roles.
- Keep chat, session history, approvals, and questions on an operator plane.
- Reuse the node companion architecture for camera, location, notifications,
  SMS, contacts, calendar, and sensors.
- Do not turn chat messages into node invocations or device results into
  channel messages.
- Pair with a short-lived QR enrollment flow and keep long-lived private keys
  in Android Keystore.
- Treat Android permissions and current app state as part of the node's live
  capability catalog.
- Ship a normal store-compatible build without broad SMS or call-log access.
- Offer full SMS capability only in an explicitly installed standalone build,
  unless store policy later provides a valid exception.
- Require durable identity and idempotency for message submission and every
  mutating device invocation.
- Do not promise an always-online node when Android has suspended the process.

## Product Outcome

The first useful release lets an operator securely pair a phone with a remote
gateway, browse authorized conversations, send text, images, and voice notes,
receive streamed answers, and answer durable questions and approvals. Messages
survive network changes, app restarts, and temporary gateway unavailability.

The first device-node beta additionally lets an authorized agent use selected
phone capabilities. A request may originate from the Android chat, Telegram,
or any other authorized MintClaw session. The phone is a named target; it is
not implicitly bound to the chat displayed in the Android application.

A later personal-use release supports this flow:

1. The operator asks an agent to find a recent message from a landlord.
2. The agent searches an explicitly authorized phone target.
3. The agent drafts a response using the returned message and thread context.
4. The phone displays the exact recipient and text in a native approval card.
5. After approval, the node sends once and reports a durable terminal or
   unknown outcome.

Incoming messages are untrusted data. Their text can inform a draft, but cannot
authorize sending, broaden node policy, or approve another action.

## Goals

1. Provide a first-party mobile chat experience for a remote MintClaw gateway.
2. Make the Android device a typed, least-privilege capability host.
3. Preserve message and invocation intent across reconnects and process death.
4. Use the existing MintClaw session, interaction, node, media, and audit
   sources of truth rather than duplicating them in the app.
5. Make permission, availability, approval, and uncertain outcomes visible to
   both the operator and the model.
6. Support a narrow store-compatible build and a more capable personal
   standalone build without pretending their authority is equal.
7. Keep the protocol suitable for later iOS, desktop, and web operator clients
   without weakening Android-specific enforcement.

## Non-Goals

- Running an LLM, AgentLoop, workspace, or provider credentials in the app.
- Replacing the user's default SMS application.
- General accessibility-driven remote control of arbitrary applications.
- Silent background camera or microphone access.
- Treating a persistent foreground service as permission to bypass Android
  privacy or lifecycle restrictions.
- Full device backup, filesystem mirroring, or unrestricted local shell.
- End-to-end encrypted sessions in the first release. TLS, authenticated
  clients, and protected gateway storage remain required.
- Protocol compatibility with OpenClaw or another companion implementation.
- Shipping Canvas, Wear OS, multi-gateway orchestration, or device
  administration in the MVP.

## Existing Foundation And Gaps

MintClaw already provides useful foundations:

- paired outbound node WebSockets and a typed capability catalog;
- operator-owned target aliases and per-agent target policy;
- model-visible capability contracts and catalog freshness checks;
- durable node invocation identity, recovery, cancellation, and explicit
  unknown outcomes;
- file and media transfer contracts;
- durable inbound chat spooling;
- canonical session history and session routing;
- durable human questions and approvals;
- the current `mintclaw` WebSocket channel for live messages, tool feedback,
  streaming updates, and inline images.

The current `mintclaw` channel is not yet a mobile control plane. It uses one
static bearer token, accepts a client-selected session ID, and primarily
broadcasts live events. It lacks a scoped mobile identity, a canonical session
catalog, history synchronization, replay cursors, durable client-operation
idempotency, background notification delivery, and a complete interaction API.

The correct next step is not to keep adding mobile exceptions to that channel.
Extract or reuse its presentation event mapping behind an authenticated
operator protocol with explicit domain contracts. Existing browser or local
clients may later migrate to the same operator protocol.

## Target Architecture

```text
Android application
|
+-- operator plane
|   +-- account and gateway enrollment
|   +-- session catalog and history synchronization
|   +-- text, media, and voice-note outbox
|   +-- live response and tool-feedback rendering
|   +-- durable question and approval UI
|
+-- node plane
    +-- paired device identity and live capability catalog
    +-- invocation ledger and recovery
    +-- Android permission and lifecycle enforcement
    +-- camera, location, notifications, SMS, and other handlers
            |
            v
remote MintClaw gateway
    +-- operator API and authenticated operator sessions
    +-- canonical session, media, interaction, and ingress services
    +-- existing node registry, policy, invocation, and audit services
    +-- AgentLoop and model providers
```

One enrollment ceremony may provision both roles, but the gateway issues
separate scoped credentials and maintains separate authorization records. A
compromised operator credential must not automatically grant device execution,
and a paired node credential must not grant access to chat history.

Each role has its own connection state, reconnect policy, and revocation path.
The first release should use separate WebSocket connections. A later transport
may multiplex them only if it preserves independent stream authentication and
cannot accidentally promote one role into the other.

### Operator Plane

The operator plane is a first-party control API, not another ordinary external
chat adapter. It should expose bounded operations for:

- listing only the workspaces, agents, and sessions visible to the operator;
- reading canonical history through snapshots and monotonic cursors;
- submitting a message or media item with a durable client operation ID;
- receiving accepted, started, streamed, finalized, failed, and superseded
  states;
- listing and answering authorized durable interactions;
- receiving a bounded projection of runtime and tool-feedback events;
- managing the current installation, connection, and notification settings.

The gateway allocates or authorizes canonical sessions. The client does not
gain access by inventing a session ID. Mobile authorization is evaluated
before history, media, or live events are returned.

The wire transport may use one versioned WebSocket plus bounded HTTP upload and
recovery endpoints. Domain services must not depend on Android or WebSocket
frame types. A path such as `/operator/v1/ws` is preferable to an
Android-specific protocol because the same contracts can later support iOS,
desktop, and web clients.

### Node Plane

The node plane reuses MintClaw's existing target, pairing, catalog, policy,
invocation, recovery, and audit model. Android provides a native transport and
capability runtime, not a second node architecture.

Each command remains typed and versioned. A handler advertises a command only
when all of these are true:

- the build variant contains the implementation;
- the operator enabled the capability;
- required Android permission or special access is currently granted;
- current lifecycle state permits execution;
- node-local policy permits the command;
- the handler can report its bounds and outcome semantics accurately.

Permission revocation, loss of notification access, background restrictions,
or an app update changes the catalog revision. The gateway must fail stale
preparation before dispatch rather than discovering the change through an
unsafe invocation.

### Shared Application, Separate State

The app may share networking, serialization, storage encryption, diagnostics,
and UI infrastructure. It must not share authority implicitly.

Recommended local stores are:

- an encrypted operator credential and synchronization cursor store;
- a durable outbound message outbox keyed by client operation ID;
- a node credential and approved catalog store;
- a bounded node invocation ledger for accepted mutating work;
- an encrypted cache of bounded history and media metadata;
- redacted diagnostics with explicit export and retention controls.

Clearing operator chat data need not unpair the node. Revoking one role must
not silently revoke or preserve the other; the UI shows both states and lets
the operator revoke each explicitly.

## Identity, Pairing, And Transport

Enrollment should use a short-lived, single-use QR payload containing only the
gateway endpoint, endpoint trust material, an opaque enrollment secret, and
requested role scopes. It must not contain a reusable gateway bearer token.

The app creates non-exportable private keys in Android Keystore and proves
possession during enrollment. Before Android node implementation, the node
identity envelope must become algorithm-agile. Hardware-backed P-256 is the
preferred Android default; Ed25519 is acceptable only on platform versions and
providers where secure key generation and signing are verified. The gateway
stores the algorithm with the public key and never infers it from key length.

Transport requirements:

- TLS is mandatory outside explicit loopback development;
- endpoint trust is verified through the platform store or QR-provisioned pin;
- every connection authenticates one scoped installation role;
- protocol versions and minimum supported app versions are negotiated;
- credentials rotate without changing the operator-visible device alias;
- revocation is effective for new and reconnected sessions;
- logs never contain enrollment secrets, credentials, message bodies, SMS
  bodies, contact values, or raw media.

Biometric confirmation may protect especially sensitive local approvals, but
biometrics do not replace gateway authorization or node-local policy.

## Message And Interaction Reliability

### Client Outbox

Every outbound chat mutation uses a random client operation ID generated before
the first network attempt. The app persists the operation and payload reference
before sending. The gateway durably records acceptance and maps repeated use of
the same ID by the same operator installation to the same canonical message.

The client state machine is explicit:

```text
created -> queued -> sending -> accepted -> completed
                     |             |
                     +-> retryable  +-> failed or cancelled
                     +-> unknown
```

A connection loss before durable acceptance may retry the same operation ID.
A connection loss after acceptance recovers status; it does not submit another
message. Media upload uses a staged object ID, declared size, content type, and
checksum, and the message references that accepted object.

### History And Live Events

Canonical gateway history remains authoritative. The app maintains a bounded
cache and an opaque synchronization cursor. Reconnect first closes any cursor
gap, then resumes live events. Duplicate event delivery is harmless and event
ordering is explicit per session.

Streaming text and tool feedback are projections, not durable history writes
performed by the phone. A final message replaces or completes its temporary
stream projection. Process death during streaming recovers from canonical
history and status rather than preserving a dangling typing indicator.

### Questions And Approvals

The app renders MintClaw's existing durable interaction records. Answers carry
the interaction ID and expected revision, and use the existing exactly-once
answer claim. The app never synthesizes approval from a generic chat message,
notification tap, or device unlock.

Sensitive device actions should use a native approval card that shows bounded,
policy-owned details. The model cannot choose which details are hidden. For an
SMS send, the card includes the resolved recipient, destination, exact text,
subscription when relevant, and whether delivery confirmation is available.

## Android Lifecycle Model

Android does not guarantee an indefinitely connected background process. The
product must expose this honestly:

- operator chat is fully interactive while the app is visible;
- a durable outbox and synchronization cursor recover after process death;
- push notifications may wake the operator for completed turns or pending
  interactions without pretending the WebSocket stayed alive;
- deferrable maintenance uses WorkManager;
- an optional user-enabled foreground service may keep remote node access
  available, with a persistent notification and declared service types;
- camera, microphone, and other while-in-use capabilities report
  `foreground_required` when Android cannot legally or safely execute them;
- node presence distinguishes `connected`, `temporarily_unavailable`, and
  `permission_required` from a generic failure.

The app must not repeatedly restart a background service, abuse high-priority
push, or ask users to disable all battery protections as its primary
reliability design.

## Capability And Distribution Policy

Build variants expose different maximum authority. Runtime permission cannot
add code or manifest permissions absent from a variant.

| Capability | Store-compatible build | Standalone personal build |
| --- | --- | --- |
| Operator chat, history, media, interactions | Yes | Yes |
| Camera and microphone while user-visible | Yes | Yes |
| User-selected photos and files | Yes | Yes |
| Current location with explicit permission | Yes | Yes |
| Notification observation and action | Optional special access | Optional special access |
| Notification inline reply | When the source exposes a reply action | Same |
| Contacts and calendar | Optional runtime permission | Optional runtime permission |
| SMS database search and thread history | No | Explicit opt-in |
| Direct SMS send | No | Explicit opt-in and approval |
| Call logs | No initial support | Separate future admission |
| Accessibility or device administration | No | No initial support |

The application requests a permission only when the operator enables or invokes
the related capability. Denial leaves the rest of the app functional.

## SMS And Messaging Design

SMS is delivered in two independent slices.

### Notification-Based Reply

The first slice uses Android notification access. The node can inspect a
bounded active messaging notification and invoke its existing inline reply
action when one is present. This avoids `READ_SMS` and `SEND_SMS`, works with
some SMS and internet-messaging applications, and fits the store-compatible
build.

Its limitations must be visible:

- only active notification content is available;
- text may be truncated or grouped;
- historical search is unavailable;
- reply support depends on the source application;
- successful dispatch to a notification action is not carrier delivery;
- application updates may alter the available action.

The command accepts an opaque notification and action reference produced by a
fresh node query. It does not accept a model-invented package, pending intent,
or phone number.

### Full SMS Capability

The standalone personal build may later advertise:

- `sms.search.v1` for bounded sender, time, direction, and text filters;
- `sms.thread.v1` for a bounded page of one resolved conversation;
- `sms.send.v1` for one exact text message to an opaque resolved thread or
  destination reference;
- `sms.status.v1` when the platform can recover a previously accepted send.

Search results use opaque message and thread references. A model cannot replace
the resolved destination by supplying a different phone number at send time.
Raw SMS content is redacted from logs and audit metadata, while the model sees
only content explicitly returned under the authorized invocation.

Direct send is mutating and defaults to one native approval per message. After
the Android API accepts a send, disconnect or process death must not cause an
automatic retry. The invocation ledger reports terminal status when known and
`unknown` otherwise. MMS, attachments, group MMS, and RCS remain unsupported
until separately admitted; notification reply may still work when the source
application exposes it.

Google Play restricts broad SMS and call-log permissions to default handlers
or approved exception cases. MintClaw should not become the default SMS app
merely to obtain permissions. The standalone variant therefore has separate
distribution, signing, privacy disclosure, and update requirements.

## Capability Safety Classes

Each Android command admission records:

- required runtime permission or special access;
- foreground, foreground-service, or background eligibility;
- data sensitivity and model exposure;
- read-only, reversible, or externally mutating behavior;
- approval mode and exact approval projection;
- acceptance, retry, cancellation, and unknown-outcome boundary;
- output, frequency, retention, and concurrency limits;
- whether the result can contain untrusted instructions.

Suggested defaults are:

| Class | Examples | Default |
| --- | --- | --- |
| Local status | battery, permissions, app version | Allow for paired target |
| Sensitive read | notifications, contacts, calendar, SMS | Explicit capability policy; bounded output |
| User-visible capture | camera, microphone, current location | Foreground or visible consent |
| External mutation | notification reply, SMS send, calendar write | Exact native approval |
| High authority | accessibility, device admin, broad filesystem | Not admitted |

No chat prompt, model argument, notification body, SMS body, or contact name can
change these defaults.

## Delivery Roadmap

Milestones are ordered by dependency and operator value, not calendar dates.
Each milestone requires an admission document before implementation.

| Milestone | Outcome | Depends on |
| --- | --- | --- |
| A0 | Admit operator and Android security contracts | This roadmap |
| A1 | Gateway operator API and durable client identity | A0 |
| A2 | Native chat MVP | A1 |
| A3 | Durable media, interactions, and background notifications | A2 |
| A4 | Android node foundation | A0 and stable node protocol |
| A5 | Camera, selected media, and current location | A4 and file artifacts |
| A6 | Notification observation and inline reply | A4 and native approvals |
| A7 | Standalone full SMS capability | A6 evidence and separate admission |
| A8 | Contacts, calendar, device status, and motion | A4 and per-capability admission |
| A9 | Voice conversation and Talk mode | A3 and audio lifecycle evidence |
| A10 | Canvas and bounded visual interaction | A3 and separate rendering threat model |
| A11 | Distribution, updates, diagnostics, and production operations | A3 and A6 |
| A12 | Multi-gateway, Wear OS, and additional platforms | Stable production app |

A1 through A3 form the first useful operator release. A4 through A6 form the
first useful device-node beta. A7 is the first full personal texting release.
The operator and node tracks may progress in parallel after A0, but no app
release may blur their credentials or authority.

### A0: Contract Admission

Define and approve:

- operator identity, role scopes, revocation, and enrollment;
- versioned operator domain operations and event schemas;
- history cursor and client-operation idempotency semantics;
- Android key algorithm and Keystore support matrix;
- node catalog projection of permission and lifecycle state;
- store-compatible and standalone product boundaries;
- threat model for a lost phone, compromised gateway, malicious message,
  replayed operation, and stolen enrollment QR.

Completion requires protocol examples, state machines, redaction rules, test
vectors, and an explicit list of excluded behavior. Do not start with a generic
mobile framework or UI shell that has no admitted end-to-end flow.

### A1: Gateway Operator API

Build transport-neutral services for scoped installation identity, session
catalog, bounded history synchronization, durable message admission, live event
projection, interaction access, and revocation. Adapt existing `mintclaw`
presentation events where their semantics match; do not expose its static
bearer and arbitrary session selection as the new protocol.

Completion evidence includes:

- an unauthorized installation cannot enumerate sessions or media;
- one client operation produces one canonical inbound message across retries;
- reconnect closes a history and event cursor gap without duplicates;
- role revocation terminates or rejects the next operation;
- bounded real-process tests cover process restart during admission and stream.

### A2: Native Chat MVP

Create a native Kotlin and Compose application with QR enrollment, gateway and
session selection, history, live streaming, text submission, connection state,
and a durable local outbox. Store private keys in Keystore and bounded local
state in an encrypted database.

Completion requires airplane-mode, process-death, token-revocation, gateway
restart, duplicate-send, and large-history tests on supported Android versions.

### A3: Durable Mobile Experience

Add staged image and voice-note uploads, media rendering, durable question and
approval cards, push notifications, final-response recovery, and bounded tool
feedback. A notification opens the exact authorized session or interaction but
does not itself approve an action.

This milestone produces the first useful operator release.

### A4: Android Node Foundation

Implement the existing node protocol natively with scoped pairing, catalog
revision, reconnect, liveness, invocation ledger, status recovery,
cancellation, policy, and redacted audit. Start with device information and
permission-status commands that have no sensitive side effects.

Completion requires a real Android device test proving accepted invocation
recovery across app process death and no capability advertisement after
permission or policy revocation.

### A5: Media And Location

Add user-visible camera capture, user-selected photo and file access, bounded
audio capture where justified, and one-shot current location. Results use the
existing node artifact and media contracts. Background capture is excluded.

### A6: Notifications

Add bounded notification discovery, freshness-scoped opaque references,
dismiss or action semantics where admitted, and inline reply. Notification
text is untrusted and redacted from logs. Mutating actions use native approval
unless an operator configures a narrower exact policy.

This milestone produces the first useful device-node beta.

### A7: Full SMS

Ship only in the separately signed standalone variant after a dedicated
privacy and security admission. Add bounded search, thread retrieval, exact
approved sending, durable status, SIM selection, and clear unsupported states
for MMS and RCS.

Completion requires real-device tests with process death before and after the
send acceptance boundary, duplicate command delivery, permission revocation,
multi-SIM selection, and malicious message content.

### A8: Organizer And Sensors

Admit contacts, calendar, device status, call-state metadata when permitted,
and motion independently. Prefer platform pickers and narrow lookups over bulk
database export. Calendar writes require exact event projection and approval.

### A9: Voice And Talk

Add push-to-talk first, then an explicitly active conversation mode with
foreground audio indication, interruption, headset routing, and bounded audio
retention. Do not keep a hidden always-listening microphone.

### A10: Visual Interaction

Evaluate Canvas or another gateway-rendered visual surface independently from
device control. Arbitrary remote HTML must not inherit native bridge authority.
Screen capture, accessibility, and input injection are separate high-risk
capabilities and remain excluded until specifically admitted.

### A11: Distribution And Operations

Provide reproducible builds, signed store and standalone artifacts, staged
updates, minimum-version enforcement, rollback, privacy disclosures, crash and
battery diagnostics, protocol conformance tests, and operator-visible role and
permission health.

Standalone updates must verify a signing identity and artifact digest before
installation. The node cannot update itself because a model supplied a URL.

### A12: Expansion

Consider multi-gateway accounts, Wear OS approvals and voice capture, tablets,
iOS, and desktop clients only after the operator and node contracts are stable.
Multi-gateway support must preserve separate identities, stores, notification
routing, and target aliases.

## Testing Strategy

Every implemented milestone should include:

- pure protocol and state-machine tests shared through language-neutral
  fixtures;
- Gateway Go tests for authorization, idempotency, cursor recovery, limits,
  redaction, and restart behavior;
- Android unit tests for stores, reducers, policy, and invocation state;
- instrumented tests for Keystore, permissions, process death, and lifecycle;
- MockWebServer or equivalent transport fault tests;
- real-process gateway-to-emulator tests in CI where stable;
- real-device evidence for notifications, SMS, camera, background behavior,
  multi-SIM, and OEM-specific restrictions before declaring those complete;
- security tests for replay, stale opaque references, cross-session access,
  cross-role credential use, malicious content, and revoked authority.

Tests must verify absence of duplicate external effects, not only successful
responses.

## Observability And Privacy

Diagnostics should expose operation IDs, state transitions, latency, reconnect
reason, protocol version, app version, capability revision, permission state,
and bounded error classifications. They must exclude message and SMS bodies,
phone numbers, contact values, media bytes, credentials, enrollment material,
and raw approval payloads.

The app offers an explicit redacted diagnostic export. Telemetry is opt-in and
non-authoritative. It cannot acknowledge messages, advance cursors, retry
invocations, or become the source of truth for connection or send state.

## Admission Checklist

Before implementing a milestone:

- identify one concrete operator flow and supported Android versions;
- name the operator and node authority required by that flow;
- define typed operations, bounds, and model-visible contracts;
- document Android permissions, special access, and lifecycle restrictions;
- define persistence, acceptance, retry, cancellation, and unknown outcomes;
- define local and gateway retention and deletion behavior;
- define native approval content and who may answer it;
- list store-policy and standalone distribution implications;
- identify real-device completion evidence;
- list explicit non-goals and stop conditions.

After implementation:

- validate merged `main`, not only a feature branch;
- deploy with each sensitive capability disabled by default;
- test permission denial and revocation, offline recovery, and process death;
- inspect logs and diagnostic export for sensitive data;
- record supported devices, Android versions, and known OEM restrictions;
- admit the next milestone only when the current one has operational evidence.

## Initial PR Sequence

The expected first implementation series is deliberately narrow:

1. A0 operator protocol, identity, and threat-model admission.
2. A1 gateway operator domain interfaces and in-memory contract tests.
3. A1 durable identity, message idempotency, and history cursor stores.
4. A1 versioned transport adapter and real-process recovery tests.
5. A2 Android project skeleton, enrollment, Keystore identity, and connection.
6. A2 session list, history, text outbox, and live response UI.
7. A3 media, interactions, and push-notification slices.
8. A4 Android node admission and implementation series.

Each PR should prove one vertical invariant and remain independently
reviewable. Do not combine the gateway operator API, complete Android UI, node
runtime, and device permissions into one large PR.

## External Platform Constraints

Implementation admissions should revalidate current Android and distribution
rules rather than treating this roadmap as a permanent platform reference:

- [Android Keystore system](https://developer.android.com/privacy-and-security/keystore)
- [Background work](https://developer.android.com/develop/background-work)
- [Foreground services](https://developer.android.com/develop/background-work/services/fgs)
- [NotificationListenerService](https://developer.android.com/reference/android/service/notification/NotificationListenerService)
- [RemoteInput](https://developer.android.com/reference/android/app/RemoteInput)
- [Permissions used only in default handlers](https://developer.android.com/guide/topics/permissions/default-handlers)
