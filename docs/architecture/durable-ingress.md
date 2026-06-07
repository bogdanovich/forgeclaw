# Durable Ingress

ForgeClaw protects inbound chat messages with a normalized durable ingress spool.

The spool sits behind `bus.MessageBus.PublishInbound`, after a channel adapter has
converted a platform-specific update into a `bus.InboundMessage`. This makes the
first layer channel-agnostic: Telegram, Slack, Discord, MQTT, webhooks, and other
channels all use the same normalized persistence path once they publish into the
bus.

## Flow

1. A channel receives a platform update.
2. The channel normalizes it into `bus.InboundMessage`.
3. `MessageBus.PublishInbound` writes the message to disk under:

   ```
   <workspace>/state/ingress-spool/inbound/
   ```

4. The bus publishes the message to the in-memory inbound channel.
5. The agent loop processes or accepts the message.
6. The agent loop calls `AckInbound`, which removes the spool entry.

On gateway startup, `ReplayInboundSpool` republishes unacked pending or processing
entries into the bus before normal channel traffic continues. This covers the
common failure case where the process exits after a normalized inbound message
has entered ForgeClaw but before the agent has processed it.

## File States

- `*.json`: pending entry, written but not yet delivered to the in-memory bus.
- `*.processing`: entry delivered to the in-memory bus and awaiting ack.
- `*.failed`: terminal tombstone for entries intentionally failed by runtime
  code.

Current gateway wiring enables the spool automatically for `picoclaw gateway`.
One-shot CLI helpers and tests use a plain in-memory bus unless they explicitly
attach an `InboundSpool`.

## Guarantee Boundary

This layer stores normalized `bus.InboundMessage` values. It does not store raw
platform payloads before channel normalization.

That means it protects all channels uniformly after they publish into ForgeClaw,
but it does not yet provide OpenClaw-style raw transport offset protection for
Telegram long polling before `PublishInbound` is called. A deeper transport-level
spool can be added later for channels whose upstream protocol needs exactly that
offset/watermark behavior.

## Steering Caveat

If a message arrives while a turn for the same session is already active, the
agent accepts it into the in-memory steering queue but keeps the ingress spool
entry until the queued continuation is injected into the running turn context
and persisted into session history. If the process exits before that injection
happens, gateway startup replays the original inbound message from the spool.

The steering queue itself is still not a separately durable queue. Once a
continuation has been injected into the running turn, the ingress spool entry is
acked and removed because the message has crossed into the session/history
durability boundary. A future durable steering queue could close any remaining
gap between in-memory turn state and deeper execution checkpoints.
