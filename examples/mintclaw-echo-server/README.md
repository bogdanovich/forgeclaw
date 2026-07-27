# mintclaw-echo-server

Minimal MintClaw Protocol WebSocket server for testing the `mintclaw_client` channel.

## Usage

```bash
go run ./examples/mintclaw-echo-server -addr :9090 -token secret
```

### Flags

| Flag     | Default | Description                        |
|----------|---------|------------------------------------|
| `-addr`  | `:9090` | Listen address                     |
| `-token` | (none)  | Auth token; empty disables auth    |

## How it works

- Listens for WebSocket connections at `/ws`
- Authenticates via `Authorization: Bearer <token>` header or `?token=<token>` query param
- Prints received `message.send` content to stdout
- Responds to `ping` with `pong`
- Lines typed into stdin are broadcast as `message.create` to all connected clients

## Testing with mintclaw_client

1. Start the server:
   ```bash
   go run ./examples/mintclaw-echo-server -token mytoken
   ```

2. Configure `mintclaw_client` in your `config.json`:
   ```json
   {
     "channel_list": {
       "mintclaw_client": {
         "enabled": true,
         "url": "ws://localhost:9090/ws",
         "token": "mytoken",
         "session_id": "test-session"
       }
     }
   }
   ```

3. Start mintclaw — the client connects and you can exchange messages interactively via stdin/stdout.
