> Back to [README](../../../README.md)

# MaixCam

MaixCam is a dedicated channel for connecting to Sipeed MaixCAM and MaixCAM2 AI camera devices. It uses TCP sockets for bidirectional communication and supports edge AI deployment scenarios.

## Configuration

```json
{
  "channel_list": {
    "maixcam": {
      "enabled": true,
      "type": "maixcam",
      "host": "0.0.0.0",
      "port": 18790,
      "allow_from": ["TRUSTED_SENDER_ID"]
    }
  }
}
```

| Field      | Type   | Required | Description                                                      |
| ---------- | ------ | -------- | ---------------------------------------------------------------- |
| enabled    | bool   | Yes      | Whether to enable the MaixCam channel                            |
| host       | string | Yes      | TCP server listening address                                     |
| port       | int    | Yes      | TCP server listening port                                        |
| allow_from | array  | No       | Allowlist of device IDs; empty denies all devices; use `["*"]` for public access     |

## Use Cases

The MaixCam channel enables MintClaw to act as an AI backend for edge devices:

- **Smart Surveillance**: MaixCAM sends image frames; MintClaw analyzes them using vision models
- **IoT Control**: Devices send sensor data; MintClaw coordinates responses
- **Offline AI**: Deploy MintClaw on a local network for low-latency inference
