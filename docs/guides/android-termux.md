# Android Termux Guide

> Back to [Guides](README.md)

This guide covers running the MintClaw terminal binary on an ARM64 Android
phone with Termux.

## Requirements

- ARM64 Android device. Run `uname -m` in Termux and use this guide when it prints `aarch64`.
- Termux installed from [Termux GitHub Releases](https://github.com/termux/termux-app/releases) or F-Droid.
- Network access for downloading the release and calling your LLM provider.
- An API key for at least one configured model provider.

## Install MintClaw

Open Termux and install the packages used by the release archive and chroot wrapper:

```bash
pkg update
pkg install -y wget tar proot
```

Download and unpack the ARM64 Linux release:

```bash
mkdir -p ~/mintclaw
cd ~/mintclaw
wget https://github.com/bogdanovich/mintclaw/releases/latest/download/mintclaw_Linux_arm64.tar.gz
tar xzf mintclaw_Linux_arm64.tar.gz
chmod +x ./mintclaw
```

Start first-run setup through `termux-chroot`, which gives the Linux binary a more standard filesystem layout than a raw Android userspace:

```bash
termux-chroot ./mintclaw onboard
```

## Configure

Edit the generated config and add at least one model provider API key:

```bash
vim ~/.mintclaw/config.json
```

The default workspace is `~/.mintclaw/workspace`. If you want MintClaw to read or write Android shared storage, run `termux-setup-storage` first and then point the workspace or any file paths at the mounted storage directory.

See [Configuration Guide](configuration.md) and [Providers & Model Configuration](providers.md) for the available config fields and provider examples.

## Run

Use one-shot agent mode to confirm the installation:

```bash
termux-chroot ./mintclaw agent -m "Hello from Termux"
```

For long-running use, start the gateway:

```bash
termux-chroot ./mintclaw gateway
```

Keep the Termux session open while MintClaw is running. Android battery optimization can stop background work, so disable battery optimization for Termux if you expect MintClaw to keep running after the screen locks.

## Update

Your config and workspace live under `~/.mintclaw`, so updating the binary does not remove them:

```bash
cd ~/mintclaw
rm -f mintclaw_Linux_arm64.tar.gz
wget https://github.com/bogdanovich/mintclaw/releases/latest/download/mintclaw_Linux_arm64.tar.gz
tar xzf mintclaw_Linux_arm64.tar.gz
chmod +x ./mintclaw
termux-chroot ./mintclaw version
```

## Troubleshooting

| Symptom | Check |
|---------|-------|
| `permission denied` | Run `chmod +x ./mintclaw` after unpacking the archive. |
| `not found` after running `./mintclaw` | Confirm `uname -m` prints `aarch64` and that you downloaded `mintclaw_Linux_arm64.tar.gz`. |
| Files or paths behave differently than Linux | Run MintClaw through `termux-chroot` instead of calling the binary directly. |
| Provider requests fail | Check the API key and network access in `~/.mintclaw/config.json`. |
| MintClaw stops when the phone sleeps | Disable Android battery optimization for Termux and keep a foreground Termux session active. |
