#!/bin/sh
set -e

mintclaw_home=${MINTCLAW_HOME:-"${HOME}/.mintclaw"}
mintclaw_config=${MINTCLAW_CONFIG:-"${mintclaw_home}/config.json"}
mintclaw_workspace="${mintclaw_home}/workspace"

# First-run: neither config nor workspace exists.
# If config.json is already mounted but workspace is missing we skip onboard to
# avoid the interactive "Overwrite? (y/n)" prompt hanging in a non-TTY container.
if [ ! -d "${mintclaw_workspace}" ] && [ ! -f "${mintclaw_config}" ]; then
    mintclaw onboard
    echo ""
    echo "First-run setup complete."
    echo "Edit ${mintclaw_config} (add your API key, etc.) then restart the container."
    exit 0
fi

# Remove stale PID file from a previous container run.
# After docker kill / OOM / crash the PID file may linger on the bind-mounted
# volume and block the next gateway start (the recorded PID could collide with
# an unrelated process inside the new container).
rm -f "${mintclaw_home}/.mintclaw.pid"

exec mintclaw gateway "$@"
