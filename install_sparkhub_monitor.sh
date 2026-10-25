#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE="spark-hub-web.service"
UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
PORT="${1:-8090}"
[[ -f "$DIR/spark_hub_server.py" && -f "$DIR/index.html" && -f "$DIR/spark_hub_fetch.py" ]] || { echo "Run this from the extracted Spark Hub project folder; required files are missing." >&2; exit 1; }
mkdir -p "$UNIT_DIR" "${XDG_STATE_HOME:-$HOME/.local/state}/spark-hub"
if [[ -f "$UNIT_DIR/$SERVICE" ]]; then cp -a "$UNIT_DIR/$SERVICE" "$UNIT_DIR/$SERVICE.backup.$(date +%Y%m%d%H%M%S)"; fi
cat > "$UNIT_DIR/$SERVICE" <<EOF
[Unit]
Description=Spark Hub dashboard and refresh API
After=network-online.target

[Service]
WorkingDirectory=$DIR
ExecStart=$(command -v python3) "$DIR/spark_hub_server.py" --port $PORT --bind 0.0.0.0
Restart=on-failure
RestartSec=3

[Install]
WantedBy=default.target
EOF
chmod +x "$DIR/setup.sh" "$DIR/spark_hub_fetch.py" "$DIR/spark_hub_server.py"
python3 -m py_compile "$DIR/spark_hub_fetch.py" "$DIR/spark_hub_server.py"
systemctl --user daemon-reload
systemctl --user enable --now "$SERVICE"
systemctl --user restart "$SERVICE"
echo "Spark Hub monitor server installed on port $PORT."
echo "Open http://localhost:$PORT/#/refresh or use your T480 LAN address."
