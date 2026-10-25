#!/usr/bin/env bash
# Spark Hub setup: first fetch, cron schedule, optional LAN web server.
#
#   cron ──> setup.sh --run [--only forum] ──> spark_hub_fetch.py ──> data.js
#                                                                     │
#   browser ──> index.html (file:// or http://spark:PORT) <───────────┘
#
# Usage:
#   ./setup.sh                    install (fetch now, cron, web server on 8090)
#   ./setup.sh --port 9000        same, different port
#   ./setup.sh --no-serve         install without web server (open index.html directly)
#   ./setup.sh --run [args]       one refresh (what cron calls); args go to the fetcher
#   ./setup.sh --status           show schedule, server and last log lines
#   ./setup.sh --uninstall        remove cron entries and web server (keeps files)

set -euo pipefail

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------
readonly APP="spark-hub"
readonly DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly FETCHER="$DIR/spark_hub_fetch.py"
readonly CONF_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/$APP"
readonly ENV_FILE="$CONF_DIR/env"                 # tokens live outside the web root
readonly STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/$APP"
readonly LOG_FILE="$STATE_DIR/fetch.log"
readonly LOCK_FILE="$STATE_DIR/fetch.lock"
readonly LOG_MAX_BYTES=2000000
readonly CRON_TAG="# $APP"
readonly CRON_FULL="17 */6 * * *"                 # full refresh every 6 hours
readonly CRON_FORUM="47 * * * *"                  # forum only, hourly
readonly SERVICE="$APP-web.service"
readonly MIN_PY_MINOR=8
DEFAULT_PORT=8090

PORT="$DEFAULT_PORT"
SERVE=1

say()  { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Refresh (called by cron)
# ---------------------------------------------------------------------------
run_fetch() {
  mkdir -p "$STATE_DIR"

  # Rotate log: keep one previous file.
  if [[ -f "$LOG_FILE" ]] && (( $(stat -c %s "$LOG_FILE") > LOG_MAX_BYTES )); then
    mv -f "$LOG_FILE" "$LOG_FILE.1"
  fi

  # Tokens are optional; load them if present.
  if [[ -f "$ENV_FILE" ]]; then
    set -a; . "$ENV_FILE"; set +a
  fi

  # One refresh at a time; skip if a run is still going.
  exec 9>"$LOCK_FILE"
  if ! flock -n 9; then
    echo "$(date '+%F %T') skip: previous run still active" >> "$LOG_FILE"
    return 0
  fi

  local rc=0
  export SPARK_HUB_PROGRESS_FILE="${SPARK_HUB_PROGRESS_FILE:-$STATE_DIR/progress.json}"
  python3 "$FETCHER" --out "$DIR" "$@" >> "$LOG_FILE" 2>&1 || rc=$?
  exec 9>&-
  return "$rc"
}

# ---------------------------------------------------------------------------
# Install steps
# ---------------------------------------------------------------------------
check_python() {
  command -v python3 >/dev/null || die "python3 not found (sudo apt install python3)"
  local minor
  minor="$(python3 -c 'import sys; print(sys.version_info[1] if sys.version_info[0] == 3 else 0)')"
  (( minor >= MIN_PY_MINOR )) || die "python3.$MIN_PY_MINOR+ required"
  command -v flock >/dev/null || die "flock not found (sudo apt install util-linux)"
  command -v crontab >/dev/null || die "crontab not found (sudo apt install cron)"
}

write_env_template() {
  mkdir -p "$CONF_DIR" "$STATE_DIR"
  if [[ -f "$ENV_FILE" ]]; then
    return
  fi
  umask 077
  cat > "$ENV_FILE" <<'EOF'
# Spark Hub tokens (all optional). Read by setup.sh --run before each refresh.
# HF_TOKEN: https://huggingface.co/settings/tokens (read scope) - higher rate limits
HF_TOKEN=
# AA_API_KEY: free key from https://artificialanalysis.ai - enables coding/reasoning ratings
AA_API_KEY=
# GITHUB_TOKEN: only needed if GitHub rate-limits you
GITHUB_TOKEN=
EOF
  say "Token file created: $ENV_FILE (edit to add HF / Artificial Analysis keys)"
}

install_cron() {
  local self="$DIR/setup.sh"
  local current
  current="$(crontab -l 2>/dev/null | grep -v "$CRON_TAG" || true)"
  {
    [[ -n "$current" ]] && printf '%s\n' "$current"
    echo "$CRON_FULL /bin/bash '$self' --run $CRON_TAG"
    echo "$CRON_FORUM /bin/bash '$self' --run --only forum $CRON_TAG"
  } | crontab -
  say "Cron installed: full refresh every 6 h, forum hourly"
}

has_user_systemd() {
  command -v systemctl >/dev/null && systemctl --user show-environment >/dev/null 2>&1
}

install_server() {
  local unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"

  if has_user_systemd; then
    mkdir -p "$unit_dir"
    cat > "$unit_dir/$SERVICE" <<EOF
[Unit]
Description=Spark Hub dashboard (static files)
After=network-online.target

[Service]
ExecStart=$(command -v python3) "$DIR/spark_hub_server.py" --port $PORT --bind 0.0.0.0
Restart=on-failure

[Install]
WantedBy=default.target
EOF
    systemctl --user daemon-reload
    systemctl --user enable --now "$SERVICE" >/dev/null
    systemctl --user restart "$SERVICE"
    say "Web server: systemd user service $SERVICE on port $PORT"
    if ! loginctl show-user "$USER" -p Linger 2>/dev/null | grep -q yes; then
      warn "To keep it running after logout/reboot without login: sudo loginctl enable-linger $USER"
    fi
    return
  fi

  # Fallback: cron @reboot + start now.
  local current
  current="$(crontab -l 2>/dev/null | grep -v "$CRON_TAG-web" || true)"
  {
    [[ -n "$current" ]] && printf '%s\n' "$current"
    echo "@reboot cd '$DIR' && python3 '$DIR/spark_hub_server.py' --port $PORT --bind 0.0.0.0 >/dev/null 2>&1 $CRON_TAG-web"
  } | crontab -
  # 9>&- : do not let the server inherit the fetch lock.
  (cd "$DIR" && nohup python3 "$DIR/spark_hub_server.py" --port "$PORT" --bind 0.0.0.0 </dev/null >/dev/null 2>&1 9>&- &)
  say "Web server: started on port $PORT (cron @reboot keeps it after restarts)"
}

uninstall() {
  crontab -l 2>/dev/null | grep -v "$CRON_TAG" | crontab - || true
  if has_user_systemd; then
    systemctl --user disable --now "$SERVICE" >/dev/null 2>&1 || true
    rm -f "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/$SERVICE"
    systemctl --user daemon-reload || true
  fi
  pkill -f "spark_hub_server.py --port $PORT" 2>/dev/null || true
  say "Removed cron entries and web server. Files in $DIR and $CONF_DIR kept."
}

status() {
  echo "Cron:"; crontab -l 2>/dev/null | grep "$CRON_TAG" || echo "  (none)"
  if has_user_systemd; then
    echo; systemctl --user --no-pager status "$SERVICE" 2>/dev/null | head -5 || true
  fi
  echo; echo "Last log lines ($LOG_FILE):"; tail -n 12 "$LOG_FILE" 2>/dev/null || echo "  (no log yet)"
}

lan_ip() {
  hostname -I 2>/dev/null | awk '{print $1}'
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  case "${1:-}" in
    --run)       shift; run_fetch "$@"; exit $? ;;
    --status)    status; exit 0 ;;
    --uninstall) uninstall; exit 0 ;;
  esac

  while (( $# )); do
    case "$1" in
      --port)     PORT="${2:?port missing}"; shift 2 ;;
      --no-serve) SERVE=0; shift ;;
      -h|--help)  sed -n '2,16p' "$0"; exit 0 ;;
      *)          die "unknown option: $1" ;;
    esac
  done

  [[ -f "$FETCHER" ]] || die "spark_hub_fetch.py must sit next to setup.sh"
  [[ -f "$DIR/index.html" ]] || die "index.html must sit next to setup.sh"
  chmod +x "$DIR/setup.sh" "$FETCHER" 2>/dev/null || true

  check_python
  write_env_template

  say "First refresh (2-6 minutes; Hugging Face is the slow part)…"
  if run_fetch; then
    say "Data written to $DIR/data.js"
  else
    warn "Some sources failed, see $LOG_FILE (page still works with what loaded)"
  fi
  tail -n 3 "$LOG_FILE" || true

  install_cron
  if (( SERVE )); then
    install_server
    say "Open:  http://localhost:$PORT   (from other machines: http://$(lan_ip):$PORT)"
  else
    say "Open:  file://$DIR/index.html"
  fi
}

main "$@"
