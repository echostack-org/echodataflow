#!/bin/bash
set -e
ENV_FILE="$HOME/.config/echodataflow/services.env"
[[ -f "$ENV_FILE" ]] || { echo "Missing $ENV_FILE"; exit 1; }

set -a
source "$ENV_FILE"
set +a

: "${ECHODATAFLOW_HOME:?ECHODATAFLOW_HOME is not set in services.env}"
: "${ECHODATAFLOW_WORKDIR:?ECHODATAFLOW_WORKDIR is not set in services.env}"
: "${ECHODATAFLOW_LOG_DIR:?ECHODATAFLOW_LOG_DIR is not set in services.env}"

mkdir -p "$ECHODATAFLOW_LOG_DIR"
cd "$ECHODATAFLOW_WORKDIR" || { echo "Missing WORKDIR: $ECHODATAFLOW_WORKDIR"; exit 1; }

exec >>"$ECHODATAFLOW_LOG_DIR/prefect-server.out.log" 2>>"$ECHODATAFLOW_LOG_DIR/prefect-server.err.log"

DB_PATH="${PREFECT_DB_PATH:-$ECHODATAFLOW_HOME/.prefect/prefect.db}"
AUTO_RESET="${PREFECT_DB_AUTORESET_ON_FAILURE:-false}"

if [[ -f "$DB_PATH" ]]; then
  command -v sqlite3 >/dev/null || { echo "Install sqlite3 for the database health check"; exit 1; }
  DB_CHECK="$(sqlite3 "$DB_PATH" "PRAGMA quick_check;" 2>/dev/null || true)"
  if [[ "$DB_CHECK" != "ok" ]]; then
    echo "Prefect DB health check failed for $DB_PATH: ${DB_CHECK:-no output}"
    if [[ "$AUTO_RESET" == "true" ]]; then
      ts="$(date +%Y%m%d_%H%M%S)"
      backup_dir="$ECHODATAFLOW_HOME/.prefect/db-backups/$ts"
      mkdir -p "$backup_dir"
      mv "${DB_PATH}"* "$backup_dir"/ 2>/dev/null || true
      echo "Archived DB files to $backup_dir"
    else
      echo "Set PREFECT_DB_AUTORESET_ON_FAILURE=true in services.env to auto-archive and reset."
      exit 1
    fi
  fi
fi

exec "$MAMBA_BIN" run -n "$ECHODATAFLOW_ENV" prefect server start --host 127.0.0.1 --port 4200
