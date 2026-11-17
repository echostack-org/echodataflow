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

exec >>"$ECHODATAFLOW_LOG_DIR/prefect-worker.out.log" 2>>"$ECHODATAFLOW_LOG_DIR/prefect-worker.err.log"

exec "$MAMBA_BIN" run -n "$ECHODATAFLOW_ENV" prefect worker start --pool "$PREFECT_POOL"
