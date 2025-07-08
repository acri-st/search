#!/bin/bash
DEV_EXTRA_PARAMETERS=""
if [ -d .venv ]; then
    echo "#### Activating virtual environment"
    . .venv/bin/activate
fi
EXTRA_DIR=""
if [ ! -z $DEV_MODE ]; then
    if [ -d /app/base-service ]; then
        echo "#### Installing base-service in editable mode"
        EXTRA_DIR="--reload-dir base-service"
        uv pip install -e /app/base-service
    fi
    if [ -d /app/shared-library ]; then
        echo "#### Installing base-service in editable mode"
        EXTRA_DIR="$EXTRA_DIR --reload-dir shared-library"
        uv pip install -e /app/shared-library
    fi
    DEV_EXTRA_PARAMETERS="--lifespan on --reload --reload-dir ${ENTRYPOINT:-"msfwk"} $EXTRA_DIR"
fi
echo "#### Startup script will start with in [DEV_MODE=${DEV_MODE:-'false'}]"
echo "uvicorn ${ENTRYPOINT:-"msfwk"}.main:app --host 0.0.0.0 --port 5000 $DEV_EXTRA_PARAMETERS"
uv run python -m uvicorn ${ENTRYPOINT:-"msfwk"}.main:app --host 0.0.0.0 --port 5000 $DEV_EXTRA_PARAMETERS