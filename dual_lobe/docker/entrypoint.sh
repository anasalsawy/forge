#!/bin/sh
set -e

# Apply migrations on start (idempotent), seed a default key if none exist,
# then exec the container command.
alembic upgrade head
python -m dual_lobe.core.bootstrap || true
exec "$@"