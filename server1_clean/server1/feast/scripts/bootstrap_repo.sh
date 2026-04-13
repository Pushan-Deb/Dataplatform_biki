#!/bin/sh
set -eu

python /opt/feast/scripts/bootstrap_data.py
feast apply
NOW_UTC="$(python -c "from datetime import datetime, timezone; print(datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'))")"
feast materialize-incremental "$NOW_UTC"
