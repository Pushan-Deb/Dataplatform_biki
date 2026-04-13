#!/bin/sh
set -eu

exec feast serve_registry --rest-api --no-grpc --port 6572
