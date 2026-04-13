#!/bin/sh
set -eu

exec feast serve --host 0.0.0.0 --port 6566
