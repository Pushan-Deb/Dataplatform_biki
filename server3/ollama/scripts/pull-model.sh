#!/bin/sh
set -eu

until curl -fsS http://ollama:11434/api/tags >/dev/null; do
  sleep 2
done

curl -fsS http://ollama:11434/api/pull \
  -d "{\"model\":\"${OLLAMA_MODEL}\",\"stream\":false}"
