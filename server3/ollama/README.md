Ollama runs here as a separate Docker Compose stack.

Main API:
- http://localhost:11434
- http://10.155.38.206:11434

Default model:
- llama3.2:1b

Useful endpoints:
- GET /api/tags
- POST /api/generate
- POST /api/chat

Notes:
- This stack is CPU-friendly and uses persistent model storage in `./data`.
- Update `OLLAMA_BASE_URL` in `.env` if the host ZeroTier IP changes.
