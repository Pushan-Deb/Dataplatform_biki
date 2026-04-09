Airbyte runs here as a separate Docker Compose stack.

Main URL:
- http://localhost:8000
- http://10.155.38.206:8000

Default credentials:
- username: dataplatform
- password: Dataplatform@123

Notes:
- This stack is based on Airbyte's official deprecated Docker Compose deployment assets.
- Update `WEBAPP_URL` in `.env` if the host ZeroTier IP changes.
