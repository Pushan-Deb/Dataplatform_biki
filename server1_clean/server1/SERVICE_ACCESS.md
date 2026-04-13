# Service Access

All external service links below use the ZeroTier IP of server 1:

- Host IP: `10.155.38.139`

## Shared Credentials

- Standard username: `dataplatform`
- Standard password: `Dataplatform@123`

## Airflow

- UI: `http://10.155.38.139:8080`
- Health: `http://10.155.38.139:8080/health`
- API base: `http://10.155.38.139:8080/api/v1`
- Login:
  - Username: `dataplatform`
  - Password: `Dataplatform@123`
- API auth:
  - HTTP Basic Auth

## OpenMetadata

- UI: `http://10.155.38.139:8585`
- Version check: `http://10.155.38.139:8585/api/v1/system/version`
- API base: `http://10.155.38.139:8585/api/v1`
- Login:
  - Email: `dataplatform@open-metadata.org`
  - Password: `Dataplatform@123`
- API auth:
  - Login endpoint: `POST /api/v1/users/login`
  - Password in the login payload must be base64-encoded
  - Base64 password value for `Dataplatform@123`: `RGF0YXBsYXRmb3JtQDEyMw==`
- Backend database:
  - PostgreSQL
- Seeded metadata hierarchy:
  - Service: `trino`
  - Database: `delta`
  - Schema: `demo`

## Marquez / OpenLineage

- Web UI: `http://10.155.38.139:3000`
- API: `http://10.155.38.139:5000`
- Admin health: `http://10.155.38.139:5001/healthcheck`
- OpenLineage events endpoint: `http://10.155.38.139:5000/api/v1/lineage`
- Login:
  - Username: `dataplatform`
  - Password: `Dataplatform@123`
- API auth:
  - HTTP Basic Auth on the main UI/API gateway
- No-auth health endpoints:
  - `http://10.155.38.139:3000/healthz`
  - `http://10.155.38.139:5000/healthz`
  - `http://10.155.38.139:5001/healthcheck`

## Keycloak

- Admin console: `http://10.155.38.139:8090`
- Login:
  - Username: `dataplatform`
  - Password: `Dataplatform@123`

## MLflow

- UI and API: `http://10.155.38.139:5002`
- Login:
  - Username: `dataplatform`
  - Password: `Dataplatform@123`
- API auth:
  - HTTP Basic Auth

## Feast

- Feature server: `http://10.155.38.139:6566`
- Registry REST server: `http://10.155.38.139:6572`
- Feature server health: `http://10.155.38.139:6566/health`
- Feature server OpenAPI: `http://10.155.38.139:6566/openapi.json`
- Registry OpenAPI: `http://10.155.38.139:6572/openapi.json`
- Login:
  - Username: `dataplatform`
  - Password: `Dataplatform@123`
- API auth:
  - HTTP Basic Auth
- Seeded Feast objects:
  - Project: `dataplatform`
  - Entity: `customer`
  - Feature view: `customer_profile_features`
  - Feature service: `customer_profile_service`

## FastAPI

- API: `http://10.155.38.139:8000`
- Health: `http://10.155.38.139:8000/health`
- Auth:
  - No shared external username/password has been configured on this gateway

## Notes

- These links are intended to be used over ZeroTier.
- Internal databases and storage services are not meant to be accessed directly from outside their own compose networks unless we expose them later on purpose.
