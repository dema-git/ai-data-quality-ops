# Local Verification

Use this runbook when checking the project from a clean local state.

## Clean Start

```bash
cp .env.example .env
make clean CONFIRM=1
make up
make ps
```

Expected result:

- `api_app` is healthy on port `8000`
- Kafka and MinIO are healthy
- PostgreSQL is healthy on port `5444`
- Airflow scheduler, webserver, API server, and DAG processor are healthy

## Dashboard And API Docs

```bash
curl -fsS -o /dev/null -w "%{http_code}\n" http://localhost:8000/
curl -fsS -o /dev/null -w "%{http_code}\n" http://localhost:8000/openapi.json
curl -fsS -o /dev/null -w "%{http_code}\n" http://localhost:8000/dashboard/metrics
curl -fsS -o /dev/null -w "%{http_code}\n" http://localhost:8000/dashboard/operations
```

All four commands should return `200`.

## Operational Token Protection

Missing token:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8000/etl/run-full
```

Wrong token:

```bash
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "X-API-Token: wrong-token" \
  http://localhost:8000/etl/run-full
```

Correct token:

```bash
curl -fsS \
  -H "X-API-Token: medallion-ops-token" \
  http://localhost:8000/etl/run-full
```

Expected result:

- missing token returns `401`
- wrong token returns `401`
- correct token returns a JSON response with `run_id`

## Airflow Auto-Token Check

Airflow calls the protected operational endpoints through a shared HTTP client.
The client reads `OPERATIONAL_API_TOKEN` from the Airflow container environment
and sends it as `X-API-Token`.

Check the token inside the scheduler container:

```bash
docker compose -f docker-compose.infra.yml -f docker-compose.app.yml -f docker-compose.airflow.yml \
  exec airflow-scheduler printenv OPERATIONAL_API_TOKEN
```

Expected result:

```text
medallion-ops-token
```

Trigger the API through the same shared client used by DAGs:

```bash
docker compose -f docker-compose.infra.yml -f docker-compose.app.yml -f docker-compose.airflow.yml \
  exec airflow-scheduler bash -lc \
  "PYTHONPATH=/opt/airflow/dags python - <<'PY'
from _shared.http_client import call_api
result = call_api('/etl/run-full', timeout_s=1800)
print(result['run_id'])
PY"
```

Expected result: a new ETL `run_id`.

## ETL Run History

Check the latest ETL runs in PostgreSQL:

```bash
docker compose -f docker-compose.infra.yml -f docker-compose.app.yml -f docker-compose.airflow.yml \
  exec db psql -U admin1 -d main -c \
  "SELECT id, status, started_at FROM pipeline.etl_runs ORDER BY id DESC LIMIT 5;"
```

The same latest runs should be visible in the dashboard under `Latest ETL runs`.

## Tests

```bash
make test
```

Expected result: all tests pass.
