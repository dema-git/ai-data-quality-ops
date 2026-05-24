# AI Data Quality Ops

This project is a local backend/data platform for experimenting with AI-assisted data quality operations.

It uses a Medallion pipeline as the baseline system: synthetic user session events are generated, sent through Kafka, stored as raw and processed Parquet files in MinIO, loaded into PostgreSQL analytical marts, and exposed through FastAPI.

The next layer of the project is data operations: controlled bad data, validation reports, incident detection, and AI-generated incident summaries based on structured pipeline facts.

The goal is to show how I structure a backend/data system that is not only runnable, but also inspectable: ingestion, object storage, orchestration, metadata, retries, protected operational endpoints, quality checks, tests, CI, and operational documentation.

## Architecture

![Architecture](readme_assets/data_flow_arch.png)

Main services:

- **FastAPI**: API, dashboard, background event generator, Kafka producer/consumer startup, and operational endpoints.
- **Kafka**: transport for generated session events.
- **MinIO**: object storage for Bronze, Silver, Gold, and archive buckets.
- **Airflow**: runs ETL, archive, and cleanup DAGs.
- **PostgreSQL**: stores Gold marts and pipeline metadata.
- **Kafdrop / pgAdmin / MinIO Console**: local inspection tools.
- **GitHub Actions**: runs the Docker-based test suite on push and PR.

## Project Structure

```text
.
├── airflow/dags/                 # Airflow DAGs and shared HTTP client
├── init-db/                      # Airflow/Postgres initialization files
├── scripts/                      # Init scripts used by Docker services
│   └── init/                     # PostgreSQL schema initialization
├── services/
│   ├── fastapi_app/              # FastAPI app, dashboard, API routes, ETL services
│   ├── faker/                    # Synthetic event generator
│   ├── kafka/                    # Kafka producer/consumer helpers
│   ├── medallion_models/         # Bronze/Silver/Gold dataclasses and transforms
│   ├── init-topic/               # Kafka topic initialization image
│   └── minio-init/               # MinIO bucket initialization image
├── tests/                        # Docker-based pytest suite
├── readme_assets/                # README screenshots and diagrams
├── docker-compose.infra.yml      # Kafka, MinIO, PostgreSQL, pgAdmin, Kafdrop
├── docker-compose.app.yml        # FastAPI application service
├── docker-compose.airflow.yml    # Airflow scheduler/webserver/api/dag-processor
├── docker-compose.tests.yml      # Test runner compose file
├── Makefile                      # Local developer commands
└── .github/workflows/ci.yml      # GitHub Actions CI
```

Runtime folders such as `pgdata`, `minio-data`, and `shared_logs` are created locally and are not part of the source code.

## Current Data Flow

1. FastAPI starts a background generator.
2. The generator creates fake user session events.
3. Events are sent to Kafka.
4. A consumer writes incoming events to Bronze Parquet files in MinIO.
5. Airflow triggers the full ETL flow.
6. Bronze files are validated.
7. Valid Bronze rows are transformed into Silver files.
8. Invalid Bronze rows are written to the `events-quality-issues` quarantine bucket.
9. Silver files are transformed into two Gold datasets:
   - `gold_page_views`
   - `gold_product_events`
10. Gold files are loaded into PostgreSQL.
11. Processed files are registered in `pipeline.outbox_tasks`.
12. Archive and cleanup DAGs move/remove files based on outbox state.

## Medallion Layers

### Bronze

Raw Kafka events are written to MinIO as Parquet files. The Bronze layer is append-only: raw data is not updated in place.

### Silver

Bronze events are cleaned and normalized into a more consistent event shape. Silver still lives in MinIO, so intermediate data is not coupled to PostgreSQL.

Before a Bronze event is promoted to Silver, it passes deterministic quality checks. Invalid events do not stop the whole ETL run. They are written to the `events-quality-issues` bucket with the failed field, issue type, severity, original raw event, and optional injected test issue marker.

### Gold

Silver events are split into analytics-ready datasets:

- page views
- product events

Gold data is then loaded into PostgreSQL tables used by the analytics endpoints.

## Airflow DAGs

### Full ETL DAG

Runs Bronze -> Silver -> Gold -> PostgreSQL.

![Full ETL DAG](readme_assets/dag1.png)

### Archive DAG

Reads pending outbox tasks and moves processed files from active buckets to archive buckets.

![Archive DAG](readme_assets/dag2.png)

### Cleanup DAGs

Remove archived files from MinIO after the archive step.

![Cleanup DAG](readme_assets/dag3.png)

## Dashboard

The dashboard is available at:

```text
http://localhost:8000/
```

It shows:

- Bronze / Silver / Gold file and row counts
- Gold-level page view and product event counters
- latest 5 ETL runs from `pipeline.etl_runs`
- outbox task status counts
- links to Swagger UI and ReDoc

The dashboard exposes quality counts and top rejected issue types. AI-assisted
incident explanations are triggered explicitly through the protected API rather
than during dashboard refreshes.

The dashboard uses HTMX to refresh live metrics without a full page reload.

![Dashboard](readme_assets/dataflow_dashboard.png)

## API Docs

FastAPI docs:

```text
http://localhost:8000/docs
http://localhost:8000/redoc
```

![Swagger UI](readme_assets/dataflow_swagger.png)

The API is split into:

- analytics endpoints
- demo data inspection endpoints
- quality summary and AI-assisted incident endpoints
- operational ETL/archive/cleanup endpoints
- dashboard partial endpoints

## Synthetic Data Generator

The application starts a background generator that creates synthetic user session events and sends them to Kafka.

The generator rate is configurable through environment variables:

```env
GENERATOR_INTERVAL_SECONDS=60
GENERATOR_SESSIONS_PER_BATCH=2
GENERATOR_BAD_DATA_RATE=0.0
```

These defaults keep local data volume modest while developing the data-quality and AI incident flows.

`GENERATOR_BAD_DATA_RATE` controls how often generated events are intentionally corrupted. The default `0.0` keeps the baseline pipeline clean. Higher values can be used later to test quality checks and incident summaries.

## Protected Operational Endpoints

Endpoints that can change pipeline state or delete files require a simple API token.

Protected endpoints:

- `GET /etl/run-full`
- `GET /outbox/archive-run`
- `GET /bronze-archive/cleanup`
- `GET /silver-archive/cleanup`

Token config:

```env
OPERATIONAL_API_TOKEN=medallion-ops-token
```

Request header:

```http
X-API-Token: medallion-ops-token
```

Example:

```bash
curl -H "X-API-Token: medallion-ops-token" http://localhost:8000/etl/run-full
```

Without the header, or with a wrong token, the API returns `401`.

Airflow DAGs use the same protected endpoints, but they do not hardcode the
header in every DAG. The shared Airflow HTTP client reads
`OPERATIONAL_API_TOKEN` from the Airflow container environment and injects
`X-API-Token` automatically. Manual calls through Swagger, browser, or `curl`
must provide the header explicitly.

This is not meant to be a full auth system. It is a small guard for local operational endpoints that should not be accidentally triggered from the browser or by unauthenticated clients.

## AI-Assisted Incident Explanation

Quality validation remains deterministic. Invalid Bronze records are
quarantined first, then summarized into an incident report and routed to one
of four specialist analysis profiles:

- `schema_payload_agent`
- `business_rules_agent`
- `session_integrity_agent`
- `timestamp_quality_agent`

The protected endpoint is manually triggered so dashboard refreshes do not
cause repeated model calls:

```http
POST /quality/incidents/current/explanation
```

The default mode is local and does not require an external API key:

```env
AI_INCIDENT_ANALYSIS_MODE=mock
OPENAI_MODEL=gpt-5.4-nano
OPENAI_MAX_OUTPUT_TOKENS=800
OPENAI_REQUEST_TIMEOUT_SECONDS=30
```

Try the full route with stored quality issues:

```bash
curl -X POST \
  -H "X-API-Token: medallion-ops-token" \
  "http://localhost:8000/quality/incidents/current/explanation?recent_limit=5"
```

The response identifies the selected specialist agent and returns an
operator-facing structured explanation with observed facts, possible causes,
recommended checks, and confidence.

For real model execution, configure the server-side environment only:

```env
AI_INCIDENT_ANALYSIS_MODE=openai
OPENAI_API_KEY=your-api-key
OPENAI_MODEL=gpt-5.4-nano
OPENAI_MAX_OUTPUT_TOKENS=800
```

In `openai` mode, the service calls the OpenAI Responses API and requests a
strict JSON-schema response. The model receives a compact incident context
produced from validation facts, not arbitrary raw pipeline data. The API key
is never returned through the endpoint or exposed in the dashboard.
`OPENAI_MAX_OUTPUT_TOKENS` caps each generated explanation so manual tests
cannot produce unexpectedly long responses.

## Local Setup

Requirements:

- Docker
- Docker Compose
- Make

Create the local env file:

```bash
cp .env.example .env
```

Start everything:

```bash
make up
```

Check containers:

```bash
make ps
```

Useful local URLs:

```text
FastAPI dashboard: http://localhost:8000/
Swagger UI:        http://localhost:8000/docs
Airflow:           http://localhost:8080/
MinIO console:     http://localhost:9101/
Kafdrop:           http://localhost:9000/
pgAdmin:           http://localhost:8889/
```

Follow logs:

```bash
make logs
```

Stop containers:

```bash
make down
```

Clean local runtime state:

```bash
make clean CONFIRM=1
```

`make clean` removes Docker volumes and local runtime folders, so it requires `CONFIRM=1`.

## How to Verify Locally

Short verification flow:

```bash
cp .env.example .env
make clean CONFIRM=1
make up
make ps
make test
```

Then check:

- dashboard opens at `http://localhost:8000/`
- Swagger opens at `http://localhost:8000/docs`
- protected operational endpoints return `401` without `X-API-Token`
- protected operational endpoints work with `X-API-Token: medallion-ops-token`
- Airflow DAGs can call protected endpoints through the shared HTTP client
- latest ETL runs appear in the dashboard

Detailed verification steps are in [docs/local-verification.md](docs/local-verification.md).

## Makefile

```text
make up       Build and start the full local stack
make ps       Show service status
make logs     Follow logs for all services
make test     Run the Docker test suite
make down     Stop the local stack
make clean    Stop stack and remove local runtime state
```

## Tests

Run tests:

```bash
make test
```

The tests run in Docker through `docker-compose.tests.yml`.

Current tests cover:

- database URL configuration
- Kafka consumer configuration
- MinIO manager behavior with dummy clients
- Medallion idempotency checks
- Gold loader duplicate-processing prevention
- operational API token validation

## CI

The GitHub Actions workflow runs on push and pull request to `main`.

It does four things:

1. checks that local `.env` files are not tracked
2. creates `.env` from `.env.example`
3. validates `docker-compose.tests.yml`
4. runs `make test`

Workflow file:

```text
.github/workflows/ci.yml
```

## Reliability Details

### Idempotency

The ETL code checks existing archive outbox records before processing files. This prevents the same active file from being processed twice if `/etl/run-full` is triggered again before the archive worker has moved it.

### ETL Run History

Each `/etl/run-full` call creates a row in `pipeline.etl_runs`.

The dashboard shows the latest runs with:

- status
- Bronze row count
- Silver row count
- Gold row count
- loaded row count
- start time

### Outbox

`pipeline.outbox_tasks` tracks file lifecycle work. Archive workers use it to decide which files should be moved and whether each task is `PENDING`, `IN_PROGRESS`, `DONE`, or `FAILED`.

### Docker Build Context

The repository includes `.dockerignore` so Docker builds do not receive `.git`, local env files, caches, logs, or local runtime data.

## Main Endpoints

Analytics:

- `GET /analytics/top-landing-pages`
- `GET /analytics/top-products`
- `GET /analytics/ab-test-summary`
- `GET /analytics/user/{user_id}/sessions`

Demo data:

- `GET /faker/sample-session`
- `GET /faker/sample-batch`

Operational:

- `GET /etl/run-full`
- `GET /outbox/archive-run`
- `GET /bronze-archive/cleanup`
- `GET /silver-archive/cleanup`

Quality operations:

- `GET /quality/issues/summary`
- `POST /quality/incidents/current/explanation`

Dashboard:

- `GET /`
- `GET /dashboard/metrics`
- `GET /dashboard/operations`

## Current Focus

- separating raw, cleaned, and analytical data layers
- keeping intermediate data in object storage
- making repeated ETL runs safe
- tracking file lifecycle through an outbox table
- exposing enough operational state in the dashboard
- keeping operational endpoints protected
- making local setup reproducible with Docker Compose and Make
- covering important behavior with focused tests
- running tests in CI

## AI Ops Layer

The AI/data-quality layer builds on top of a working Medallion ETL baseline instead of replacing it.

Implemented:

- configurable synthetic data generation rate
- controlled invalid event injection
- data quality validation reports
- structured quality incident reports
- specialized agent routing by dominant quality category
- OpenAI-compatible incident explanations based on compact structured reports
- dashboard section for quality status

Possible next extensions:

- time-windowed incident severity instead of cumulative quality counts
- stale pipeline run and outbox backlog incident rules
- display of the most recently generated explanation in the dashboard

The AI layer should explain pipeline state and recommend next actions. It should not mutate data, run cleanup, or trigger recovery actions without an explicit operational endpoint.

## Limitations

This is a local Docker Compose project. It is not a production deployment.

Things I intentionally did not add:

- Alembic migrations
- full user auth / roles
- production secrets management
- distributed locking for multiple ETL workers
- full end-to-end integration tests for the entire stack
- real S3 lifecycle policies
- deployment manifests

For this project, I kept the scope around local reproducibility, pipeline behavior, and operational clarity.
