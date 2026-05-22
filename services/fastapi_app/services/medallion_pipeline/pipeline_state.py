#################################################
# pipeline_state.py
#
# This module manages the processing state and the outbox table
# for the medallion data pipeline.
#
# It is responsible for:
# - Tracking incremental progress (checkpoint logic)
# - Reading pending tasks from the outbox table
# - Updating task status (IN_PROGRESS, DONE, FAILED)
#
# The processing_state table works as a checkpoint mechanism.
# It stores the last successfully processed timestamp and ID
# for each dataset and layer.
#
# The outbox_tasks table is used for background workers
# (for example, archive worker).
#
# All updates are executed inside transactions to keep
# the state consistent.

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text

from db_utils.database import build_database_url

engine = create_engine(build_database_url("postgresql+psycopg2"))


def ensure_etl_runs_table() -> None:
    """
    Create the ETL run history table when an existing local DB is reused.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS pipeline.etl_runs (
                    id BIGSERIAL PRIMARY KEY,
                    status TEXT NOT NULL,
                    bronze_count INT NOT NULL DEFAULT 0,
                    silver_count INT NOT NULL DEFAULT 0,
                    gold_page_views_count INT NOT NULL DEFAULT 0,
                    gold_product_events_count INT NOT NULL DEFAULT 0,
                    loaded_page_views_count INT NOT NULL DEFAULT 0,
                    loaded_product_events_count INT NOT NULL DEFAULT 0,
                    error_message TEXT,
                    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMP,
                    duration_seconds NUMERIC(10,3)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_etl_runs_started_at
                    ON pipeline.etl_runs(started_at DESC)
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_etl_runs_status
                    ON pipeline.etl_runs(status)
                """
            )
        )

@dataclass
class OutboxTask:
    """
    Represents one row from pipeline.outbox_tasks.

    This object is used by workers to process
    archive or other background tasks.
    """
    id: int
    event_type: str
    dataset: str
    layer: str
    partition_key: str
    status: str
    attempts: int
    last_error: Optional[str]
    created_at: datetime
    updated_at: datetime


def map_outbox_tasks(rows: List[Dict[str, Any]]) -> List[OutboxTask]:
    """
    Convert raw DB rows (dict format) into OutboxTask objects.
    """
    return [OutboxTask(**row) for row in rows]


def update_processing_state(dataset: str, layer: str,
                            last_processed_ts: Optional[datetime] = None,
                            last_processed_id: Optional[int] = None) -> None:
    """
    Update incremental checkpoint for a dataset and layer.

    If the record does not exist, it will be inserted.
    If it exists, it will be updated.

    The timestamp is updated using GREATEST() to ensure
    we never move the checkpoint backwards.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO pipeline.processing_state (
                    dataset, layer, last_processed_ts, last_processed_id
                )
                VALUES (:dataset, :layer, :ts, :id)
                ON CONFLICT (dataset, layer)
                DO UPDATE
                SET
                    last_processed_ts = GREATEST(
                        COALESCE(pipeline.processing_state.last_processed_ts, '-infinity'::timestamp),
                        COALESCE(EXCLUDED.last_processed_ts, '-infinity'::timestamp)
                    ),
                    last_processed_id = COALESCE(EXCLUDED.last_processed_id,
                                                 pipeline.processing_state.last_processed_id),
                    updated_at = NOW()
                """
            ),
            {
                "dataset": dataset,
                "layer": layer,
                "ts": last_processed_ts,
                "id": last_processed_id,
            },
        )

def fetch_pending_tasks(*, event_type: Optional[str] = None,
                        dataset: Optional[str] = None, layer: Optional[str] = None,
                        limit: int = 50,) -> List[Dict[str, Any]]:
    """
    Fetch tasks from pipeline.outbox_tasks that are ready to run.

    Conditions:
    - status is PENDING or FAILED
    - attempts < 5

    Optional filters:
    - event_type
    - dataset
    - layer

    Tasks are ordered by updated_at (oldest first).
    """
    base_sql = """
        SELECT 
            id, event_type, dataset, layer, partition_key,
            status, attempts, last_error, created_at, updated_at
        FROM pipeline.outbox_tasks
        WHERE (status = 'PENDING' OR status = 'FAILED')
          AND attempts < 5
    """

    params: Dict[str, Any] = {"limit": limit}

    if event_type:
        base_sql += " AND event_type = :event_type"
        params["event_type"] = event_type
    if dataset:
        base_sql += " AND dataset = :dataset"
        params["dataset"] = dataset
    if layer:
        base_sql += " AND layer = :layer"
        params["layer"] = layer

    base_sql += " ORDER BY updated_at ASC LIMIT :limit"

    query = text(base_sql)

    with engine.begin() as conn:
        rows = conn.execute(query, params).mappings().all()

    return [dict(row) for row in rows]


def fetch_latest_outbox_tasks(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch the latest outbox records for the operational dashboard.

    These rows represent recent ETL handoffs between Medallion layers.
    """
    query = text(
        """
        SELECT
            id, event_type, dataset, layer, partition_key,
            status, attempts, last_error, created_at, updated_at
        FROM pipeline.outbox_tasks
        ORDER BY updated_at DESC, id DESC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()

    return [dict(row) for row in rows]


def start_etl_run() -> int:
    """
    Create a RUNNING ETL run record and return its ID.
    """
    ensure_etl_runs_table()

    with engine.begin() as conn:
        run_id = conn.execute(
            text(
                """
                INSERT INTO pipeline.etl_runs (status)
                VALUES ('RUNNING')
                RETURNING id
                """
            )
        ).scalar_one()

    return run_id


def finish_etl_run(run_id: int, result: Dict[str, Any]) -> None:
    """
    Mark an ETL run as successful and store key row counts.
    """
    bronze_to_silver = result.get("bronze_to_silver", {})
    silver_to_gold = result.get("silver_to_gold", {})
    gold_loader = result.get("gold_loader", {})

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE pipeline.etl_runs
                SET status = 'SUCCESS',
                    bronze_count = :bronze_count,
                    silver_count = :silver_count,
                    gold_page_views_count = :gold_page_views_count,
                    gold_product_events_count = :gold_product_events_count,
                    loaded_page_views_count = :loaded_page_views_count,
                    loaded_product_events_count = :loaded_product_events_count,
                    finished_at = NOW(),
                    duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
                WHERE id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "bronze_count": bronze_to_silver.get("bronze_count", 0),
                "silver_count": silver_to_gold.get("silver_count", 0),
                "gold_page_views_count": silver_to_gold.get("gold_page_views_count", 0),
                "gold_product_events_count": silver_to_gold.get("gold_product_events_count", 0),
                "loaded_page_views_count": gold_loader.get("inserted_page_views", 0),
                "loaded_product_events_count": gold_loader.get("inserted_product_events", 0),
            },
        )


def fail_etl_run(run_id: int, error_message: str) -> None:
    """
    Mark an ETL run as failed and store a short error message.
    """
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE pipeline.etl_runs
                SET status = 'FAILED',
                    error_message = :error_message,
                    finished_at = NOW(),
                    duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
                WHERE id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "error_message": error_message[:500],
            },
        )


def fetch_latest_etl_runs(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch latest full ETL runs for the operational dashboard.
    """
    ensure_etl_runs_table()

    query = text(
        """
        SELECT
            id, status, bronze_count, silver_count,
            gold_page_views_count, gold_product_events_count,
            loaded_page_views_count, loaded_product_events_count,
            error_message, started_at, finished_at, duration_seconds
        FROM pipeline.etl_runs
        ORDER BY started_at DESC, id DESC
        LIMIT :limit
        """
    )

    with engine.begin() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()

    return [dict(row) for row in rows]


def fetch_outbox_status_counts() -> Dict[str, int]:
    """
    Count outbox tasks by status for the operational dashboard.
    """
    query = text(
        """
        SELECT status, COUNT(*) AS tasks_count
        FROM pipeline.outbox_tasks
        GROUP BY status
        """
    )

    counts = {
        "PENDING": 0,
        "IN_PROGRESS": 0,
        "DONE": 0,
        "FAILED": 0,
    }

    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()

    for row in rows:
        counts[row["status"]] = row["tasks_count"]

    counts["TOTAL"] = sum(counts.values())
    return counts


def mark_task_in_progress(task_id: int) -> bool:
    """"
    Try to mark a task as IN_PROGRESS.

    The update will succeed only if:
    - status is PENDING or FAILED
    - attempts < 5

    Returns True if the task was successfully claimed.
    Returns False if another worker already claimed it.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'IN_PROGRESS',
            attempts = attempts + 1,
            updated_at = NOW()
        WHERE id = :id
          AND status IN ('PENDING', 'FAILED')
          AND attempts < 5
        """
    )

    with engine.begin() as conn:
        res = conn.execute(query, {"id": task_id})
        return res.rowcount == 1


def mark_task_done(task_id: int) -> None:
    """
    Mark task as DONE.

    This will work only if the task is currently IN_PROGRESS.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'DONE',
            updated_at = NOW()
        WHERE id = :id
          AND status = 'IN_PROGRESS'
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"id": task_id})


def mark_task_failed(task_id: int, error_message: str) -> None:
    """
    Mark task as FAILED and store the error message.

    Only tasks in IN_PROGRESS status can be marked as FAILED.
    Error message is limited to 500 characters.
    """
    query = text(
        """
        UPDATE pipeline.outbox_tasks
        SET status = 'FAILED',
            last_error = :err,
            updated_at = NOW()
        WHERE id = :id
          AND status = 'IN_PROGRESS'
        """
    )

    with engine.begin() as conn:
        conn.execute(query, {"id": task_id, "err": error_message[:500]})
