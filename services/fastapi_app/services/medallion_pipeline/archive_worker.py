#####################################################################
# archive_worker.py
#
#Archive Worker for Medallion Pipeline
#
#This module implements an outbox-based worker responsible for archiving files
#between MinIO buckets.

# Do:
# * Reads ARCHIVE tasks from the pipeline outbox table
# * Determines the correct source and archive bucket based on dataset and layer
# * Moves the object to the archive bucket
# * Updates task status (IN_PROGRESS, DONE, FAILED)
#
#Used for archiving data across Bronze, Silver and Gold layers
#in the medallion architecture.
#####################################################################

from typing import Dict
from minio.error import S3Error
from minio_utils.minio_client import get_minio_manager
from services.medallion_pipeline.pipeline_state import (
    fetch_pending_tasks,
    mark_task_in_progress,
    mark_task_done,
    mark_task_failed,
    OutboxTask,
    map_outbox_tasks,
)

from services.medallion_pipeline.medallion_service import (
    BRONZE_BUCKET,
    BRONZE_ARCHIVE_BUCKET,
    SILVER_BUCKET,
    SILVER_ARCHIVE_BUCKET,
    GOLD_PAGE_VIEWS_BUCKET,
    GOLD_PAGE_VIEWS_ARCHIVE_BUCKET,
    GOLD_PRODUCT_VIEWS_BUCKET,
    GOLD_PRODUCT_VIEWS_ARCHIVE_BUCKET,
)

# Initialize MinIO manager
minio_manager = get_minio_manager()


# Routing map:
# (dataset, layer) -> (source_bucket, archive_bucket)
BUCKET_ROUTING: Dict[tuple, tuple] = {
    ("web_events", "bronze"): (BRONZE_BUCKET, BRONZE_ARCHIVE_BUCKET),
    ("web_events", "silver"): (SILVER_BUCKET, SILVER_ARCHIVE_BUCKET),
    ("web_events", "gold_page_views"): (GOLD_PAGE_VIEWS_BUCKET, GOLD_PAGE_VIEWS_ARCHIVE_BUCKET),
    ("web_events", "gold_product_events"): (GOLD_PRODUCT_VIEWS_BUCKET, GOLD_PRODUCT_VIEWS_ARCHIVE_BUCKET),
}


def run_archive_worker(limit: int = 50) -> Dict[str, int]:
    """
    Outbox worker for file archiving

    Flow:
    - Read tasks from pipeline.outbox_tasks (event_type='ARCHIVE')
    - For each task:
        1. Set status to IN_PROGRESS
        2. Move object to archive bucket
        3. On success -> mark DONE
        4. On error -> mark FAILED and store last_error
    """

    #1. Fetch raw rows from DB
    raw_rows = fetch_pending_tasks(
        event_type="ARCHIVE",
        dataset="web_events",
        limit=limit,
    )

    # Map DB rows to domain objects
    tasks = map_outbox_tasks(raw_rows)

    # Nothing to process
    if not tasks:
        return {"taken": 0, "archived": 0, "failed": 0, "skipped": 0}

    archived = 0
    failed = 0
    skipped = 0

    for task in tasks:
        route_key = (task.dataset, task.layer)
        buckets = BUCKET_ROUTING.get(route_key)

        # No bucket
        if not buckets:
            msg = f"Unknown dataset/layer for archive: {task.dataset}/{task.layer}"
            mark_task_failed(task.id, msg)
            failed += 1
            continue

        source_bucket, archive_bucket = buckets

        try:
            #2 Mark task as IN_PROGRESS
            claimed = mark_task_in_progress(task.id)

            if not claimed:
                skipped += 1
                continue

            #3. Move object from source to archive bucket
            minio_manager.move_single_object(
                source_bucket=source_bucket,
                target_bucket=archive_bucket,
                object_name=task.partition_key,
            )

            #4. Mark task as DONE on success
            mark_task_done(task.id)
            archived += 1

        except S3Error as e:
            if e.code == "NoSuchKey":
                mark_task_done(task.id)
                skipped += 1
                continue

            mark_task_failed(task.id, f"S3Error {e.code}: {e.message}")
            failed += 1

        except Exception as e:
            mark_task_failed(task.id, str(e))
            failed += 1

    return {
        "taken": len(tasks),
        "archived": archived,
        "failed": failed,
        "skipped": skipped,
    }
