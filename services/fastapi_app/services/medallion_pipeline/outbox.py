####################################################################
# outbox.py
#
# Outbox pattern implementation for tracking successfully archived files.
#
# This module provides functionality to record files that have been successfully transferred
# from one storage bucket to another. Records are only written to the database AFTER the file
# transfer is confirmed successful, ensuring data consistency.
#
# The outbox table serves as a reliable log of all completed file transfers. Background workers
# use this table to discover which files have been moved to the next layer's archive and need
# to be archived in their original location. This prevents data loss and ensures proper cleanup
# of transferred files.
#
# Key guarantees:
# - Records are only created after successful file transfer
# - Each record confirms a file is safely stored in the destination bucket
# - Workers can reliably find all files ready for archival
####################################################################

from datetime import datetime
from typing import Iterable, Optional, Set

from sqlalchemy import bindparam, create_engine, text

from db_utils.database import build_database_url


engine = create_engine(build_database_url("postgresql+psycopg2"))


def enqueue_archive_task(dataset: str, layer: str, partition_key: str,
                         event_type: str = "ARCHIVE",
                         next_retry_at: Optional[datetime] = None
                         ) -> bool:
    """
    Register a file that has been successfully transferred to the next layer.

    IMPORTANT: This function should ONLY be called after confirming that the file
    has been successfully copied to the destination bucket. The database record
    acts as proof that the file transfer completed successfully and is now ready
    for archival in the source bucket.

    Returns True when a task was created and False when the same archive task
    already exists.
    """

    if next_retry_at is None:
        next_retry_at = datetime.now()

    with engine.begin() as conn:
        inserted_id = conn.execute(
            text(
                """
                INSERT INTO pipeline.outbox_tasks (
                    event_type, dataset, layer, partition_key,
                    status, attempts, next_retry_at
                )
                VALUES (:event_type, :dataset, :layer, :partition_key,
                        'PENDING', 0, :next_retry_at)
                ON CONFLICT (event_type, dataset, layer, partition_key)
                DO NOTHING
                RETURNING id
                """
            ),
            {
                "event_type": event_type,
                "dataset": dataset,
                "layer": layer,
                "partition_key": partition_key,
                "next_retry_at": next_retry_at,
            },
        ).scalar_one_or_none()

    return inserted_id is not None


def get_existing_archive_partition_keys(
        dataset: str,
        layer: str,
        partition_keys: Iterable[str],
        event_type: str = "ARCHIVE",
) -> Set[str]:
    """
    Return partition keys that already have an outbox task.

    ETL stages use this to avoid re-processing files that were already handed
    off to the archive worker, even if the worker has not moved them yet.
    """
    keys = [key for key in partition_keys if key]
    if not keys:
        return set()

    query = text(
        """
        SELECT partition_key
        FROM pipeline.outbox_tasks
        WHERE event_type = :event_type
          AND dataset = :dataset
          AND layer = :layer
          AND partition_key IN :partition_keys
        """
    ).bindparams(bindparam("partition_keys", expanding=True))

    with engine.begin() as conn:
        rows = conn.execute(
            query,
            {
                "event_type": event_type,
                "dataset": dataset,
                "layer": layer,
                "partition_keys": keys,
            },
        ).scalars().all()

    return set(rows)
