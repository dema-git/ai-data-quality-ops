######################################################################
# gold_loader.py
#
# This module is responsible for loading data into the GOLD layer.
#
# It performs the following steps:
# - reads Parquet files from MinIO (gold buckets)
# - converts rows into GoldPageView and GoldProductEvent models
# - inserts data into mart.gold_page_views and mart.gold_product_events
# - creates archive tasks for successfully processed files
#
# The module is used as part of the medallion pipeline to move data
# from object storage into the analytical database layer.
#######################################################################

import math
from dataclasses import asdict
from typing import List, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from db_utils.database import build_database_url
from minio_utils.files_handler import get_files_data
from services.medallion_pipeline.pipeline_state import (fetch_pending_tasks,
                                                        mark_task_done)
from services.medallion_pipeline.outbox import (
    enqueue_archive_task,
    get_existing_archive_partition_keys,
)
from exceptions_logging.logger import AppLogger
from services.medallion_models.gold_models import GoldPageView, GoldProductEvent


log = AppLogger(component="gold_loader")


engine = create_engine(build_database_url("postgresql+psycopg2"))

BATCH_SIZE = 10_000

def insert_gold_page_views(events: List[GoldPageView]) -> int:
    """
    Insert a list of GoldPageView instances into mart.gold_page_views.
    """
    if not events:
        log.warning("insert_gold_page_views called with empty list")
        return 0

    rows = [asdict(e) for e in events]
    total = 0

    log.info("insert_gold_page_views started", total_rows=len(rows),
             batch_size=BATCH_SIZE)
    try:
        with engine.begin() as conn:
            for start in range(0, len(rows), BATCH_SIZE):
                chunk = rows[start : start + BATCH_SIZE]
                conn.execute(
                    text(
                        """
                        INSERT INTO mart.gold_page_views (
                            event_time,
                            session_id,
                            user_id,
                            page_url,
                            page_category,
                            page_item,
                            scroll_depth,
                            ab_group
                        )
                        VALUES (
                            :event_time,
                            :session_id,
                            :user_id,
                            :page_url,
                            :page_category,
                            :page_item,
                            :scroll_depth,
                            :ab_group
                        )
                        """
                    ),
                    chunk,
                )
                total += len(chunk)

        log.info("insert_gold_page_views done", inserted=total)
        return total

    except SQLAlchemyError as e:
        log.exception(f"insert_gold_page_views database error: {e.args}",
                      total_rows=len(rows))
        raise
    except Exception as e:
        log.exception(f"insert_gold_page_views unexpected error: {e.args}",
                      total_rows=len(rows))
        raise


def insert_gold_product_events(events: List[GoldProductEvent]) -> int:
    """
    Insert a list of GoldProductEvent instances into mart.gold_product_events.
    """
    if not events:
        return 0

    rows = [asdict(e) for e in events]
    total = 0

    log.info("insert_gold_product_events started", total_rows=len(rows),
             batch_size=BATCH_SIZE)
    try:
        with engine.begin() as conn:
            for start in range(0, len(rows), BATCH_SIZE):
                chunk = rows[start : start + BATCH_SIZE]
                conn.execute(
                    text(
                        """
                        INSERT INTO mart.gold_product_events (
                            event_time,
                            session_id,
                            user_id,
                            product_id,
                            price,
                            ab_group,
                            page_url
                        )
                        VALUES (
                            :event_time,
                            :session_id,
                            :user_id,
                            :product_id,
                            :price,
                            :ab_group,
                            :page_url
                        )
                        """
                    ),
                    chunk,
                )
                total += len(chunk)

        log.info("insert_gold_product_events done", inserted=total)
        return total
    except SQLAlchemyError:
        log.exception("insert_gold_product_events database error", total_rows=len(rows))
        raise
    except Exception as e:
        log.exception(f"insert_gold_product_events unexpected error: {e.args}",
                      total_rows=len(rows))
        raise


GOLD_PAGE_VIEWS_BUCKET = "events-gold-page-views"
GOLD_PRODUCT_VIEWS_BUCKET = "events-gold-product-events"


def filter_unprocessed_gold_files(files: List[Dict], layer: str) -> List[Dict]:
    """
    Keep only Gold files that do not already have archive tasks.

    This makes DB loading idempotent when the loader is called repeatedly before
    the archive worker moves processed files out of the active Gold buckets.
    """
    object_names = [f.get("object_name") for f in files if f.get("object_name")]
    processed_keys = get_existing_archive_partition_keys(
        dataset="web_events",
        layer=layer,
        partition_keys=object_names,
    )

    return [
        f for f in files
        if f.get("object_name") not in processed_keys
    ]


def clean_nan(d: dict) -> dict:
    cleaned = {}
    for k, v in d.items():
        if isinstance(v, float) and math.isnan(v):
            cleaned[k] = None
        else:
            cleaned[k] = v
    return cleaned

def process_gold_outbox_tasks() -> Dict[str, int]:
    """
    - Read gold parquet files from MinIO
    - Convert rows to Gold models
    - Insert into mart tables
    - Enqueue archive tasks per processed object
    """
    log.info(
        "process_gold_outbox_tasks started",
        page_views_bucket=GOLD_PAGE_VIEWS_BUCKET,
        product_events_bucket=GOLD_PRODUCT_VIEWS_BUCKET,
        batch_size=BATCH_SIZE,
    )

    try:
        # Download all Parquet files from GOLD buckets in MinIO
        page_view_files = get_files_data(GOLD_PAGE_VIEWS_BUCKET)
        product_event_files = get_files_data(GOLD_PRODUCT_VIEWS_BUCKET)
        page_view_files_to_process = filter_unprocessed_gold_files(
            page_view_files,
            layer="gold_page_views",
        )
        product_event_files_to_process = filter_unprocessed_gold_files(
            product_event_files,
            layer="gold_product_events",
        )

        # These lists will contain validated Gold model instances
        page_views: List[GoldPageView] = []
        product_events: List[GoldProductEvent] = []

        #Parse page view files and convert rows into GoldPageView objects
        for f in page_view_files_to_process:
            for row in f["data"]:
                row = clean_nan(row)
                page_views.append(GoldPageView(**row))

        #  Parse product event files and convert rows into GoldProductEvent objects
        for f in product_event_files_to_process:
            for row in f["data"]:
                product_events.append(GoldProductEvent(**row))

        # If no rows were parsed from files, stop processing
        if not page_views and not product_events:
            log.warning(
                "no gold files found",
                page_view_files=len(page_view_files),
                product_event_files=len(product_event_files),
            )
            return {
                "inserted_page_views": 0,
                "inserted_product_events": 0,
                "page_view_files": len(page_view_files_to_process),
                "product_event_files": len(product_event_files_to_process),
                "skipped_page_view_files": (
                    len(page_view_files) - len(page_view_files_to_process)
                ),
                "skipped_product_event_files": (
                    len(product_event_files) - len(product_event_files_to_process)
                ),
            }

        # Insert parsed rows into the GOLD database tables (batched)
        inserted_pv = insert_gold_page_views(page_views) if page_views else 0
        inserted_pe = insert_gold_product_events(product_events) if product_events else 0

        # After successful DB insert, enqueue archive tasks
        for f in page_view_files_to_process:
            enqueue_archive_task(
                dataset="web_events",
                layer="gold_page_views",
                partition_key=f["object_name"],
                event_type="ARCHIVE",
            )

        for f in product_event_files_to_process:
            enqueue_archive_task(
                dataset="web_events",
                layer="gold_product_events",
                partition_key=f["object_name"],
                event_type="ARCHIVE",
            )
        log.info(
            "process_gold_outbox_tasks done",
            page_view_files=len(page_view_files_to_process),
            product_event_files=len(product_event_files_to_process),
        )

    except Exception as e:
        log.exception(
            f"process_gold_outbox_tasks failed: {e.args}",
        )
        return {
            "inserted_page_views": 0,
            "inserted_product_events": 0,
            "page_view_files": 0,
            "product_event_files": 0,
        }


    return {
        "inserted_page_views": inserted_pv,
        "inserted_product_events": inserted_pe,
        "page_view_files": len(page_view_files_to_process),
        "product_event_files": len(product_event_files_to_process),
        "skipped_page_view_files": (
            len(page_view_files) - len(page_view_files_to_process)
        ),
        "skipped_product_event_files": (
            len(product_event_files) - len(product_event_files_to_process)
        ),
    }
