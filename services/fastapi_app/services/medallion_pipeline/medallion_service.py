#################################################
# medallion_service.py
#
# Implements the Medallion Architecture ETL pipeline:
# - Bronze → Silver: raw events cleanup & validation
# - Silver → Gold: business logic transformations
#
# Each layer stores data in MinIO and archives
# processed files to prevent reprocessing !!!
#
##################################################

from dataclasses import asdict
from typing import List, Dict
from minio_utils.files_handler import get_files_data, upload_batch

from services.medallion_pipeline.outbox import (
    enqueue_archive_task,
    get_existing_archive_partition_keys,
)
from services.medallion_pipeline.pipeline_state import update_processing_state
from services.medallion_models.bronze_model import BronzeWebEvent
from services.medallion_models.gold_models import GoldProductEvent, GoldPageView
from services.medallion_models.silver_model import SilverWebEvent
from services.medallion_models.helpers import (bronze_to_silver, silver_to_gold_page_view,
                                               silver_to_gold_product)
from exceptions_logging.logger import AppLogger


log = AppLogger(component="medallion_service")

BRONZE_BUCKET = "events-bronze"
BRONZE_ARCHIVE_BUCKET = "events-bronze-archive"
SILVER_BUCKET = "events-silver"
SILVER_ARCHIVE_BUCKET = "events-silver-archive"

GOLD_PAGE_VIEWS_BUCKET = "events-gold-page-views"
GOLD_PAGE_VIEWS_ARCHIVE_BUCKET = "events-gold-page-views-archive"

GOLD_PRODUCT_VIEWS_BUCKET = "events-gold-product-events"
GOLD_PRODUCT_VIEWS_ARCHIVE_BUCKET = "events-gold-product-events-archive"


def filter_unprocessed_files(files: List[Dict], layer: str) -> List[Dict]:
    """
    Keep only files that do not already have an archive task.

    Archive tasks are created after a file is successfully processed into the
    next layer. If a second ETL run starts before the archive worker moves the
    object, this check prevents re-processing the same file.
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


def run_bronze_to_silver() -> Dict[str, int]:
    """
    Read all Bronze events from MinIO, transform them to Silver,
    store them in the Silver bucket, and archive processed Bronze files.
    """
    log.info(
        "bronze_to_silver started",
        bronze_bucket=BRONZE_BUCKET,
        silver_bucket=SILVER_BUCKET,
    )

    try:
        bronze_files = get_files_data(BRONZE_BUCKET)
        skipped_files = 0
        bronze_files_to_process = filter_unprocessed_files(
            bronze_files,
            layer="bronze",
        )
        skipped_files = len(bronze_files) - len(bronze_files_to_process)

        log.info(
            "bronze files loaded",
            files_count=len(bronze_files),
            processable_files=len(bronze_files_to_process),
            skipped_files=skipped_files,
        )

        bronze_events: List[BronzeWebEvent] = []
        for f in bronze_files_to_process:
            for row in f["data"]:
                # Each row is a flat Bronze event dict → map it to dataclass.
                bronze_events.append(BronzeWebEvent(**row))

        if not bronze_events:
            # Nothing to process
            log.warning(
                "no bronze events to process",
                files_count=len(bronze_files),
            )
            return {
                "bronze_count": 0,
                "silver_count": 0,
                "skipped_files": skipped_files,
            }

        # Transform Bronze → Silver via helper func.
        silver_events: List[SilverWebEvent] = []
        for bronze in bronze_events:
            silver = bronze_to_silver(bronze)
            silver_events.append(silver)

        log.info(
            "bronze_to_silver transformed",
            bronze_count=len(bronze_events),
            silver_count=len(silver_events),
        )

        # Convert Silver dataclasses to plain dicts for upload
        silver_records: List[Dict] = [asdict(e) for e in silver_events]

        # upload_batch expects List[List[Dict]] → wrap into one batch.
        upload_batch(batch=[silver_records], bucket_name=SILVER_BUCKET)
        log.info(
            "silver parquet uploaded",
            silver_count=len(silver_records),
        )

    except Exception as e:
        log.exception(f"bronze_to_silver failed{e.args}")
        return {"bronze_count": 0, "silver_count": 0}

    # Move processed Bronze files to an archive bucket.
    # This prevents re-processing the same data in the next run
    # move_files_to_another_bucket(BRONZE_BUCKET, BRONZE_ARCHIVE_BUCKET)
    try:
        max_ts = max(e.event_time for e in silver_events)
        update_processing_state(
            dataset="web_events",
            layer="bronze_to_silver",
            last_processed_ts=max_ts,
        )

        for f in bronze_files_to_process:
            bronze_object_name = f.get("object_name")
            enqueue_archive_task(
                dataset="web_events",
                layer="bronze",
                partition_key=bronze_object_name,
                event_type="ARCHIVE",
            )

        log.info(
            "bronze_to_silver side effects done",
            archived_files=len(bronze_files_to_process),
        )
    except Exception as e:
        log.exception(f"bronze_to_silver side effects failed (state/outbox): {e.args}")

    return {
        "bronze_count": len(bronze_events),
        "silver_count": len(silver_events),
        "skipped_files": skipped_files,
    }


def run_silver_to_gold() -> Dict[str, int]:
    """
    Read all Silver events from MinIO, transform them to Gold models,
    store them in two separate Gold buckets, and archive processed
    Silver files.
    """
    log.info(
        "silver_to_gold started",
        silver_bucket=SILVER_BUCKET,
        gold_page_views_bucket=GOLD_PAGE_VIEWS_BUCKET,
        gold_product_events_bucket=GOLD_PRODUCT_VIEWS_BUCKET,
    )

    try:
        silver_files = get_files_data(SILVER_BUCKET)
        skipped_files = 0
        silver_files_to_process = filter_unprocessed_files(
            silver_files,
            layer="silver",
        )
        skipped_files = len(silver_files) - len(silver_files_to_process)

        log.info(
            "silver files loaded",
            files_count=len(silver_files),
            processable_files=len(silver_files_to_process),
            skipped_files=skipped_files,
        )

        silver_events: List[SilverWebEvent] = []
        for f in silver_files_to_process:
            for row in f["data"]:
                # Each row is a flat Silver event dict → map it to dataclass.
                silver_events.append(SilverWebEvent(**row))

        if not silver_events:
            # Nothing to process
            log.warning(
                "no silver events to process",
                files_count=len(silver_files),
            )
            return {
                "silver_count": 0,
                "gold_page_views_count": 0,
                "gold_product_events_count": 0,
                "skipped_files": skipped_files,
            }

        # Transform Silver → Gold models.
        page_views: List[GoldPageView] = []
        product_events: List[GoldProductEvent] = []

        for silver in silver_events:
            # Build page-view style gold record (if applicable).
            pv = silver_to_gold_page_view(silver)
            if pv is not None:
                page_views.append(pv)

            # Build product-event style gold record (if applicable).
            pe = silver_to_gold_product(silver)
            if pe is not None:
                product_events.append(pe)

        log.info(
            "silver_to_gold transformed",
            silver_count=len(silver_events),
            gold_page_views_count=len(page_views),
            gold_product_events_count=len(product_events),
        )

        # Convert Gold dataclasses to plain dicts for upload.
        page_view_records: List[Dict] = [asdict(e) for e in page_views]
        product_event_records: List[Dict] = [asdict(e) for e in product_events]



        # Upload Gold batches to their buckets (if there is any data).
        # upload_batch expects List[List[Dict]] → wrap into one batch.
        if page_view_records:
            upload_batch(batch=[page_view_records], bucket_name=GOLD_PAGE_VIEWS_BUCKET)

        if product_event_records:
            upload_batch(batch=[product_event_records], bucket_name=GOLD_PRODUCT_VIEWS_BUCKET)

        log.info(
            "gold parquet uploaded",
            gold_page_views_count=len(page_view_records),
            gold_product_events_count=len(product_event_records),
        )

    except Exception as e:
        log.exception(f"silver_to_gold failed: {e.args}")
        return {"bronze_count": 0, "silver_count": 0}

    # Move processed Silver files to an archive bucket.
    # This prevents re-processing the same data in the next run.
    # move_files_to_another_bucket(SILVER_BUCKET, SILVER_ARCHIVE_BUCKET)

    try:
        max_ts = max(e.event_time for e in silver_events)
        update_processing_state(
            dataset="web_events",
            layer="silver_to_gold",
            last_processed_ts=max_ts,
        )

        for f in silver_files_to_process:
            silver_object_name = f.get("object_name")
            enqueue_archive_task(
                dataset="web_events",
                layer="silver",
                partition_key=silver_object_name,
                event_type="ARCHIVE",
            )

        log.info(
            "silver_to_gold side effects done",
            archived_files=len(silver_files_to_process),
        )

    except Exception as e:
        log.exception(f"silver_to_gold side effects failed (state/outbox): {e.args}")


    return {
        "silver_count": len(silver_events),
        "gold_page_views_count": len(page_views),
        "gold_product_events_count": len(product_events),
        "skipped_files": skipped_files,
    }


def get_medallion_stats() -> Dict[str, int]:
    """
    Helper to collect basic Medallion metrics from MinIO.

    It does NOT change any state and does NOT move files.
    It only reads current parquet data from Bronze/Silver/Gold buckets
    and counts rows & files.

    This is safe to call from the FastAPI dashboard.
    """
    # Bronze
    bronze_files = get_files_data(BRONZE_BUCKET)
    bronze_files_count = len(bronze_files)
    bronze_rows_count = sum(len(f["data"]) for f in bronze_files)

    # Silver
    silver_files = get_files_data(SILVER_BUCKET)
    silver_files_count = len(silver_files)
    silver_rows_count = sum(len(f["data"]) for f in silver_files)

    # Gold: page views
    gold_pv_files = get_files_data(GOLD_PAGE_VIEWS_BUCKET)
    gold_pv_files_count = len(gold_pv_files)
    gold_pv_rows_count = sum(len(f["data"]) for f in gold_pv_files)

    # Gold: product events
    gold_pe_files = get_files_data(GOLD_PRODUCT_VIEWS_BUCKET)
    gold_pe_files_count = len(gold_pe_files)
    gold_pe_rows_count = sum(len(f["data"]) for f in gold_pe_files)

    # Gold-rows count
    gold_total_rows = gold_pv_rows_count + gold_pe_rows_count
    gold_total_files = gold_pv_files_count + gold_pe_files_count

    return {
        "bronze_files": bronze_files_count,
        "bronze_rows": bronze_rows_count,
        "silver_files": silver_files_count,
        "silver_rows": silver_rows_count,
        "gold_files": gold_total_files,
        "gold_rows": gold_total_rows,
        "gold_page_view_files": gold_pv_files_count,
        "gold_page_view_rows": gold_pv_rows_count,
        "gold_product_event_files": gold_pe_files_count,
        "gold_product_event_rows": gold_pe_rows_count,
    }
