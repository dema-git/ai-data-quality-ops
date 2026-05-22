#################################################
# bronze_writer.py
#
# Background task that runs on FastAPI startup.
# Writes Bronze layer events to MinIO storage.
# Takes a list of BronzeWebEvent objects and saves
# them as a single parquet file in the bronze bucket.
#
##################################################
from dataclasses import asdict
from typing import List

from services.medallion_models.bronze_model import BronzeWebEvent
from minio_utils.files_handler import upload_batch



def save_bronze_events(events: List[BronzeWebEvent]) -> None:
    """
    Save Bronze events to MinIO as one batch (1 parquet = 1 batch)
    """
    if not events:
        return

    # convert dataclasses -> dict
    records = [asdict(e) for e in events]

    # upload_batch expects: List[List[Dict]]
    upload_batch(batch=[records], bucket_name="events-bronze")