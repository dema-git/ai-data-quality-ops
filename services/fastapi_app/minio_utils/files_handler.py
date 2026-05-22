##############################################################################
# files_handler.py
#
# High-level helpers over MinIO for this project:
# - upload_batch(...)  → write events as Parquet
# - get_files_data(...) → read Parquet into Python dicts
# - delete_all_objects(...) → clean bucket
##############################################################################

import os
from io import BytesIO
from typing import List, Dict
import pandas as pd

from .minio_client import get_minio_manager
from .batch_uploader import BatchUploader
from exceptions_logging.custom_exceptions import MinIOException
from exceptions_logging.logger import AppLogger

log = AppLogger(component="files_handler")

manager = get_minio_manager()

def upload_batch(batch: List[List[Dict]], bucket_name="active-bucket") -> None:
    """
    Helper to upload a batch of records to a MinIO bucket
    as a single Parquet file.
    """
    if not batch:
        log.warning("upload_batch called with empty batch", bucket_name=bucket_name)
        return

    uploader = BatchUploader(manager, bucket_name)

    log.info(
        "upload_batch started",
        bucket_name=bucket_name,
        outer_batches=len(batch),
    )

    try:
        uploader.upload_batch(batch)
        log.info("upload_batch done", bucket_name=bucket_name)
    except Exception as e:
        log.exception(f"upload_batch failed: {e.args}", bucket_name=bucket_name)
        raise


def get_all_files_from_bucket(bucket_name: str) -> Dict[str, bytes]:
    """
    Return a dictionary where keys are object names and values
    are raw file contents as bytes.
    """
    return manager.download_all_objects(bucket_name)


def get_files_data(bucket_name: str) -> List[Dict]:
    """
    Download all Parquet files from the specified bucket, read them via pandas
    and return a list of dicts:
    {
        "object_name": "YYYY/MM/DD/events_....parquet",
        "filename":    "events_....parquet",
        "data":        [ {column: value, ...}, ... ]
    }
    """
    log.info("get_files_data started", bucket_name=bucket_name)

    files = get_all_files_from_bucket(bucket_name)
    arr = []
    for object_name, content in files.items():
        try:
            df = pd.read_parquet(BytesIO(content))

            arr.append({
                "object_name": object_name,
                "filename": os.path.basename(object_name),
                "data": df.to_dict(orient="records"),
            })

            log.info(
                "parquet file parsed",
                bucket_name=bucket_name,
                object_name=object_name,
            )
        except Exception as e:
            log.exception(
                f"failed to read parquet file: {e.args}",
                bucket_name=bucket_name,
                object_name=object_name,
            )
            raise MinIOException(
                f"Failed to read parquet file '{object_name}' from bucket '{bucket_name}'"
            )


    log.info(
        "get_files_data done",
        bucket_name=bucket_name,
        files_count=len(arr),
    )
    return arr


def delete_all_objects(bucket_name: str) -> None:
    """
    Delete all objects from a bucket.
    """
    log.warning("delete_all_objects called", bucket_name=bucket_name)

    try:
        manager.delete_all_objects(bucket_name)
        log.info("delete_all_objects done", bucket_name=bucket_name)

    except Exception as e:
        log.exception(f"delete_all_objects failed: {e.args}", bucket_name=bucket_name)
        raise