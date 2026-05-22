########################################################################
# batch_uploader.py
#
# This module defines a BatchUploader class responsible for transforming
# incoming batched event data into a tabular format and uploading it to
# MinIO as Parquet files.
########################################################################

from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd
import tempfile
import os
from exceptions_logging.custom_exceptions import MinIOException
from .minio_client import MinioManager
from exceptions_logging.logger import AppLogger


log = AppLogger(component="batch_uploader")

@dataclass
class BatchUploader:
    """
    Helper class to upload batched events into MinIO as Parquet files.
    """
    minio_manager: MinioManager
    bucket_name: str

    def __post_init__(self):
        # make sure bucket exists
        try:
            log.info("ensure_bucket started", bucket_name=self.bucket_name)
            self.minio_manager.ensure_bucket(self.bucket_name)
            log.info("ensure_bucket done", bucket_name=self.bucket_name)
        except Exception as e:
            log.exception("ensure_bucket failed", bucket_name=self.bucket_name)
            raise

    @staticmethod
    def build_dated_object_name(base_prefix: str = "events") -> str:
        """
        Build object name with date-based prefix:
        <base_prefix>/YYYY/MM/DD/events_YYYYMMDD_HHMMSS_micro.parquet

        Example:
            2026/02/05/events_20260205_192555_376174.parquet
        """
        now = datetime.now()
        date_prefix = now.strftime("%Y/%m/%d")
        file_name = now.strftime(f"{base_prefix}_%Y%m%d_%H%M%S_%f.parquet")
        return f"{date_prefix}/{file_name}"


    @staticmethod
    def transform_batch_to_rows(batch: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Flatten nested batch [[dict, dict...], [...]] -> [dict, dict...]
        """
        rows: List[Dict[str, Any]] = []
        for records in batch:
            for record in records:
                rows.append(record)
        return rows

    def upload_batch(self, batch: List[List[Dict[str, Any]]]) -> None:
        """
        - flatten batch
        - make pandas DataFrame
        - write to Parquet temporary file
        - upload file to MinIO
        """
        if not batch or not batch[0]:
            log.warning("empty batch received, skipping upload",
                        bucket_name=self.bucket_name)
            return

        rows = self.transform_batch_to_rows(batch)
        if not rows:
            log.warning("batch has zero records, skipping upload",
                        bucket_name=self.bucket_name)
            return

        parquet_file = None

        try:
            log.info(
                "upload_batch started",
                bucket_name=self.bucket_name,
                batches_count=len(batch),
            )

            df = pd.DataFrame(rows)

            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmp:
                parquet_file = tmp.name

                # parquet writer via pandas
                df.to_parquet(parquet_file, index=False)

            object_name = self.build_dated_object_name(base_prefix="events")
            log.info(
                "uploading parquet to minio",
                bucket_name=self.bucket_name,
                object_name=object_name,
                rows_count=len(df),
                parquet_file_size_bytes=os.path.getsize(parquet_file) if parquet_file else None,
            )
            self.minio_manager.upload_file(self.bucket_name, object_name, parquet_file)
            log.info(
                "upload_batch done",
                bucket_name=self.bucket_name,
                object_name=object_name,
                rows_count=len(df),
            )


        except Exception as e:
            log.exception(
                f"failed to upload batch to minio: {e.args}",
                bucket_name=self.bucket_name,
            )
            raise MinIOException("Failed to upload batch to MinIO") from e

        finally:
            try:
                os.remove(parquet_file)
                log.debug("temp parquet file deleted", path=parquet_file)
            except Exception as e:
                log.warning(f"failed to delete temp parquet file: {e.args}", path=parquet_file)
