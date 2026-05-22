##################################################################################
# minio_client.py
#
# It contains basic MinIO operations such as:
# - creating and checking buckets
# - uploading objects
# - downloading objects
# - moving objects between buckets
# - deleting objects
###################################################################################

from dataclasses import dataclass
from typing import Dict, Any

from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
import os
from minio.deleteobjects import DeleteObject
from exceptions_logging.logger import AppLogger
from urllib.parse import quote_plus


log = AppLogger(component="minio_manager")

ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")


@dataclass
class MinioConfig:
    """
    MinIO connection configuration.
    All required parameters to initialize a MinIO client instance.
    """
    host: str = "minio:9010"
    access_key: str = ACCESS_KEY
    secret_key: str = SECRET_KEY
    secure: bool = False


@dataclass
class MinioManager:
    """
    MinIO client wrapper.

    Provides high-level methods for managing buckets and objects
    using an initialized MinIO client.
    """
    config: MinioConfig


    def __post_init__(self):
        # Initialize MinIO client
        try:
            self.client = Minio(
                self.config.host,
                access_key=self.config.access_key,
                secret_key=self.config.secret_key,
                secure=self.config.secure
            )
            log.info(
                "minio client initialized",
                host=self.config.host,
                secure=self.config.secure,
            )
        except Exception:
            log.exception("failed to initialize minio client")
            raise


    def ensure_bucket(self, bucket_name: str):
        """
        Ensure that the given bucket exists. Create it if needed.
        """
        try:
            if not self.client.bucket_exists(bucket_name):
                log.info("creating bucket", bucket_name=bucket_name)
                self.client.make_bucket(bucket_name)
            else:
                log.debug("bucket already exists", bucket_name=bucket_name)

        except Exception:
            log.exception("ensure_bucket failed", bucket_name=bucket_name)
            raise


    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        """
        Upload a local file to the given bucket under the given object_name.
        """
        try:
            with open(file_path, "rb") as f:
                self.client.put_object(
                    bucket_name=bucket_name,
                    object_name=object_name,
                    data=f,
                    length=os.path.getsize(file_path),
                    content_type="application/octet-stream"
                )
        except Exception as e:
            log.exception(
                f"upload_file failed: {e.args}",
                bucket_name=bucket_name,
                object_name=object_name,
            )
            raise


    def download_all_objects(self, bucket_name: str, download_path: str = None) -> dict:
        """
        Download all objects from a bucket.

        If download_path is provided:
            Files are written to disk.
        Otherwise:
            Raw bytes are returned in a dictionary.
        """
        objects_data = {}

        try:
            log.info("download_all_objects started", bucket_name=bucket_name)

            objects = list(self.client.list_objects(bucket_name, recursive=True))

            if not objects:
                log.warning("bucket is empty", bucket_name=bucket_name)

            for obj in self.client.list_objects(bucket_name, recursive=True):
                response = None
                try:
                    response = self.client.get_object(bucket_name, obj.object_name)
                    if download_path:
                        file_path = os.path.join(download_path, obj.object_name)
                        os.makedirs(os.path.dirname(file_path), exist_ok=True)
                        with open(file_path, "wb") as f:
                            for chunk in response.stream(32 * 1024):
                                f.write(chunk)
                        log.debug(
                            "object downloaded to disk",
                            bucket_name=bucket_name,
                            object_name=obj.object_name,
                        )
                    else:
                        objects_data[obj.object_name] = response.read()
                finally:
                    if response:
                        response.close()

            log.info(
                "download_all_objects done",
                bucket_name=bucket_name,
                objects_count=len(objects),
            )
            return objects_data

        except Exception:
            log.exception("download_all_objects failed", bucket_name=bucket_name)
            raise


    def delete_all_objects(self, bucket_name: str) -> Dict[str, Any]:
        """
        Delete all objects from the given bucket.
        Returns summary with deleted count and error count.
        """

        objects = self.client.list_objects(bucket_name, recursive=True)

        delete_list = [DeleteObject(obj.object_name) for obj in objects]

        if not delete_list:
            return {
                "bucket": bucket_name,
                "deleted": 0,
                "errors": 0,
            }

        errors = 0
        try:
            for err in self.client.remove_objects(bucket_name, delete_list):
                errors += 1

        except S3Error as e:
            raise

        return {
            "bucket": bucket_name,
            "deleted": len(delete_list),
            "errors": errors,
        }


    def move_single_object(self, source_bucket: str, target_bucket: str, object_name: str) -> None:
        """
        Move a single object from one MinIO bucket to another.

        Steps:
        1. Ensure the target bucket exists (create if needed).
        2. Copy the object from source bucket to target bucket.
        3. Remove the object from the source bucket.
        """

        try:
            log.info(
                "move_single_object started",
                source_bucket=source_bucket,
                target_bucket=target_bucket,
                object_name=object_name,
            )
            self.ensure_bucket(target_bucket)
            src = CopySource(source_bucket, object_name)
            self.client.copy_object(
                bucket_name=target_bucket,
                object_name=object_name,
                source=src,
            )
            self.client.remove_object(source_bucket, object_name)
            log.info(
                "move_single_object done",
                source_bucket=source_bucket,
                target_bucket=target_bucket,
                object_name=object_name,
            )
        except Exception as e:
            log.exception(
                f"move_single_object failed: {e.args}",
                source_bucket=source_bucket,
                target_bucket=target_bucket,
                object_name=object_name,
            )
            raise


def get_minio_manager(config: MinioConfig = None) -> MinioManager:
    """
    Factory to create a MinioManager instance.
    """
    if config is None:
        config = MinioConfig()
    return MinioManager(config)