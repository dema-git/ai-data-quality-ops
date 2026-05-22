#####################################################################
# tests/test_minio_client.py
#
# These tests verify the core behavior of MinioManager without using
# a real MinIO server. A lightweight DummyMinioClient is injected instead.
#
# The tests confirm that:
# - buckets are created only when missing
# - deleting all objects returns a proper summary
# - moving an object calls copy + remove in the right order
# - uploading a file calls put_object with correct arguments
#
# The goal is to ensure MinioManager’s logic works independently from
# the real MinIO SDK and network interactions.
#####################################################################

import pytest
from services.fastapi_app.minio_utils import minio_client as minio_client_mod


class DummyMinioClient:
    """
    Minimal in-memory stub for the real MinIO client.
    Tracks method calls and simulates:
    - bucket existence checks
    - bucket creation
    - object listings
    - object removal
    - object copying
    - file upload
    """

    def __init__(self, host, access_key=None, secret_key=None, secure=False):
        self.host = host
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure

        self.bucket_exists_calls = []
        self.make_bucket_calls = []
        self.list_objects_calls = []
        self.remove_objects_calls = []
        self.copy_object_calls = []
        self.put_object_calls = []

        self._existing_buckets = set()
        self._objects_by_bucket = {}

    # helper methods for configuring stub behavior
    def set_bucket_exists(self, bucket_name: str, exists: bool):
        if exists:
            self._existing_buckets.add(bucket_name)
        else:
            self._existing_buckets.discard(bucket_name)

    def set_objects(self, bucket_name: str, object_names):
        self._objects_by_bucket[bucket_name] = list(object_names)

    # methods mimicking real MinIO behavior
    def bucket_exists(self, bucket_name: str) -> bool:
        self.bucket_exists_calls.append(bucket_name)
        return bucket_name in self._existing_buckets

    def make_bucket(self, bucket_name: str):
        self.make_bucket_calls.append(bucket_name)
        self._existing_buckets.add(bucket_name)

    def list_objects(self, bucket_name: str, recursive: bool = True):
        self.list_objects_calls.append((bucket_name, recursive))

        object_names = self._objects_by_bucket.get(bucket_name, [])
        class Obj:
            def __init__(self, name):
                self.object_name = name

        for name in object_names:
            yield Obj(name)

    def get_object(self, bucket_name: str, object_name: str):
        raise NotImplementedError

    def remove_objects(self, bucket_name: str, delete_list):
        self.remove_objects_calls.append((bucket_name, list(delete_list)))
        return iter([])

    def remove_object(self, bucket_name: str, object_name: str):
        self.remove_objects_calls.append((bucket_name, object_name))

    def copy_object(self, bucket_name: str, object_name: str, source):
        self.copy_object_calls.append((bucket_name, object_name, source))

    def put_object(self, bucket_name: str, object_name: str, data, length: int, content_type: str):
        self.put_object_calls.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "length": length,
                "content_type": content_type,
            }
        )


def _make_manager_with_dummy(monkeypatch) -> tuple[minio_client_mod.MinioManager, DummyMinioClient]:
    """
    Creates MinioManager with DummyMinioClient injected
    instead of the real Minio SDK client.
    """
    dummy = None

    def dummy_minio_ctor(host, access_key=None, secret_key=None, secure=False):
        nonlocal dummy
        dummy = DummyMinioClient(host, access_key, secret_key, secure)
        return dummy

    monkeypatch.setattr(minio_client_mod, "Minio", dummy_minio_ctor)

    cfg = minio_client_mod.MinioConfig(
        host="minio:9010",
        access_key="user",
        secret_key="pass",
        secure=False,
    )
    manager = minio_client_mod.MinioManager(cfg)

    assert dummy is not None
    return manager, dummy


def test_ensure_bucket_creates_when_missing(monkeypatch):
    """
    ensure_bucket() creates the bucket if it does NOT exist.
    """
    manager, dummy = _make_manager_with_dummy(monkeypatch)

    dummy.set_bucket_exists("test-bucket", False)

    manager.ensure_bucket("test-bucket")

    assert dummy.bucket_exists_calls == ["test-bucket"]
    assert dummy.make_bucket_calls == ["test-bucket"]


def test_ensure_bucket_does_not_create_when_exists(monkeypatch):
    """
    ensure_bucket() does NOT call make_bucket when bucket already exists
    """
    manager, dummy = _make_manager_with_dummy(monkeypatch)

    dummy.set_bucket_exists("existing-bucket", True)

    manager.ensure_bucket("existing-bucket")

    assert dummy.bucket_exists_calls == ["existing-bucket"]
    assert dummy.make_bucket_calls == []


def test_delete_all_objects_empty_bucket_returns_zeroes(monkeypatch):
    """
    delete_all_objects() returns zero counters for an empty bucket
    """
    manager, dummy = _make_manager_with_dummy(monkeypatch)

    dummy.set_objects("empty-bucket", [])

    result = manager.delete_all_objects("empty-bucket")

    assert result["bucket"] == "empty-bucket"
    assert result["deleted"] == 0
    assert result["errors"] == 0


def test_delete_all_objects_non_empty_bucket_counts_deleted(monkeypatch):
    """
    delete_all_objects() returns correct count for a non-empty bucket.
    """
    manager, dummy = _make_manager_with_dummy(monkeypatch)

    dummy.set_objects("data-bucket", ["a.parquet", "b.parquet", "c.parquet"])

    result = manager.delete_all_objects("data-bucket")

    assert result["bucket"] == "data-bucket"
    assert result["deleted"] == 3
    assert result["errors"] == 0
    assert len(dummy.remove_objects_calls) == 1
    bucket_name, delete_list = dummy.remove_objects_calls[0]
    assert bucket_name == "data-bucket"
    assert len(delete_list) == 3


def test_move_single_object_calls_copy_and_remove(monkeypatch):
    """
    move_single_object() must call copy_object() then remove_object()
    """
    manager, dummy = _make_manager_with_dummy(monkeypatch)

    dummy.set_bucket_exists("target-bucket", True)

    manager.move_single_object("source-bucket", "target-bucket", "path/obj.parquet")

    assert "target-bucket" in dummy.bucket_exists_calls

    assert len(dummy.copy_object_calls) == 1
    assert len(dummy.remove_objects_calls) == 1

    copy_bucket, copy_object_name, source = dummy.copy_object_calls[0]
    assert copy_bucket == "target-bucket"
    assert copy_object_name == "path/obj.parquet"

    remove_bucket, remove_object_name = dummy.remove_objects_calls[0]
    assert remove_bucket == "source-bucket"
    assert remove_object_name == "path/obj.parquet"


def test_upload_file_calls_put_object_with_expected_args(monkeypatch, tmp_path):
    """
    upload_file() passes correct metadata to put_object().
    """
    manager, dummy = _make_manager_with_dummy(monkeypatch)

    tmp_file = tmp_path / "test-file.bin"
    content = b"1234567890"
    tmp_file.write_bytes(content)

    manager.upload_file("bucket-x", "obj/name.bin", str(tmp_file))

    assert len(dummy.put_object_calls) == 1
    call = dummy.put_object_calls[0]

    assert call["bucket_name"] == "bucket-x"
    assert call["object_name"] == "obj/name.bin"
    assert call["length"] == len(content)
    assert call["content_type"] == "application/octet-stream"