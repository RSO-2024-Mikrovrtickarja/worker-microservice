from abc import ABCMeta, abstractmethod
from pathlib import Path
import boto3
from typing import Annotated, BinaryIO
from fastapi import Depends
from mypy_boto3_s3 import S3Client

from .configuration import settings


class BaseFileStorage(metaclass=ABCMeta):
    @abstractmethod
    def download_file(self, name: str, writable: BinaryIO):
        return NotImplemented

    @abstractmethod
    def upload_file(self, name: str, readable: BinaryIO):
        return NotImplemented

    @abstractmethod
    def delete_file(self, name: str):
        return NotImplemented


class LocalFileSystemStorage(BaseFileStorage):
    _base_directory: Path

    def __init__(self):
        super().__init__()

        if settings.local_filesystem_base_directory is None:
            raise RuntimeError(
                "Invalid configuration: storage backend is set to local, \
                 but local_filesystem_base_directory is unset"
            )

        self._base_directory = Path(settings.local_filesystem_base_directory).resolve()

    def download_file(self, file_name: str, writable: BinaryIO):
        file_name_only = Path(file_name).name
        full_file_path: Path = self._base_directory / file_name_only

        with full_file_path.open("rb") as file:
            writable.write(file.read())

    def upload_file(self, file_name: str, readable: BinaryIO) -> str:
        file_name_only = Path(file_name).name
        full_file_path: Path = self._base_directory / file_name_only

        with full_file_path.open("wb") as file:
            file.write(readable.read())

        return full_file_path.name

    def delete_file(self, file_name: str):
        file_name_only = Path(file_name).name
        full_file_path: Path = self._base_directory / file_name_only

        full_file_path.unlink(missing_ok=False)


class S3Storage(BaseFileStorage):
    _s3_client: S3Client

    def __init__(self):
        super().__init__()

        if settings.s3_service_url is None:
            raise RuntimeError(
                "Invalid configuration: storage backend is set to s3, \
                 but s3_service_url is unset"
            )

        if settings.s3_bucket is None:
            raise RuntimeError(
                "Invalid configuration: storage backend is set to s3, \
                 but s3_bucket is unset"
            )

        self._s3_client = boto3.client(
            "s3", use_ssl=True, verify=True, endpoint_url=settings.s3_service_url
        )

    def download_file(self, object_name: str, writable: BinaryIO):
        self._s3_client.download_file(settings.s3_bucket, object_name, writable)

    def upload_file(self, object_id: str, readable: BinaryIO) -> str:
        self._s3_client.upload_file(readable, settings.s3_bucket, object_id)

        return object_id

    def delete_file(self, object_id: str):
        self._s3_client.delete_object(Bucket=settings.s3_bucket, Key=object_id)


def get_storage_instance() -> BaseFileStorage:
    storage_backend: str = settings.storage_backend.lower()

    if storage_backend == "local":
        return LocalFileSystemStorage()
    elif storage_backend == "s3":
        return S3Storage()
    else:
        raise RuntimeError(
            'Invalid storage_backend configuration value: \
             expected either "local" or "s3".'
        )


StorageDependency = Annotated[BaseFileStorage, Depends(get_storage_instance)]
