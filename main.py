from PIL import Image, ImageOps
import zmq
from configuration import settings
from pydantic import BaseModel
from typing import Optional
import uuid
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI
from database import SessionDependency, create_db_and_tables
from enum import Enum
from tempfile import SpooledTemporaryFile
import os


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Creating database and tables...")
    create_db_and_tables()

    yield


app = FastAPI(lifespan=lifespan)


class ImageFormat(Enum):
    BLP = "BLP"
    BMP = "BMP"
    BUFR = "BUFR"
    DDS = "DDS"
    DIP = "DIB"
    EPS = "EPS"
    GIF = "GIF"
    GRIB = "GRIB"
    HDF5 = "HDF5"
    ICNS = "ICNS"
    ICO = "ICO"
    IM = "IM"
    JPEG = "JPEG"
    JPEG2000 = "JPEG2000"
    MPS = "MPS"
    PCX = "PCX"
    PNG = "PNG"
    PPM = "PPM"
    SGI = "SGI"
    SPIDER = "SPIDER"
    TGA = "TGA"
    TIFF = "TIFF"
    WEBP = "WEBP"
    WMF = "WMF"


class ImageProcessingJobStatus(Enum):
    SUCCESS = "success"
    ERROR = "error"


class InternalImageProcessingJobConfirmation(BaseModel):
    is_ok: bool


class InternalImageProcessingJob(BaseModel):
    job_id: uuid.UUID

    # This represents an image file path or S3 object name,
    # depending on which storage backend is configured.
    image_path: str
    image_id: uuid.UUID

    # These are the actual processing parameters.
    resize_image_to_width: int
    resize_image_to_height: int
    change_to_format: ImageFormat


class PublicImageProcessingJobUpdateRequest(BaseModel):
    destination_image_id: Optional[uuid.UUID]
    status: Optional[str]


class ProcessImageJob:
    _zmq_context: zmq.Context
    _zmq_socket: zmq.Socket

    def __init__(self):
        self._zmq_context = zmq.Context()

        self._zmq_socket = self._zmq_context.socket(zmq.REP)
        self._zmq_socket.connect(f"tcp://{settings.zmq_host}:{settings.zmq_port}")

        while True:
            try:
                received_job_json = self._zmq_socket.recv()
                job_conformation = InternalImageProcessingJobConfirmation(is_ok=True)
            except Exception as err:
                job_conformation = InternalImageProcessingJobConfirmation(is_ok=False)
                raise
            finally:
                self._zmq_socket.send(job_conformation.model_dump_json())

            received_job = InternalImageProcessingJob.model_validate_json(
                received_job_json
            )

            self.transform_image(
                received_job.image_path,
                received_job.image_path,
                received_job.resize_image_to_width,
                received_job.resize_image_to_height,
                received_job.change_to_format,
            )

            patch_obj = PublicImageProcessingJobUpdateRequest(
                destination_image_id=uuid.uuid4,
                status=ImageProcessingJobStatus.SUCCESS,
            )

            host = settings.photo_storage_host
            port = settings.photo_storage_port
            image_id = received_job.image_id
            job_id = received_job.job_id

            patch_url = f"http://{host}:{port}/images/{image_id}/jobs/{job_id}"

            patch_response = requests.patch(
                patch_url, json=patch_obj.model_validate_json()
            )
            if patch_response.status_code != 200:
                print("failure")

    def transform_image(
        img_fpath: str,
        new_img_fpath: str,
        resize_width: int,
        resize_height: int,
        new_img_format: ImageFormat,
    ):
        try:
            storage = SessionDependency()
            intermediate_image_buffer = SpooledTemporaryFile(mode="w+b")
            storage.download_file(img_fpath, intermediate_image_buffer)
            intermediate_image_buffer.seek(0, os.SEEK_SET)

            with Image.open(intermediate_image_buffer) as img_obj:
                # old_img_format = img_obj.format
                new_img_obj = img_obj.resize((resize_width, resize_height))
                # new_img_obj = ImageOps.scale(img_obj, scale_factor, resample_method)

                output_image_buffer = SpooledTemporaryFile(mode="w+b")
                new_img_obj.save(output_image_buffer, new_img_format.value)
                output_image_buffer.seek(0, os.SEEK_SET)

            storage.upload_file(new_img_fpath, output_image_buffer)

        except FileNotFoundError as err:
            raise


if __name__ == "__main__":
    process_image_job = ProcessImageJob()
#    uvicorn.run(users_app, host="0.0.0.0", port=8002)
