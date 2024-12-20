from PIL import Image, ImageOps
import zmq
from configuration import settings
from pydantic import BaseModel
from typing import Optional
import uuid
import requests
from contextlib import asynccontextmanager
from fastapi import FastAPI
from enum import Enum
from tempfile import SpooledTemporaryFile
import os
import json
import io


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

    # These are the actual processing parameters.
    resize_image_to_width: int
    resize_image_to_height: int
    change_to_format: str


class PublicImageProcessingJobUpdateRequest(BaseModel):
    destination_image_id: Optional[uuid.UUID]
    status: Optional[str]


class ProcessImageJob:
    _zmq_context: zmq.Context
    _zmq_socket: zmq.Socket

    def __init__(self):
        self._zmq_context = zmq.Context()

        self._zmq_socket = self._zmq_context.socket(zmq.REP)
        self._zmq_socket.bind(f"tcp://{settings.zmq_host}:{settings.zmq_port}")

    def transform_image(
        self,
        image_buffer,
        output_image_buffer,
        img_fpath: str,
        resize_width: int,
        resize_height: int,
        new_img_format: str,
    ):
        # ) -> SpooledTemporaryFile:
        try:
            # storage = SessionDependency()
            # intermediate_image_buffer = SpooledTemporaryFile(mode="w+b")
            # storage.download_file(img_fpath, intermediate_image_buffer)
            # intermediate_image_buffer.seek(0, os.SEEK_SET)

            with Image.open(image_buffer) as img_obj:
                # old_img_format = img_obj.format
                new_img_obj = img_obj.resize((resize_width, resize_height))
                # new_img_obj = ImageOps.scale(img_obj, scale_factor, resample_method)

                new_img_obj.save(output_image_buffer, new_img_format)
                # new_img_obj.save("./testimage.jpeg", new_img_format)
                output_image_buffer.seek(0, os.SEEK_SET)

            # storage.upload_file(img_fpath, output_image_buffer)
            # return output_image_buffer

        except FileNotFoundError as err:
            raise


process_image_job = ProcessImageJob()

if __name__ == "__main__":
    # process_image_job = ProcessImageJob()

    while True:
        try:
            received_job_bytes = process_image_job._zmq_socket.recv()
            print("Received job.")
            job_conformation = InternalImageProcessingJobConfirmation(is_ok=True)
        except Exception as err:
            job_conformation = InternalImageProcessingJobConfirmation(is_ok=False)
            raise
        finally:
            process_image_job._zmq_socket.send(
                (job_conformation.model_dump_json()).encode("utf-8")
            )

        received_job = InternalImageProcessingJob.model_validate_json(
            received_job_bytes
        )

        host = settings.photo_storage_host
        port = settings.photo_storage_port
        job_id = received_job.job_id

        download_specific_image_url = (
            f"http://{host}:{port}/worker/jobs/{job_id}/download-source-image"
        )
        download_specific_image_get_response = requests.get(
            download_specific_image_url, stream=True
        )

        if download_specific_image_get_response.status_code != 200:
            raise Exception("Failed to download specific image.")

        filename = (
            download_specific_image_get_response.headers.get("Content-Disposition", "")
            .split("filename=")[1]
            .strip('"')
        )

        image_buffer = SpooledTemporaryFile(mode="wb")
        image_buffer.write(download_specific_image_get_response.content)

        output_image_buffer = SpooledTemporaryFile(mode="w+b")

        process_image_job.transform_image(
            image_buffer,
            output_image_buffer,
            received_job.image_path,
            received_job.resize_image_to_width,
            received_job.resize_image_to_height,
            received_job.change_to_format,
        )

        upload_processes_image_url = (
            f"http://{host}:{port}/worker/jobs/{job_id}/finalize"
        )
        # upload_image = {"filename": filename, "file": output_image_buffer}
        upload_image = {"file": (filename, output_image_buffer)}
        # with open(filename, "rb") as file_to_send:
        # upload_image = {"file": file_to_send}
        upload_processes_image_post_response = requests.post(
            upload_processes_image_url, files=upload_image
        )

        if upload_processes_image_post_response.status_code != 200:
            print(upload_processes_image_post_response.text)
            raise Exception("Failed to upload processesed image.")

        job_update_patch_obj = PublicImageProcessingJobUpdateRequest(
            status=ImageProcessingJobStatus.SUCCESS.value,
        )

        job_update_patch_url = f"http://{host}:{port}/worker/jobs/{job_id}"

        job_update_patch_response = requests.patch(
            job_update_patch_url, json=job_update_patch_obj.model_validate_json()
        )
        if job_update_patch_response.status_code != 200:
            raise Exception("Failed to send update_job_status to storage.")
