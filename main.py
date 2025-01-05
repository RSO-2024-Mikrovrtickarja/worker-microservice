import os
import signal
import threading
import uuid
from datetime import datetime
from enum import Enum
from tempfile import SpooledTemporaryFile
from typing import Optional

import pika
import requests
import uvicorn
from PIL import Image
from fastapi import FastAPI, Response
from pika.adapters.blocking_connection import BlockingChannel
from pydantic import BaseModel

from worker.configuration import settings

app = FastAPI()


@app.get("/health")
def health_check():
    return Response(status_code=200)


# class ImageFormat(Enum):
#     BLP = "BLP"
#     BMP = "BMP"
#     BUFR = "BUFR"
#     DDS = "DDS"
#     DIP = "DIB"
#     EPS = "EPS"
#     GIF = "GIF"
#     GRIB = "GRIB"
#     HDF5 = "HDF5"
#     ICNS = "ICNS"
#     ICO = "ICO"
#     IM = "IM"
#     JPEG = "JPEG"
#     JPEG2000 = "JPEG2000"
#     MPS = "MPS"
#     PCX = "PCX"
#     PNG = "PNG"
#     PPM = "PPM"
#     SGI = "SGI"
#     SPIDER = "SPIDER"
#     TGA = "TGA"
#     TIFF = "TIFF"
#     WEBP = "WEBP"
#     WMF = "WMF"


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


class PublicImage(BaseModel):
    id: uuid.UUID
    file_name: str
    uploaded_at: datetime


class PublicSingleImageResponse(BaseModel):
    image: PublicImage


def transform_image(
    image_buffer,
    output_image_buffer,
    resize_width: int,
    resize_height: int,
    new_img_format: str,
):
    with Image.open(image_buffer) as img_obj:
        new_img_obj = img_obj.resize((resize_width, resize_height))

        # This forces RGB mode (e.g. PNG images can have RGBA, and saving them as RGBA would fail
        # because, for example, JPEG does not support transparency)
        new_img_obj = new_img_obj.convert("RGB")

        new_img_obj.save(output_image_buffer, new_img_format)
        output_image_buffer.seek(0, os.SEEK_SET)


def handle_incoming_message(
    channel: BlockingChannel,
    method,
    _properties,
    body: bytes
):
    job = InternalImageProcessingJob.model_validate_json(body)
    job_id = job.job_id

    storage_host = settings.photo_storage_host
    storage_port = settings.photo_storage_port

    print(f"Downloading image from job {job_id} for processing.")

    download_specific_image_url = (
        f"http://{storage_host}:{storage_port}/worker/jobs/{job_id}/download-source-image"
    )

    download_specific_image_get_response = requests.get(
        download_specific_image_url,
        stream=True
    )

    if download_specific_image_get_response.status_code != 200:
        raise Exception("Failed to download specific image.")


    filename = (
        download_specific_image_get_response.headers
        .get("Content-Disposition", "")
        .split("filename=")[1]
        .strip('"')
    )

    image_buffer = SpooledTemporaryFile(mode="wb")
    image_buffer.write(download_specific_image_get_response.content)

    output_image_buffer = SpooledTemporaryFile(mode="w+b")

    print(f"Transforming image for job {job_id}.")

    transform_image(
        image_buffer,
        output_image_buffer,
        job.resize_image_to_width,
        job.resize_image_to_height,
        job.change_to_format,
    )

    upload_processes_image_url = (
        f"http://{storage_host}:{storage_port}/worker/jobs/{job_id}/finalize"
    )

    upload_image = {"uploaded_file": (filename, output_image_buffer)}

    print(f"Uploading transformed image for job {job_id}.")

    upload_processes_image_post_response = requests.post(
        upload_processes_image_url,
        files=upload_image
    )

    if upload_processes_image_post_response.status_code != 200:
        print(upload_processes_image_post_response.text)
        raise Exception("Failed to upload processed image.")

    uploaded_image_response = PublicSingleImageResponse.model_validate_json(
        upload_processes_image_post_response.content
    )

    print(
        f"Uploaded transformed image for job {job_id} under "
        f"UUID {uploaded_image_response.image.id}."
    )

    job_update_patch_obj = PublicImageProcessingJobUpdateRequest(
        destination_image_id=uploaded_image_response.image.id,
        status=ImageProcessingJobStatus.SUCCESS.value,
    )

    job_update_patch_url = f"http://{storage_host}:{storage_port}/worker/jobs/{job_id}"

    print(f"Updating job {job_id} status.")

    job_update_patch_response = requests.patch(
        job_update_patch_url,
        data=job_update_patch_obj.model_dump_json().encode("utf-8"),
    )

    if job_update_patch_response.status_code != 200:
        raise Exception("Failed to send update_job_status to storage.")

    print(f"Job {job_id} is fully finished.")

    channel.basic_ack(delivery_tag=method.delivery_tag)



class ProcessImageJob:
    _connection: pika.BlockingConnection
    _channel: BlockingChannel

    def __init__(self):
        self._connection = pika.BlockingConnection(
            parameters=pika.ConnectionParameters(
                host=settings.rabbitmq_host,
                port=settings.rabbitmq_port,
            )
        )

        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=settings.rabbitmq_queue_name)

        self._channel.basic_consume(
            queue=settings.rabbitmq_queue_name,
            on_message_callback=handle_incoming_message
        )

    def start_consuming(self):
        self._channel.start_consuming()

    def stop_consuming(self):
        self._channel.stop_consuming()


image_processor = ProcessImageJob()


original_sigint_handler = signal.getsignal(signal.SIGINT)
if not callable(original_sigint_handler):
    original_sigint_handler = None

def handle_sigint(*args, **kwargs):
    image_processor.stop_consuming()

    if original_sigint_handler is not None:
        original_sigint_handler(*args, **kwargs)


signal.signal(signal.SIGINT, handle_sigint)



def run_http_server():
    print("Starting internal HTTP server for health checks.")
    uvicorn.run(app, host="0.0.0.0", port=8005)


def run_converter_loop():
    try:
        print("Starting RabbitMQ consumer.")
        image_processor.start_consuming()
    except KeyboardInterrupt:
        image_processor.stop_consuming()
    finally:
        print("Exiting converter loop.")



if __name__ == "__main__":
    converter_thread = threading.Thread(target=run_converter_loop)
    converter_thread.start()

    run_http_server()
    converter_thread.join(timeout=None)
