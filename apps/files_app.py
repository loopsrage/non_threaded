import asyncio
import csv
import io
import logging
import os
import traceback
from typing import List, Any, Dict, Annotated

import pandas as pd
import puremagic
from fastapi import APIRouter, HTTPException, UploadFile, Depends, Header
from pydantic import BaseModel
from starlette import status
from starlette.concurrency import run_in_threadpool
from fastapi_utils.cbv import cbv

from lib.async_clean.utils import clean_pipeline
from fastapi import Request

from lib.fsspecclean.memfs import FSpecFS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("files_listener")
router = APIRouter()

def get_storage(request: Request) -> "FSpecFS":
    return request.app.state.storage

def _validate_structure(header):
    # Read a small sample of the text
    sample = header.decode("utf-8")
    has_header = csv.Sniffer().has_header(sample)
    if not has_header:
        raise ValueError("Headers required")

    dialect = csv.Sniffer().sniff(sample)
    if dialect.delimiter not in [",", ";", "\t"]:
        raise ValueError("No common delimiter found")

def _validate_max_size(file):
    max_file_size: int = int(os.getenv("MAX_FILE_SIZE"))
    if file.size > max_file_size:
        raise HTTPException(status_code=413, detail="File too large")

async def _validate_csv_header(file):
    header = await file.read(2048)
    await file.seek(0)

    try:
        mime = await asyncio.to_thread(puremagic.from_stream, file.file)
        await file.seek(0)

        if mime not in ["text/csv", "text/plain"]:
            raise HTTPException(status_code=415, detail="Invalid file type")
    except Exception:
        mime = "text/plain"

    is_csv_content = b"," in header or b";" in header
    is_valid_mime = mime in ["text/csv", "text/plain", "application/csv"]
    is_valid_header = file.content_type in ["text/csv", "application/vnd.ms-excel"]
    if not (is_csv_content and (is_valid_mime or is_valid_header)):
        raise HTTPException(status_code=415, detail=f"Invalid file type: {mime}")
    return header

async def _read_contents_in_threadpool(storage, file, request_id):
    contents = await file.read()

    def encase(ct):
        df = pd.read_csv(io.BytesIO(ct))
        storage.save_raw_file(df, request_id, use_pipe=True)
        return df

    return await run_in_threadpool(encase, contents)

def _validate_file_extension(file):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must have a .csv extension")

class AnalysisResponse(BaseModel):
    targets: List[Dict[str, Any]]
    features: List[Dict[str, Any]]
    images: List[str]

class UploadResponse(BaseModel):
    request_id: str

class ListFilesResponse(BaseModel):
    files: List[str]

@cbv(router)
class FileListener:

    storage: FSpecFS = Depends(get_storage)
    @router.post("/upload")
    async def upload_file(self, file: UploadFile,
                          x_request_id: Annotated[str | None, Header()] = None ):
        try:
            _validate_max_size(file)
            header = await _validate_csv_header(file)
            _validate_file_extension(file)
            await run_in_threadpool(_validate_structure, header)
            await file.seek(0)
            df = await _read_contents_in_threadpool(self.storage, file, x_request_id)
            results = await clean_pipeline(df, self.storage, x_request_id)
            return UploadResponse(request_id=x_request_id)
        except HTTPException as http_exc:
            raise http_exc
        except Exception as e:
            traceback.print_exception(e)
            logger.error(f"Upload failed for RequestID {x_request_id}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="An internal error occurred during file processing"
            )
        finally:
            await file.close()

    @router.get("/download")
    async def download_file(self, request_id):
        logger.info(request_id)

    @router.get("/list", response_model=ListFilesResponse)
    async def list_files(self, request_id):
        response = ListFilesResponse(files=[])
        for x in self.storage.list_raw_files(request_id):
            response.files.append(x)

        for x in self.storage.list_clean_files(request_id):
            response.files.append(x)

        for x in self.storage.list_images(request_id):
            response.files.append(x)

        return response