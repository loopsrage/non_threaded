import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from starlette.datastructures import MutableHeaders
from starlette.staticfiles import StaticFiles

from dotenv import load_dotenv
from apps.files_app import router as files_router
from lib.fsspecclean.memfs import FSpecFS

load_dotenv()

storage = FSpecFS(filesystem=os.getenv("STORAGE_PROTOCOL", "memory"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api_logger")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP LOGIC ---

    yield  # --- The app is now running and handling requests ---
    if hasattr(storage, "close"):
        storage.close()
    elif hasattr(storage.client, "close"):
        storage.client.close()
    logger.info(f"Shutdown complete.")

app = FastAPI(lifespan=lifespan)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.perf_counter()

    headers = MutableHeaders(scope=request.scope)
    request_id = uuid.uuid4().hex
    headers.append("X-Request-Id", request_id)

    response = await call_next(request)
    process_time = time.perf_counter() - start_time

    response.headers["X-Process-Time"] = f"{process_time:.4f}s"
    response.headers["X-Request-Id"] = str(request_id)

    logger.info(f"Method: {request.method}, RequestId: {request_id}, Path: {request.url.path} Time: {process_time:.4f}s")

    return response

app.include_router(files_router)
app.state.storage = storage
static_dir = os.getenv("STATIC_DIR", "static")
if os.path.exists(static_dir):
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
else:
    logger.warning(f"Static directory '{static_dir}' not found. Skipping mount.")
