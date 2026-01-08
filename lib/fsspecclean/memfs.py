import io
from typing import Any

from fastapi import Request

from lib.fsspecclean.base_fsspecfs import FSpecFS

def get_storage(request: Request) -> "FSpecFS":
    return request.app.state.storage

class MemFS(FSpecFS):

    def __init__(self):
        super().__init__("memory")

    def store(self, request_id, key, value):
        self._write(self.file_path(request_id, key), value, True)

    def load(self, request_id,  key: str, value: io.BytesIO) -> Any:
        self._read(self.file_path(request_id, key), value, True)