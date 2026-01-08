import functools
import io
from typing import Any

import fsspec
import pandas as pd
from fastapi import Request

def get_storage(request: Request) -> "FSpecFS":
    return request.app.state.storage

class FSpecFS:
    _fs: Any = None
    _filesytem = None
    _read_csv = None

    _clean_file_name = None
    _raw_file_name = None

    def __init__(self, filesystem: str = None):
        self._filesystem = filesystem
        if self._filesystem is None:
            self._filesystem = "memory"

        self._fs = fsspec.filesystem(filesystem)
        self._read_csv = functools.partial(
            pd.read_csv,
            compression="gzip",
            storage_options={'fs': self.client})

        self._raw_file_name = "raw.csv.gz"
        self._clean_file_name = "clean.csv.gz"

    @property
    def client(self):
        return self._fs

    @property
    def filesystem(self):
        return self._filesystem

    @property
    def _clean_filename(self):
        return self._clean_file_name

    @property
    def raw_filename(self):
        return self._raw_file_name

    def _write(self, file_path: str, file_buffer: io.BytesIO, use_pipe=None):
        file_buffer.seek(0)
        if use_pipe is None:
            use_pipe = False

        errors = []
        if use_pipe:
            try:
                self.client.pipe_file(file_path, file_buffer.getvalue())
            except Exception as pipe_err:
                errors.append(pipe_err)

        try:
            with self.client.open(file_path, "wb") as fs:
                fs.write(file_buffer.getbuffer())
        except Exception as write_err:
            raise ExceptionGroup("errors", [*errors, write_err])

    def _write_df(self, file_path: str, df: pd.DataFrame, use_pipe=None):
        file_buffer = io.BytesIO()
        df.to_csv(file_buffer, index=False, compression='gzip')
        self._write(file_path, file_buffer, use_pipe)

    def _write_png(self, file_path, figure, use_pipe=None):
        img_buffer = io.BytesIO()
        figure.savefig(img_buffer, format='png')
        self._write(file_path, img_buffer, use_pipe)

    def file_path(self, request_id, file_name: str, sub_dir = None):
        core = f"{self._filesystem}://{request_id}"
        if sub_dir is None:
            return f"{core}/{file_name}"
        return f"{core}/{sub_dir}/{file_name}"

    def get_clean_file(self, request_id: str):
        file_path = f"{self.file_path(request_id, self._clean_file_name)}"
        return self._read_csv(file_path)

    def get_raw_file(self, request_id: str):
        file_path = f"{self.file_path(request_id, self._raw_file_name)}"
        return self._read_csv(file_path)

    def save_clean_file(self, request_id, data, use_pipe=None):
        file_path = f"{self.file_path(request_id, self._clean_file_name)}"
        self._write_df(file_path, data, use_pipe)

    def save_raw_file(self, data, request_id, use_pipe=None):
        file_path = f"{self.file_path(request_id, self._raw_file_name)}"
        self._write_df(file_path, data, use_pipe)

    def save_png_file(self, request_id, file_name, figure, use_pipe=None):
        file_path = f"{self.file_path(request_id, file_name, sub_dir="images")}"
        self._write_png(file_path, figure, use_pipe)

    def list_raw_files(self, request_id: str):
        file_path = f"{self.file_path(request_id, "raw*")}"
        for i in self.client.glob(file_path):
            yield i

    def list_clean_files(self, request_id: str):
        file_path = f"{self.file_path(request_id, "clean*")}"
        for i in self.client.glob(file_path):
            yield i

    def list_images(self, request_id: str):
        file_path = f"{self.file_path(request_id, "images/*.png")}"
        for i in self.client.glob(file_path):
            yield i

    def close(self):
        # Only needed if using protocols like SFTP/FTP/SSH
        if hasattr(self._fs, "close"):
            self._fs.close()