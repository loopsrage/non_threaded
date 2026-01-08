import io
from typing import Any

import fsspec

class FSpecFS:
    _fs: Any = None
    _filesytem = None

    def __init__(self, filesystem: str = None):
        self._filesystem = filesystem
        if self._filesystem is None:
            self._filesystem = "memory"

        self._fs = fsspec.filesystem(filesystem)

    @property
    def client(self):
        return self._fs

    @property
    def filesystem(self):
        return self._filesystem

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

    def _read(self, file_path: str, file_buffer: io.BytesIO, use_pipe=None):
        if use_pipe is None:
            use_pipe = False

        errors = []

        # 1. Attempt "pipe" equivalent for reading (cat_file)
        if use_pipe:
            try:
                # cat_file returns the entire file content as bytes
                data = self.client.cat_file(file_path)
                file_buffer.write(data)
                file_buffer.seek(0)
                return
            except Exception as pipe_err:
                errors.append(pipe_err)

        # 2. Fallback to standard open for reading
        try:
            with self.client.open(file_path, "rb") as fs:
                # Reads directly into the buffer memory
                file_buffer.write(fs.read())
            file_buffer.seek(0)
        except Exception as read_err:
            # Raises combined errors if both attempts fail
            raise ExceptionGroup("errors", [*errors, read_err])

    def file_path(self, request_id, file_name: str, sub_dir = None):
        core = f"{self._filesystem}://{request_id}"
        if sub_dir is None:
            return f"{core}/{file_name}"
        return f"{core}/{sub_dir}/{file_name}"

    def close(self):
        # Only needed if using protocols like SFTP/FTP/SSH
        if hasattr(self._fs, "close"):
            self._fs.close()