import functools
import io

import pandas as pd
from lib.fsspecclean.base_fsspecfs import FSpecFS

class CleanFs(FSpecFS):
    _read_csv = None

    def __init__(self, filesystem: str = None):
        super().__init__(filesystem=filesystem)
        self._read_csv = functools.partial(
            pd.read_csv,
            compression="gzip",
            storage_options={'fs': self.client})

    @property
    def clean_filename(self):
        return "clean.csv.gz"

    @property
    def raw_filename(self):
        return "raw.csv.gz"

    def _write_df(self, file_path: str, df: pd.DataFrame, use_pipe=None):
        file_buffer = io.BytesIO()
        df.to_csv(file_buffer, index=False, compression='gzip')
        self._write(file_path, file_buffer, use_pipe)

    def get_clean_file(self, request_id: str):
        file_path = f"{self.file_path(request_id, self.clean_filename)}"
        return self._read_csv(file_path)

    def get_raw_file(self, request_id: str):
        file_path = f"{self.file_path(request_id, self.raw_filename)}"
        return self._read_csv(file_path)

    def save_clean_file(self, request_id, data, use_pipe=None):
        file_path = f"{self.file_path(request_id, self.clean_filename)}"
        self._write_df(file_path, data, use_pipe)

    def save_raw_file(self, request_id, data, use_pipe=None):
        file_path = f"{self.file_path(request_id, self.raw_filename)}"
        self._write_df(file_path, data, use_pipe)

    def list_raw_files(self, request_id: str):
        file_path = f"{self.file_path(request_id, "raw*")}"
        for i in self.client.glob(file_path):
            yield i

    def list_clean_files(self, request_id: str):
        file_path = f"{self.file_path(request_id, "clean*")}"
        for i in self.client.glob(file_path):
            yield i