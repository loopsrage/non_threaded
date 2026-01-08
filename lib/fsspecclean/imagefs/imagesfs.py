import io

from lib.fsspecclean.base_fsspecfs.base_fsspecfs import FSpecFS

class ImagesFs(FSpecFS):

    def __init__(self, filesystem: str = None):
        super().__init__(filesystem)

    def _write_png(self, file_path, figure, use_pipe=None):
        img_buffer = io.BytesIO()
        figure.savefig(img_buffer, format='png')
        self._write(file_path, img_buffer, use_pipe)

    def list_images(self, request_id: str):
        file_path = f"{self.file_path(request_id, "images/*.png")}"
        for i in self.client.glob(file_path):
            yield i

    def save_png_file(self, request_id, file_name, figure, use_pipe=None):
        file_path = f"{self.file_path(request_id, file_name, sub_dir="images")}"
        self._write_png(file_path, figure, use_pipe)
