import os.path
import tarfile
from collections.abc import Callable

from .fileobjs import EncryptSplitFileObj

import settings


class SplitTarFiles:
    def __init__(self,
                 output_filename_template: str,
                 output_file_idx: int,
                 encrypt_key: bytes | None,
                 compression: str,
                 buffer_mem_size: int,
                 upload_callback: Callable[[str], None]):
        self.output_filename_template = output_filename_template
        self.output_file_idx = output_file_idx
        self.encrypt_key = encrypt_key
        self.compression = compression
        self.buffer_mem_size = buffer_mem_size
        self.upload_callback = upload_callback

        self.fileobj = None
        self.current_tarfile = None

        self.create_new_tarfile_part()


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


    def create_new_tarfile_part(self) -> None:
        self.close()

        output_filename = os.path.join(os.path.dirname(self.output_filename_template),
                                       f"{self.output_file_idx:03}_{os.path.basename(self.output_filename_template)}")
        self.fileobj = EncryptSplitFileObj(output_filename,
                                           self.encrypt_key,
                                           self.upload_callback)
        self.current_tarfile = tarfile.open(fileobj=self.fileobj,
                                            format=settings.TARFILE_FORMAT,
                                            mode=f'w:{self.compression if self.compression else ""}',
                                            bufsize=self.buffer_mem_size)
        self.output_file_idx += 1

    def tell(self) -> int:
        assert self.fileobj
        return self.fileobj.tell()

    def get_tarfile_name(self) -> str:
        assert self.fileobj
        return os.path.basename(self.fileobj.filename())

    def add(self, filename) -> None:
        assert self.current_tarfile
        self.current_tarfile.add(filename)

    def close(self) -> None:
        if self.current_tarfile:
            self.current_tarfile.close()
            self.current_tarfile = None

        if self.fileobj:
            self.fileobj.close()
            self.fileobj = None
