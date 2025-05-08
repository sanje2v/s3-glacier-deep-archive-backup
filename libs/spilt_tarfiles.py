import os.path
import tarfile
from collections.abc import Callable

from .common import UploadTaskStatus
from .state_db import StateDB
from .fileobjs import EncryptSplitFileObj

import settings
from utils import generate_random_name, remove_file_ignore_errors


class SplitTarFiles:
    def __init__(self,
                 state_db: StateDB,
                 output_filename_template: str,
                 output_file_idx: int,
                 encrypt_key: bytes | None,
                 compression: str,
                 buffer_mem_size: int,
                 upload_callback: Callable[[str], None]):
        self.state_db = state_db
        self.output_filename_template = output_filename_template
        self.output_file_idx = output_file_idx
        self.encrypt_key = encrypt_key
        self.compression = compression
        self.buffer_mem_size = buffer_mem_size
        self.upload_callback = upload_callback

        self.output_filename = None
        self.temp_filename = None
        self.fileobj = None
        self.tarfile = None

        self.create_new_tarfile_part()


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close(completed_write=(exc_type is None))


    def create_new_tarfile_part(self) -> None:
        self.close(completed_write=True)

        output_dir = os.path.dirname(self.output_filename_template)
        output_file = f"{self.output_file_idx:03}_{os.path.basename(self.output_filename_template)}"
        self.output_filename = os.path.join(output_dir, output_file)
        self.temp_filename = os.path.join(output_dir, generate_random_name())
        self.fileobj = EncryptSplitFileObj(self.temp_filename,
                                           self.encrypt_key)
        self.tarfile = tarfile.open(fileobj=self.fileobj,
                                    format=settings.TARFILE_FORMAT,
                                    mode=f'w:{self.compression if self.compression else ""}',
                                    bufsize=self.buffer_mem_size)
        self.output_file_idx += 1

    def tell(self) -> int:
        assert self.fileobj
        return self.fileobj.tell()

    def get_tarfile_name(self) -> str:
        assert self.output_filename
        return os.path.basename(self.output_filename)

    def add(self, filename) -> None:
        assert self.tarfile
        self.tarfile.add(filename)

    def close(self, completed_write: bool) -> None:
        assert (self.tarfile and self.fileobj) or (not self.tarfile and not self.fileobj)
        if self.tarfile:
            self.tarfile.close()
            self.tarfile = None

            self.fileobj.close()
            self.fileobj = None

            if completed_write:
                os.rename(self.temp_filename, self.output_filename)
                output_file = os.path.basename(self.output_filename)
                self.state_db.record_changed_work_state(UploadTaskStatus.PACKAGED, tar_file=output_file)
                self.upload_callback(self.output_filename)
            else:
                remove_file_ignore_errors(self.temp_filename)

            self.output_filename = self.temp_filename = None
