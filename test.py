import tarfile
from functools import partial
from typing import List
from libs.fileobjs import EncryptSplitFileObj


def put_on_upload_queue(filename, is_last):
    print(is_last)


with EncryptSplitFileObj(filename='/tmp/thatdir/thisfile.tar',
                            encrypt_key=None,
                            split_size=(1024*1024),
                            callback=partial(put_on_upload_queue)) as fileobj:
    with tarfile.open(fileobj=fileobj,
                        format=tarfile.PAX_FORMAT,
                        mode=f'w:',
                        bufsize=(1024*1024)) as tar:
        tar.add('/tmp/thisdir', arcname='.', recursive=True)