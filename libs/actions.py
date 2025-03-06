import os
import logging
import tarfile
from functools import partial
from typing import List

from utils import listfiles_recursive_iter

from .upload_worker_pool import UploadWorkerPool
from .fileobjs import EncryptSplitFileObj, JoinUnencryptFileObj


def backup(src_dirs: List[str],
           output_filename: str,
           split_size: int,
           num_upload_workers: int,
           compress: bool,
           compression_type: str,
           encrypt_key: bytes = None):
    assert encrypt_key is None or len(encrypt_key) == 32

    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    if compress:
        output_filename += f'.{compression_type}'

    for src_dir in src_dirs:
        for filename in listfiles_recursive_iter(src_dir):
            if os.stat(filename).st_size > (split_size*1024):
                raise ValueError(f"File '{filename}' is larger than split size of {split_size} KB.")
            else:
                logging.info(f"Backing up '{filename}'...")

    with UploadWorkerPool(num_workers=num_upload_workers) as upload_worker_pool:
        for src_filename in listfiles_recursive_iter(src_dir):
            upload_worker_pool.put_on_upload_queue(src_filename)
        with EncryptSplitFileObj(filename=output_filename,
                                    encrypt_key=encrypt_key,
                                    split_size=(split_size*1024),
                                    callback=partial(upload_worker_pool.put_on_upload_queue)) as fileobj:
            with tarfile.open(fileobj=fileobj,
                                format=tarfile.PAX_FORMAT,
                                mode=f'w:{compression_type if compress else ""}',
                                bufsize=(split_size*1024)) as tar:
                tar.add(src_dir, arcname='.', recursive=True)


def restore(src_filename: str,
            decompress: bool,
            compression_type: str,
            decrypt_key: bytes = None):
    assert decrypt_key is None or len(decrypt_key) == 32
    return

def download_decrypt_join(glacier_resource, src_filename, output_dir, encrypted, encrypted_key):
    assert encrypted and len(encrypted_key) == 32

    # Download first file
    glacier_resource.download_file(src_filename + '.0', src_filename + '.0')

    # Open this file in append mode, download other parts and append to this file
    with open(src_filename + '.0', mode='ab') as file:
        for i in range(1, 1000):
            try:
                glacier_resource.download_file(src_filename + f'.{i}', src_filename + f'.{i}')
                with open(src_filename + f'.{i}', mode='rb') as part:
                    file.write(part.read())

            finally:
                os.remove(src_filename + f'.{i}')


    encrypted = encrypted.lower() == 'true'
    encrypted_key = encrypted_key.encode('utf-8')
    assert len(encrypted_key) == 32
    with JoinUnencryptFileObj(filenames=[src_filename + f'.{i}' for i in range(1000)],
                              encrypt=encrypted,
                              encrypt_key=encrypted_key) as fileobj:
        with tarfile.open(fileobj=fileobj,
                          mode='r|',
                          bufsize=1024*1024) as tar:
            tar.extractall(output_dir)


def decrypt(src_filenames, output_dir, encrypted, encrypted_key):
    encrypted = encrypted.lower() == 'true'
    encrypted_key = encrypted_key.encode('utf-8')
    assert len(encrypted_key) == 32
    with JoinUnencryptFileObj(filenames=[src_filenames + f'.{i}' for i in range(1000)],
                              encrypt=encrypted,
                              encrypt_key=encrypted_key) as fileobj:
        with tarfile.open(fileobj=fileobj,
                          mode='r|',
                          bufsize=1024*1024) as tar:
            tar.extractall(output_dir)

