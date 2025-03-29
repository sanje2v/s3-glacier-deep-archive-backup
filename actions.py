import os
import sqlite3
sqlite3.threadsafety = 3    # CAUTION: Make sure this option is set to searialized (i.e. 3) as we write to db from multiple threads
import logging
import tarfile
from typing import List
from datetime import datetime
from functools import partial

import settings
from utils import listfiles_recursive_iter, GB_to_bytes
from libs import TaskType, WorkerPool, EncryptSplitFileObj #, JoinUnencryptFileObj


def backup(src_dirs: List[str],
           output_filename: str,
           split_size_gb: int,
           bucket: str,
           num_upload_workers: int,
           compress: bool,
           compression_type: str,
           encrypt_key: bytes = None):
    assert encrypt_key is None or len(encrypt_key) == settings.ENCRYPT_KEY_LENGTH

    # Create destination folder and prepare output filename
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    if compress:
        output_filename += f'.{compression_type}'

    STATE_DB_FILENAME = datetime.now().strftime('%Y%m%d-%H%M_state.sqlitedb')
    with sqlite3.connect(STATE_DB_FILENAME) as state_db:
        with WorkerPool(state_db=state_db,
                        s3_bucket_name=bucket,
                        num_workers=num_upload_workers,
                        task_type=TaskType.UPLOADER,
                        max_retry_attempts=settings.MAX_RETRY_ATTEMPTS) as upload_worker_pool:
            # NOTE: This worker pool context will block (i.e. will not exit) until all tasks are done.
            with EncryptSplitFileObj(filename=output_filename,
                                     encrypt_key=encrypt_key,
                                     upload_callback=partial(upload_worker_pool.put_on_tasks_queue)) as fileobj:
                # For each directory, enumerate files and add them to a tar file
                for src_dir in src_dirs:
                    with tarfile.open(fileobj=fileobj,
                                      format=settings.TARFILE_FORMAT,
                                      mode=f'w:{compression_type if compress else ""}',
                                      bufsize=settings.BUFFER_MEM_SIZE_BYTES) as tar:
                        for src_filename in listfiles_recursive_iter(src_dir):
                            # If the total bytes written is larger than split_size_gb,
                            # queue it for upload and start a new tar file.
                            if fileobj.last_part_size() >= split_size_gb*1024*1024*10:
                                fileobj.create_new_part()
                            tar.add(src_filename, arcname='.')
    
        logging.info("All uploading tasks completed successfully!")



def restore(src_files: List[str],
            decompress: bool,
            compression_type: str,
            decrypt_key: bytes,
            output_dir):
    assert decrypt_key is None or len(decrypt_key) == settings.ENCRYPT_KEY_LENGTH
    
    # Create destination folder and prepare output filename
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)

    with WorkerPool(s3_bucket_name=bucket,
                    num_download_workers=num_download_workers,
                    task_type=TaskType.DOWNLOADER,
                    max_retry_attempts=settings.MAX_RETRY_ATTEMPTS) as download_worker_pool:
        for src_file in src_files:
            download_worker_pool.put_on_tasks_queue(src_file)
        
    # Verify checksum of downloaded files
    # TODO

    # Decrypt to proper TAR file, if a decryption key was provided
    for file in files:
        with tarfile.open("test.text"):
            
