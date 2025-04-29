import os.path
import logging
from copy import deepcopy
from functools import partial
from concurrent.futures import ThreadPoolExecutor

import boto3
import boto3.session

from .common import TaskType, UploadTaskStatus
from .state_db import StateDB
from .fileobjs import DecryptFileObj

import settings
from utils import remove_file_ignore_errors


class WorkerPool:
    def __init__(self,
                 num_workers: int,
                 task_type: TaskType,
                 autoclean: bool,
                 state_db: StateDB,
                 s3_bucket_name: str=None,
                 max_retry_attempts: int=None,
                 decrypt_key: bytes=None,
                 test_run: bool=False):
        self.num_workers = num_workers
        self.task_type = task_type
        self.autoclean = autoclean
        self.state_db = state_db
        self.s3_bucket_name = s3_bucket_name
        self.max_retry_attempts = max_retry_attempts
        self.decrypt_key = decrypt_key
        self.test_run = test_run

        self.thread_pool = ThreadPoolExecutor(max_workers=num_workers,
                                              thread_name_prefix=f's3-glacier-backup-{self.task_type}')


    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.thread_pool.shutdown(wait=True)     # Wait for everything to be uploaded/downloaded before disposing thread pool

    def _progress_callback(self, total_bytes, bytes_processed):
        pass

    def _work(self, tar_filename: str):
        assert self.task_type in [TaskType.UPLOAD, TaskType.DECRYPT]
        
        match self.task_type:
            case TaskType.UPLOAD:
                session = boto3.Session()   # NOTE: Load S3 credentials and configuration from '~/.aws'
                s3_client = session.client('s3',
                                            config=boto3.session.Config(max_pool_connections=2,
                                                                        retries={'max_attempts': self.max_retry_attempts,
                                                                                'mode': 'standard'}))
                S3_EXTRA_ARGS_DICT = {
                    'ChecksumAlgorithm': 'sha256'
                }
                if not self.test_run:
                    S3_EXTRA_ARGS_DICT['StorageClass'] = 'DEEP_ARCHIVE'     # If using Amazon AWS (not local debug endpoint), ask to put it in Glacier Deep Archive
                
                s3_client.upload_file(tar_filename,
                                      self.s3_bucket_name,
                                      os.path.basename(tar_filename),
                                      Callback=partial(self._progress_callback, os.path.getsize(tar_filename)),
                                      ExtraArgs=S3_EXTRA_ARGS_DICT)

            case TaskType.DECRYPT:
                decryption_key = self.state_db.get_encryption_key()
                with DecryptFileObj(tar_filename,
                                    decryption_key) as decryptor:
                    output_filename = tar_filename.removesuffix(settings.ENCRYPTED_FILE_EXTENSION)
                    decryptor.decrypt(output_filename, settings.BUFFER_MEM_SIZE_BYTES)

        if self.autoclean:
            remove_file_ignore_errors(tar_filename)


    def _work_wrapper(self, tar_filename):
        try:
            tar_file = os.path.basename(tar_filename)

            # Work started
            if self.task_type == TaskType.UPLOAD:
                self.state_db.record_changed_work_state(UploadTaskStatus.STARTED, tar_file=tar_file)
            logging.info(f"{'Uploading' if self.task_type == TaskType.UPLOAD else 'Decrypting'} '{tar_filename}'...")

            # Doing work
            self._work(tar_filename)

            # Work succeeded
            if self.task_type == TaskType.UPLOAD:
                self.state_db.record_changed_work_state(UploadTaskStatus.UPLOADED, tar_file=tar_file)
            logging.info(f"{'Uploaded' if self.task_type == TaskType.UPLOAD else 'Decrypted'} '{tar_filename}'.")

        except Exception as exception:
            # Work failed
            if self.task_type == TaskType.UPLOAD:
                self.state_db.record_changed_work_state(UploadTaskStatus.FAILED, tar_file=tar_file)
            logging.error(f"Failed to {self.task_type} '{tar_filename}' with '{exception}'.")


    def put_on_tasks_queue(self, tar_filename):
        self.thread_pool.submit(self._work_wrapper, deepcopy(tar_filename))
