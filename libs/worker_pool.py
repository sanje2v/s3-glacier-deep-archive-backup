import os.path
import logging
from copy import deepcopy
from functools import partial
from concurrent.futures import ThreadPoolExecutor, Future

import boto3
import boto3.session

from .common import TaskType, TaskStatus
from .state_db import StateDB


class WorkerPool:
    def __init__(self,
                 state_db: StateDB,
                 s3_bucket_name: str,
                 num_workers: int,
                 task_type: TaskType,
                 max_retry_attempts: int,
                 test_run: bool):
        self.state_db = state_db
        self.num_workers = num_workers
        self.s3_bucket_name = s3_bucket_name
        self.task_type = task_type
        self.max_retry_attempts = max_retry_attempts
        self.test_run = test_run

        self.thread_pool = ThreadPoolExecutor(max_workers=num_workers,
                                              thread_name_prefix='s3-glacier-backup')


    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.thread_pool.shutdown(wait=True)     # Wait for everything to be uploaded/downloaded before disposing thread pool

    def _progress_callback(self, total_bytes, bytes_processed):
        pass

    def _work(self, tar_filename):
        session = boto3.Session()   # NOTE: Load S3 credentials and configuration from '~/.aws'
        s3_client = session.client('s3',
                                    config=boto3.session.Config(max_pool_connections=2,
                                                                retries={'max_attempts': self.max_retry_attempts,
                                                                        'mode': 'standard'}))
        S3_EXTRA_ARGS_DICT = {
            'ChecksumAlgorithm': 'sha256'
        } if self.task_type == TaskType.UPLOAD else {
            'ChecksumMode': 'ENABLED'
        }
        
        if self.task_type == TaskType.UPLOAD:
            if not self.test_run:
                S3_EXTRA_ARGS_DICT['StorageClass'] = 'DEEP_ARCHIVE'     # If using Amazon AWS (not local debug endpoint), ask to put it in Glacier Deep Archive
            
            s3_client.upload_file(tar_filename,
                                  self.s3_bucket_name,
                                  os.path.basename(tar_filename),
                                  Callback=partial(self._progress_callback, os.path.getsize(tar_filename)),
                                  ExtraArgs=S3_EXTRA_ARGS_DICT)
        else:
            s3_client.download_file(self.s3_bucket_name,
                                    os.path.basename(tar_filename),
                                    tar_filename,
                                    Callback=partial(self._progress_callback),
                                    ExtraArgs=S3_EXTRA_ARGS_DICT)
        
            # Verify checksum of downloaded file
            # TODO

    def _work_wrapper(self, tar_filename):
        try:
            tar_file = os.path.basename(tar_filename)

            # Work started
            self.state_db.record_changed_work_state(TaskStatus.STARTED, tar_file=tar_file)
            logging.info(f"{'Uploading' if self.task_type == TaskType.UPLOAD else 'Downloading'} '{tar_filename}'...")

            # Doing work
            self._work(tar_filename)

            # Work succeeded
            self.state_db.record_changed_work_state(TaskStatus.COMPLETED, tar_file=tar_file)
            logging.info(f"{'Uploaded' if self.task_type == TaskType.UPLOAD else 'Downloaded'} '{tar_filename}'.")

        except Exception as exception:
            # Work failed
            self.state_db.record_changed_work_state(TaskStatus.FAILED, tar_file=tar_file)
            logging.error(f"Failed to {self.task_type} '{tar_filename}' with '{exception}'.")


    def put_on_tasks_queue(self, tar_filename):
        self.thread_pool.submit(self._work_wrapper, deepcopy(tar_filename))
