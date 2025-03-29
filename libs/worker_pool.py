import os.path
import logging
from functools import partial
from threading import BoundedSemaphore
from multiprocessing.pool import ThreadPool

import boto3
import boto3.session

from libs import TaskType

import config


class WorkerPool(ThreadPool):
    def __init__(self,
                 state_db,
                 s3_bucket_name: str,
                 num_workers: int,
                 num_work_cache: int,
                 task_type: TaskType,
                 max_retry_attempts: int):
        super().__init__(processes=num_workers)

        self.state_db = state_db
        self.num_workers = num_workers
        self.s3_bucket_name = s3_bucket_name
        self.task_type = task_type
        self.max_retry_attempts = max_retry_attempts

        self.tasks = []
        self.workers_limiting_lock = BoundedSemaphore(num_workers + num_work_cache)  # If all worker threads are busy, we block

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.join()     # Wait for everything to be uploaded/downloaded before disposing thread pool


    def _progress_callback(self, total_bytes, bytes_processed):
        pass

    def _work(self, filename):
        try:
            logging.info(f"{'Uploading' if self.task_type == TaskType.UPLOAD else 'Downloading'} '{filename}'...")
            session = boto3.session.Session(aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                                            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY)
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
                #if endpoint_url is None:
                #    S3_EXTRA_ARGS_DICT['StorageClass'] = 'DEEP_ARCHIVE'     # If using Amazon AWS (not local debug endpoint), ask to put it in Glacier Deep Archive
                
                s3_client.upload_file(filename,
                                      self.s3_bucket_name,
                                      os.path.basename(filename),
                                      Callback=partial(self._progress_callback, os.path.getsize(filename)),
                                      ExtraArgs=S3_EXTRA_ARGS_DICT)
                
                # Upload done so note this in DB
                #self.state_db.execute(f"INSERT INTO uploads VALUES ({datetime.now()}, {filename})")
            else:
                s3_client.download_file(self.s3_bucket_name,
                                        os.path.basename(filename),
                                        filename,
                                        ExtraArgs=S3_EXTRA_ARGS_DICT)
            
                # Verify checksum of downloaded file
                # TODO

            logging.info(f"{'Uploaded' if self.task_type == TaskType.UPLOAD else 'Downloaded'} '{filename}'.")

        finally:
            self.workers_limiting_lock.release()

    def _task_error_callback(self, filename, exception):
        logging.error(f"Failed to {self.task_type} '{filename}' with '{exception}'.")

    def put_on_todo_queue(self, filename):
        self.workers_limiting_lock.acquire(blocking=True)
        task = self.apply_async(self._work,
                                args=(filename,),
                                error_callback=partial(self._task_error_callback, filename))
        self.tasks.append(task)