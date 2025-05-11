import random
import os.path
import logging
from time import sleep
from copy import deepcopy
from functools import partial
from threading import BoundedSemaphore
from concurrent.futures import ThreadPoolExecutor, Future

import boto3
import boto3.session
from rich.progress import Progress,\
                          Task,\
                          TextColumn,\
                          BarColumn,\
                          DownloadColumn,\
                          TransferSpeedColumn,\
                          TimeRemainingColumn,\
                          TimeElapsedColumn

from .common import TaskType, UploadTaskStatus
from .state_db import StateDB
from .fileobjs import DecryptFileObj

import settings
from utils import remove_file_ignore_errors, mins_to_secs


class WorkerPool:
    def __init__(self,
                 num_workers: int,
                 task_type: TaskType,
                 autoclean: bool,
                 state_db: StateDB,
                 s3_bucket_name: str=None,
                 test_run: bool=False):
        self.num_workers = num_workers
        self.task_type = task_type
        self.autoclean = autoclean
        self.state_db = state_db
        self.s3_bucket_name = s3_bucket_name
        self.test_run = test_run

        self.thread_pool = ThreadPoolExecutor(max_workers=num_workers,
                                              thread_name_prefix=f's3-glacier-backup-{self.task_type}')
        self.task_futures: list[Future] = []
        self.task_submission_limiting_semaphore = BoundedSemaphore(num_workers + settings.NUM_WORKS_PRODUCE_AHEAD)
        if task_type == TaskType.UPLOAD:
            self.progresses = Progress(TextColumn("[progress.description]{task.description}"),
                                       BarColumn(),
                                       DownloadColumn(),
                                       TransferSpeedColumn(),
                                       TimeRemainingColumn(),
                                       TimeElapsedColumn(), transient=False, refresh_per_second=1)
            self.progresses.start()
            self.progress_tasks_dict: dict[str, Task] = {}


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Wait for everything to be uploaded/downloaded before disposing thread pool unless it is Ctrl+c
        self.thread_pool.shutdown(wait=(exc_type is not KeyboardInterrupt), cancel_futures=(exc_type is KeyboardInterrupt))
        del self.thread_pool

        if self.task_type == TaskType.UPLOAD:
            self.progresses.stop()

    def _upload_progress_callback(self, tar_file: str, bytes_processed: int):
        progress_task = self.progress_tasks_dict[tar_file]
        self.progresses.update(progress_task, advance=bytes_processed)

    def _work(self, tar_file: str, tar_filename: str) -> None:
        assert self.task_type in [TaskType.UPLOAD, TaskType.DECRYPT]

        match self.task_type:
            case TaskType.UPLOAD:
                session = boto3.Session()   # NOTE: Load S3 credentials and configuration from '~/.aws'
                s3_client = session.client('s3',
                                            config=boto3.session.Config(max_pool_connections=settings.MAX_CONCURRENT_SINGLE_FILE_UPLOADS,
                                                                        retries={'max_attempts': settings.MAX_RETRY_ATTEMPTS,
                                                                                'mode': 'standard'}))
                S3_EXTRA_ARGS_DICT = {
                    'ChecksumAlgorithm': 'sha256'
                }
                if not self.test_run:
                    # If using Amazon AWS (not local debug endpoint), ask to put it in Glacier Deep Archive
                    S3_EXTRA_ARGS_DICT['StorageClass'] = 'DEEP_ARCHIVE'

                self.progress_tasks_dict[tar_file] = self.progresses.add_task(description=f"Uploading '{tar_file}'",
                                                                              total=os.path.getsize(tar_filename))
                s3_client.upload_file(tar_filename,
                                      self.s3_bucket_name,
                                      tar_file,
                                      Callback=partial(self._upload_progress_callback, tar_file),
                                      ExtraArgs=S3_EXTRA_ARGS_DICT)

            case TaskType.DECRYPT:
                decryption_key = self.state_db.get_encryption_key()
                with DecryptFileObj(tar_filename,
                                    decryption_key) as decryptor:
                    output_filename = tar_filename.removesuffix(settings.ENCRYPTED_FILE_EXTENSION)
                    decryptor.decrypt(output_filename, settings.BUFFER_MEM_SIZE_BYTES)

        if self.autoclean:
            remove_file_ignore_errors(tar_filename)


    def _work_wrapper(self, tar_filename: str) -> None:
        tar_file = os.path.basename(tar_filename)

        retry = True
        while retry:
            # Work started
            if self.task_type == TaskType.UPLOAD:
                self.state_db.record_changed_work_state(UploadTaskStatus.STARTED, tar_file=tar_file)
            logging.info(f"{'Uploading' if self.task_type == TaskType.UPLOAD else 'Decrypting'} '{tar_filename}'...")

            try:
                # Doing work
                self._work(tar_file, tar_filename)
                retry = False

            except Exception as ex:
                # Work failed
                if self.task_type == TaskType.UPLOAD:
                    self.state_db.record_changed_work_state(UploadTaskStatus.FAILED, tar_file=tar_file)
                logging.error(f"Failed to {self.task_type} '{tar_filename}' with '{repr(ex)}'.")

                wait_mins = random.randint(**settings.RETRY_WAIT_TIME_RANGE_MINS)
                logging.info(f"Will be retrying in {wait_mins} minutes.")
                sleep(mins_to_secs(wait_mins))

        # Work succeeded
        if self.task_type == TaskType.UPLOAD:
            self.state_db.record_changed_work_state(UploadTaskStatus.UPLOADED, tar_file=tar_file)
        logging.info(f"{'Uploaded' if self.task_type == TaskType.UPLOAD else 'Decrypted'} '{tar_filename}'.")

        self.task_submission_limiting_semaphore.release()


    def put_on_tasks_queue(self, tar_filename: str) -> None:
        self.task_submission_limiting_semaphore.acquire(blocking=True)
        self.task_futures.append(self.thread_pool.submit(self._work_wrapper, deepcopy(tar_filename)))

    def wait_on_all_tasks(self) -> None:
        for task_future in self.task_futures:
            task_future.result()

        self.task_futures.clear()
