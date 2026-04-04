import sys
import os.path
import logging
from time import sleep
from copy import deepcopy
from functools import partial
from concurrent.futures import ThreadPoolExecutor,\
                                Future,\
                                wait,\
                                FIRST_COMPLETED,\
                                ALL_COMPLETED

import sqlite3
import boto3
import boto3.session
import boto3.s3.transfer
from rich.progress import Progress,\
                          TaskID,\
                          TextColumn,\
                          BarColumn,\
                          DownloadColumn,\
                          TransferSpeedColumn,\
                          TimeRemainingColumn,\
                          TimeElapsedColumn

from .state_db import StateDB
from .fileobjs import DecryptFileObj
from .common import TaskType, UploadTaskStatus

import settings
from utils import remove_file_ignore_errors,\
                  mins_to_secs,\
                  KB_to_bytes,\
                  logrithmic_scale_value


class WorkerPool:
    def __init__(self,
                 num_workers: int,
                 task_type: TaskType,
                 autoclean: bool,
                 state_db: StateDB,
                 s3_bucket_name: str | None=None,
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
        if task_type == TaskType.UPLOAD:
            self.progresses = Progress(TextColumn("[progress.description]{task.description}"),
                                       BarColumn(),
                                       DownloadColumn(),
                                       TransferSpeedColumn(),
                                       TimeRemainingColumn(),
                                       TimeElapsedColumn(), transient=False, refresh_per_second=1)
            self.progresses.start()
            self.progress_tasks_dict: dict[str, TaskID] = {}


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Wait for everything to be uploaded/downloaded before disposing thread pool unless it is Ctrl+C
        self.thread_pool.shutdown(wait=exc_type is not KeyboardInterrupt,
                                  cancel_futures=exc_type is KeyboardInterrupt)
        del self.thread_pool

        if self.task_type == TaskType.UPLOAD:
            self.progresses.stop()

    def _upload_progress_callback(self, tar_file: str, bytes_processed: int):
        progress_task = self.progress_tasks_dict[tar_file]
        self.progresses.update(progress_task, advance=bytes_processed)

    def _work(self, tar_file: str, tar_filename: str) -> None:      # CAUTION: Runs in worker thread
        assert self.task_type in [TaskType.UPLOAD, TaskType.DECRYPT]

        match self.task_type:
            case TaskType.UPLOAD:
                session = boto3.Session()   # NOTE: Load S3 credentials and configuration from '~/.aws'
                session_config = boto3.session.Config(retries={'max_attempts': settings.MAX_RETRY_ATTEMPTS, 'mode': 'standard'})
                s3_client = session.client('s3', config=session_config)
                S3_EXTRA_ARGS_DICT = {
                    'ChecksumAlgorithm': 'sha256'
                }
                if not self.test_run:
                    # If using Amazon AWS (not local debug endpoint), ask to put it in Glacier Deep Archive
                    S3_EXTRA_ARGS_DICT['StorageClass'] = 'DEEP_ARCHIVE'

                MAX_BANDWIDTH_PER_WORKER_BYTES_PER_SEC = max(settings.TOTAL_MAX_BANDWIDTH_BYTES_PER_SEC // self.num_workers, KB_to_bytes(1))\
                                                                if settings.TOTAL_MAX_BANDWIDTH_BYTES_PER_SEC > 0 else None
                transfer_config = boto3.s3.transfer.TransferConfig(max_concurrency=settings.MAX_CONCURRENT_SINGLE_FILE_UPLOADS,
                                                                   use_threads=False,
                                                                   max_bandwidth=MAX_BANDWIDTH_PER_WORKER_BYTES_PER_SEC)

                self.progress_tasks_dict[tar_file] = self.progresses.add_task(description=f"Uploading '{tar_file}'",
                                                                              total=os.path.getsize(tar_filename))
                s3_client.upload_file(tar_filename,
                                      self.s3_bucket_name,
                                      tar_file,
                                      Config=transfer_config,
                                      Callback=partial(self._upload_progress_callback, tar_file),
                                      ExtraArgs=S3_EXTRA_ARGS_DICT)

            case TaskType.DECRYPT:
                decryption_key = self.state_db.get_encryption_key()
                with DecryptFileObj(tar_filename, decryption_key) as decryptor:
                    output_filename = tar_filename.removesuffix(settings.ENCRYPTED_FILE_EXTENSION)
                    decryptor.decrypt(output_filename, settings.BUFFER_MEM_SIZE_BYTES)

        if self.autoclean:
            remove_file_ignore_errors(tar_filename)


    def _work_wrapper(self, tar_filename: str) -> None:         # CAUTION: Runs in worker thread
        tar_file = os.path.basename(tar_filename)

        for i in range(sys.maxsize):     # Basically infinite loop
            try:
                if self.task_type == TaskType.UPLOAD:
                    self.state_db.record_changed_work_state(UploadTaskStatus.STARTED, tar_file=tar_file)
                logging.info(f"{'Uploading' if self.task_type == TaskType.UPLOAD else 'Decrypting'} '{tar_filename}'...")

                self._work(tar_file, tar_filename)
                break       # Uploaded succeeded

            except sqlite3.OperationalError as ex:
                logging.error("Database error occurred while trying to record state change for "\
                              f"'{tar_file}' with error '{repr(ex)}'! Program will terminate immediately.")
                sys.exit(-1)

            except Exception as ex:
                if self.task_type == TaskType.UPLOAD:
                    self.state_db.record_changed_work_state(UploadTaskStatus.FAILED, tar_file=tar_file)
                logging.error(f"Failed to {self.task_type} '{tar_filename}' with '{repr(ex)}'.")

                # Wait for logarithmically longer minutes hoping the network issue will be resolved
                wait_mins = logrithmic_scale_value(i, *settings.RETRY_WAIT_TIME_RANGE_MINS)
                logging.info(f"Will be retrying in {wait_mins} minutes.")
                sleep(mins_to_secs(wait_mins))

            except:
                logging.error(f"Unknown error occurred while trying to {self.task_type} '{tar_filename}'. "\
                              "Program will terminate immediately!")
                sys.exit(-1)

        # Record and report task completion
        if self.task_type == TaskType.UPLOAD:
            self.state_db.record_changed_work_state(UploadTaskStatus.UPLOADED, tar_file=tar_file)
        logging.info(f"{'Uploaded' if self.task_type == TaskType.UPLOAD else 'Decrypted'} '{tar_filename}'.")


    def put_on_tasks_queue(self, tar_filename: str) -> None:
        self.task_futures.append(self.thread_pool.submit(self._work_wrapper, deepcopy(tar_filename)))

    def get_num_tasks_running(self) -> int:
        return sum([1 for task_future in self.task_futures if task_future.running()])

    def wait_on_any_task(self) -> None:
        wait(self.task_futures, return_when=FIRST_COMPLETED)

    def wait_on_all_tasks(self) -> None:
        wait(self.task_futures, return_when=ALL_COMPLETED)
        self.task_futures.clear()
