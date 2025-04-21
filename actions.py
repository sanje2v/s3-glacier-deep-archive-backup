import os
import logging
import tarfile
from typing import List
from http import HTTPStatus
from datetime import datetime

import boto3
from tabulate import tabulate

import settings
from utils import list_files_recursive_iter, MB_to_bytes, GB_to_bytes, str_to_bytes, createBucketIfNotExists
from libs import TaskType, TaskStatus, WorkerPool, EncryptSplitFileObj, StateDB



def backup(src_dirs: List[str],
           output_filename_template: str,
           split_size: int,
           bucket: str,
           num_upload_workers: int,
           compress: bool,
           compression_type: str,
           encrypt_key: bytes,
           test_run: bool):
    db_filename = datetime.now().strftime(settings.STATE_DB_FILENAME_TEMPLATE)
    _backup(db_filename,
            src_dirs,
            output_filename_template,
            split_size,
            bucket,
            num_upload_workers,
            compress,
            compression_type,
            encrypt_key,
            test_run)


def resume(db_filename: str):
    logging.info("Trying to resume from last failed backup point...")
    with StateDB(db_filename) as state_db:
        cmd_args = state_db.get_last_cmd_args()
        del cmd_args['db_filename']

    # CAUTION: Following needs to be called after 'with' context so that
    # state DB is NOT opened two times.
    _backup(db_filename, **cmd_args)


def list(collate: bool, db_filename: str):
    try:
        with StateDB(db_filename) as state_db:
            record_headers, work_records = state_db.get_work_records_with_headers(collate)

    except ValueError:
        logging.error(f"Corrupted state DB '{db_filename}'!")
        exit(1)
    
    print()
    print()
    print(tabulate(work_records, headers=record_headers, numalign='right', stralign='center'))
    print()


def sync(bucket: str, db_filename: str):
    with StateDB(db_filename) as state_db:
        already_uploaded_tar_files = state_db.get_already_uploaded_files(tar_files_instead=True)
        for tar_file, existsInS3 in _checkFilesExistsInS3(bucket, already_uploaded_tar_files):
            if not existsInS3:
                logging.error(f"'{tar_file}' was not found in S3 so its state changed to '{TaskStatus.FAILED}'!")
                state_db.record_changed_work_state(TaskStatus.FAILED, tar_file=tar_file)
    logging.info("Done")


def download(tar_files: List[str],
             bucket: str,
             decrypt_key: str,
             output_folder: str):
    assert not decrypt_key or len(decrypt_key) == settings.ENCRYPT_KEY_LENGTH

    with WorkerPool(s3_bucket_name=bucket,
                    num_download_workers=num_download_workers,
                    task_type=TaskType.DOWNLOAD,
                    max_retry_attempts=settings.MAX_RETRY_ATTEMPTS) as download_worker_pool:
        for src_file in src_files:
            download_worker_pool.put_on_tasks_queue(src_file)

    print("Done")


def delete(all: bool,
           bucket: str,
           files: List[str],
           db_filename: str):
    with StateDB(db_filename) as state_db:
        match all:
            case True:
                result = input("Are you sure you want to delete all backed up files? (Y/n) ")
                match result:
                    case 'Y':
                        result = input("Should I delete the specified bucket containing the files, which will be faster? (Y/n) ")
                        match result:
                            case 'Y':
                                _delete_bucket(bucket)

                            case _:
                                already_uploaded_tar_files = state_db.get_already_uploaded_files(tar_files_instead=True)
                                _delete(state_db, bucket, already_uploaded_tar_files)

                    case _:
                        logging.info("Aborted as 'Y' input was not received!")

            case False:
                _delete(state_db, bucket, files)


def _backup(db_filename: str,
            src_dirs: List[str],
            output_filename_template: str,
            split_size: int,
            bucket: str,
            num_upload_workers: int,
            compress: bool,
            compression_type: str,
            encrypt_key: str,
            test_run: bool):
    assert not encrypt_key or len(encrypt_key) == settings.ENCRYPT_KEY_LENGTH
    
    # CAUTION: Call 'locals()' immediately before any variable assignment so that only this function's arguments are captured
    with StateDB(db_filename, locals()) as state_db:
        createBucketIfNotExists(bucket)

        with WorkerPool(state_db=state_db,
                        s3_bucket_name=bucket,
                        num_workers=num_upload_workers,
                        task_type=TaskType.UPLOAD,
                        max_retry_attempts=settings.MAX_RETRY_ATTEMPTS,
                        test_run=test_run) as upload_worker_pool:
            split_size = MB_to_bytes(split_size) if test_run else GB_to_bytes(split_size)   # CAUTION: For testing, we use MB splits for ease
            encrypt_key = str_to_bytes(encrypt_key) if encrypt_key else None

            # Create destination folder and prepare output filename (i.e. add compression type extension postfix)
            os.makedirs(os.path.dirname(output_filename_template), exist_ok=True)
            if compress:
                output_filename_template += f'.{compression_type}'

            # NOTE: This worker pool context will block (i.e. will not exit) until all tasks are done.
            with EncryptSplitFileObj(output_filename_template,
                                     encrypt_key,
                                     upload_callback=upload_worker_pool.put_on_tasks_queue) as fileobj:
                # If resuming backup/uploads, we skip files that were already processed
                already_uploaded_files = state_db.get_already_uploaded_files()

                # For each directory, enumerate files and add them to a tar file
                for src_dir in src_dirs:
                    with tarfile.open(fileobj=fileobj,
                                      format=settings.TARFILE_FORMAT,
                                      mode=f'w:{compression_type if compress else ""}',
                                      bufsize=settings.BUFFER_MEM_SIZE_BYTES) as tar:
                        for src_filename in list_files_recursive_iter(src_dir):
                            # If the total bytes written is larger than split_size,
                            # queue it for upload and start a new tar file.
                            if src_filename in already_uploaded_files:
                                # This file has already been processed so skip
                                already_uploaded_files.remove(src_filename)
                                continue
                            
                            if fileobj.last_part_size() >= split_size:
                                fileobj.create_new_part()

                            state_db.record_changed_work_state(TaskStatus.SCHEDULED,
                                                               filename=src_filename,
                                                               tar_file=fileobj.get_tar_file())
                            tar.add(src_filename, arcname='.')

    logging.info("All uploading tasks completed successfully!")


def _checkFilesExistsInS3(bucket: str, tar_files: List[str]):
    session = boto3.Session()
    s3_client = session.client('s3')

    results = []
    for tar_file in tar_files:
        try:
            s3_client.head_object(Bucket=bucket, Key=tar_file)
            results.append(True)

        except s3_client.exceptions.NoSuchKey:
            results.append(False)

    return zip(tar_files, results)


def _delete(state_db: StateDB, bucket: str, tar_files: List[str]):
    session = boto3.Session()   # NOTE: Load S3 credentials and configuration from '~/.aws'
    s3_client = session.client('s3')
    
    for tar_file in tar_files:
        logging.info(f"Trying to delete '{tar_file}'...")
        response = s3_client.delete_object(Key=tar_file, Bucket=bucket)['ResponseMetadata']
        if response['HTTPStatusCode'] in (HTTPStatus.OK, HTTPStatus.NO_CONTENT):
            state_db.delete_work_record(tar_file)
            logging.info("Done")
        else:
            logging.error(f"Failed to delete file '{tar_file}'! Please check that such a file and containing bucket exists.")


def _delete_bucket(bucket: str):
    session = boto3.Session()
    s3_client = session.client('s3')
    s3_client.delete_bucket(Bucket=bucket)
    logging.info(f"'{bucket}' was deleted.")
