import os
import logging
from typing import List
from http import HTTPStatus
from datetime import datetime

import boto3
from tabulate import tabulate

import settings
from utils import *
from libs import TaskType, UploadTaskStatus, WorkerPool, SplitTarFiles, StateDB



def backup(src_dirs: List[str],
           output_filename_template: str,
           split_size: int,
           bucket: str,
           num_upload_workers: int,
           compression: str,
           encrypt_key: bytes,
           autoclean: bool,
           test_run: bool):
    db_filename = datetime.now().strftime(settings.STATE_DB_FILENAME_TEMPLATE)
    _backup(**locals())


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
        tar_file_and_ifexists = zip(already_uploaded_tar_files, checkFilesExistsInS3(bucket, already_uploaded_tar_files))
        for tar_file, ifexists in tar_file_and_ifexists:
            if not ifexists:
                logging.error(f"'{tar_file}' was not found in S3 so its state changed to '{UploadTaskStatus.FAILED}'!")
                state_db.record_changed_work_state(UploadTaskStatus.FAILED, tar_file=tar_file)
    logging.info("Done")


def decrypt(decrypt_key: str,
            autoclean: bool,
            tar_files_folder: str):
    assert not decrypt_key or len(decrypt_key) == settings.ENCRYPT_KEY_LENGTH

    with WorkerPool(num_workers=settings.DEFAULT_NUM_UPLOAD_WORKERS,
                    task_type=TaskType.DECRYPT,
                    decrypt_key=str_to_bytes(decrypt_key),
                    autoclean=autoclean) as decrypt_worker_pool:
        for encrypted_tar_filename in list_files_recursive_iter(tar_files_folder,
                                                                file_extension=settings.ENCRYPTED_FILE_EXTENSION):
            decrypt_worker_pool.put_on_tasks_queue(encrypted_tar_filename)

    print("Done")


def delete(all: bool,
           bucket: str,
           files: List[str],
           db_filename: str):
    with StateDB(db_filename) as state_db:
        match all:
            case True:
                result = input("Are you sure you want to delete all backed up files "\
                               "(Bucket itself must be delete using AWS Console)? (Y/n) ")
                match result:
                    case 'Y':
                        _delete(state_db,
                                bucket,
                                state_db.get_already_uploaded_files(tar_files_instead=True))

                    case _:
                        logging.info("Aborted as 'Y' input was not received!")

            case False:
                _delete(state_db, bucket, tar_files=files)


def _backup(db_filename: str,
            src_dirs: List[str],
            output_filename_template: str,
            split_size: int,
            bucket: str,
            num_upload_workers: int,
            compression: str,
            encrypt_key: str,
            autoclean: bool,
            test_run: bool):
    assert not encrypt_key or len(encrypt_key) == settings.ENCRYPT_KEY_LENGTH
    
    # CAUTION: Call 'locals()' immediately before any variable assignment so that only this function's arguments are captured
    with StateDB(db_filename, locals()) as state_db:
        # NOTE: This worker pool context will block (i.e. will not exit) until all tasks are done.
        with WorkerPool(state_db=state_db,
                        s3_bucket_name=bucket,
                        num_workers=num_upload_workers,
                        task_type=TaskType.UPLOAD,
                        max_retry_attempts=settings.MAX_RETRY_ATTEMPTS,
                        autoclean=autoclean,
                        test_run=test_run) as upload_worker_pool:
            split_size = MB_to_bytes(split_size) if test_run else GB_to_bytes(split_size)   # CAUTION: For testing, we use MB splits for ease

            # Create destination folder and prepare output filename (i.e. add compression type extension postfix)
            os.makedirs(os.path.dirname(output_filename_template), exist_ok=True)
            if compression and not output_filename_template.lower().endswith(f'.{compression}'):
                output_filename_template += f'.{compression}'
            
            if encrypt_key:
                output_filename_template += settings.ENCRYPTED_FILE_EXTENSION
                encrypt_key = str_to_bytes(encrypt_key)

            with SplitTarFiles(output_filename_template,
                               encrypt_key,
                               (encrypt_key[:settings.ENCRYPT_NONCE_LENGTH] if encrypt_key else None),
                               compression,
                               settings.BUFFER_MEM_SIZE_BYTES,
                               upload_worker_pool.put_on_tasks_queue) as split_tarfiles:
                # If resuming backup/uploads, we skip files that were already processed
                already_uploaded_files = state_db.get_already_uploaded_files()

                # For each directory, enumerate files and add them to a tar file
                for src_dir in src_dirs:
                    for src_filename in list_files_recursive_iter(src_dir):
                        # If the total bytes written is larger than split_size,
                        # queue it for upload and start a new tar file.
                        if src_filename in already_uploaded_files:
                            # This file has already been processed so skip
                            already_uploaded_files.remove(src_filename)
                            logging.info(f"Skipping '{src_filename}' as it is marked as '{UploadTaskStatus.UPLOADED}' in state DB!")
                            continue
                        
                        if split_tarfiles.tell() >= split_size:
                            split_tarfiles.create_new_tarfile_part()

                        state_db.record_changed_work_state(UploadTaskStatus.SCHEDULED,
                                                           filename=src_filename,
                                                           tar_file=split_tarfiles.get_tarfile_name())
                        split_tarfiles.add(src_filename)

    logging.info("Done")


def _delete(state_db: StateDB, bucket: str, tar_files: List[str]):
    session = boto3.Session()   # NOTE: Load S3 credentials and configuration from '~/.aws'
    s3_client = session.client('s3')

    for tar_file in tar_files:
        logging.info(f"Trying to delete '{tar_file}'...")
        response = s3_client.delete_object(Key=tar_file, Bucket=bucket)['ResponseMetadata']
        state_db.delete_work_record(tar_file)
        match response['HTTPStatusCode']:
            case HTTPStatus.OK | HTTPStatus.NO_CONTENT:
                state_db.delete_work_record(tar_file)
                logging.info(f"'{tar_file}' deleted!")
            case _:
                logging.error(f"Failed to delete file '{tar_file}'! "\
                              "Please check that such a file and containing bucket exists.")
    
    logging.info("Done")
