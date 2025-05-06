import os
import logging
from http import HTTPStatus
from datetime import datetime

import boto3
from rich import print
from rich.table import Table

import settings
from utils import *
from libs import TaskType, UploadTaskStatus, WorkerPool, SplitTarFiles, StateDB



def backup(src_dirs: list[str],
           output_filename_template: str,
           split_size: int,
           bucket: str,
           num_upload_workers: int,
           compression: str,
           encrypt: bool,
           autoclean: bool,
           test_run: bool):
    db_filename = abspath(datetime.now().strftime(settings.STATE_DB_FILENAME_TEMPLATE))
    logging.info(f"Recording backup state in '{db_filename}'...")
    _backup(**locals())


def resume(db_filename: str):
    logging.info("Trying to resume from last failed backup point...")
    with StateDB(db_filename) as state_db:
        cmd_args = state_db.get_last_cmd_args()
        cmd_args['db_filename'] = abspath(db_filename)

    # CAUTION: Following needs to be called after 'with' context so that
    # state DB is NOT opened two times.
    _backup(**cmd_args)


def show(collate: int, db_filename: str):
    try:
        with StateDB(db_filename) as state_db:
            record_headers, work_records = state_db.get_work_records_with_headers(collate)

    except ValueError:
        logging.error(f"Corrupted state DB '{db_filename}'!")
        exit(1)

    table = Table(title="Backup Records")
    for header in record_headers:
        table.add_column(header, justify='center')

    for record in work_records:
        table.add_row(*[str(cell) for cell in record])

    print()
    print(table)
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


def decrypt(autoclean: bool,
            db_filename: str,
            tar_files_folder: str):
    with StateDB(db_filename) as state_db:
        with WorkerPool(settings.DEFAULT_NUM_UPLOAD_WORKERS,
                        TaskType.DECRYPT,
                        autoclean,
                        state_db) as decrypt_worker_pool:
                for encrypted_tar_filename in list_files_recursive_iter(tar_files_folder,
                                                                        file_extension=settings.ENCRYPTED_FILE_EXTENSION):
                    decrypt_worker_pool.put_on_tasks_queue(encrypted_tar_filename)

    print("Done")


def delete(all: bool,
           bucket: str,
           files: list[str],
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
            src_dirs: list[str],
            output_filename_template: str,
            split_size: int,
            bucket: str,
            num_upload_workers: int,
            compression: str,
            encrypt: bool,
            autoclean: bool,
            test_run: bool):
    # CAUTION: Call 'locals()' immediately before any variable assignment so that only this function's arguments are captured
    with StateDB(db_filename, locals()) as state_db:
        # NOTE: This worker pool context will block (i.e. will not exit) until all tasks are done.
        with WorkerPool(num_upload_workers,
                        TaskType.UPLOAD,
                        autoclean,
                        state_db,
                        s3_bucket_name=bucket,
                        test_run=test_run) as upload_worker_pool:
            split_size = MB_to_bytes(split_size) if test_run else GB_to_bytes(split_size)   # CAUTION: For testing, we use MB splits for ease

            # Create destination folder and prepare output filename (i.e. add compression type extension postfix)
            os.makedirs(os.path.dirname(output_filename_template), exist_ok=True)
            if compression and not output_filename_template.lower().endswith(f'.{compression}'):
                output_filename_template += f'.{compression}'

            if encrypt:
                output_filename_template += settings.ENCRYPTED_FILE_EXTENSION
                encrypt_key = state_db.get_encryption_key()
            else:
                encrypt_key = None

            # Check if there are some complete TAR files that haven't beed uploaded yet.
            # If so, upload them first.
            already_packaged_tar_files = state_db.get_already_packaged_tar_files()
            for already_packaged_tar_file in already_packaged_tar_files:
                already_packaged_tar_filename = os.path.join(os.path.dirname(output_filename_template), already_packaged_tar_file)
                if os.path.isfile(already_packaged_tar_filename):
                    # Upload it now using background thread pool
                    logging.info(f"Found '{already_packaged_tar_file}' TAR file ready to upload. Putting on upload queue.")
                    upload_worker_pool.put_on_tasks_queue(already_packaged_tar_filename)
                else:
                    # Mark as failed
                    logging.error(f"The TAR file '{already_packaged_tar_file}' is marked '{UploadTaskStatus.PACKAGED}' but cannot be found! Ignoring.")
                    remove_file_ignore_errors(already_packaged_tar_filename)
                    state_db.record_changed_work_state(UploadTaskStatus.FAILED, tar_file=already_packaged_tar_file)
            upload_worker_pool.wait_on_all_tasks()

            # If resuming backup/uploads, we skip files that were already processed
            already_uploaded_files = state_db.get_already_uploaded_files()
            output_filename_idx = len(state_db.get_already_uploaded_files(tar_files_instead=True))

            with SplitTarFiles(state_db,
                               output_filename_template,
                               output_filename_idx,
                               encrypt_key,
                               compression,
                               settings.BUFFER_MEM_SIZE_BYTES,
                               upload_worker_pool.put_on_tasks_queue) as split_tarfiles:
                logging.info(f"Starting a new TAR file '{split_tarfiles.get_tarfile_name()}' for backup...")

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
                            logging.info(f"Starting a new TAR file '{split_tarfiles.get_tarfile_name()}' for backup...")

                        logging.info(f"Processing '{src_filename}'...")
                        state_db.record_changed_work_state(UploadTaskStatus.SCHEDULED,
                                                           filename=src_filename,
                                                           tar_file=split_tarfiles.get_tarfile_name())
                        split_tarfiles.add(src_filename)

    logging.info("Done")


def _delete(state_db: StateDB, bucket: str, tar_files: list[str]):
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
