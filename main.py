import os.path
import argparse
import logging
import logging.config

import actions
from utils import *
import settings
from consts import TAR_COMPRESSION_TYPES


def main(command, **kwargs):
    if not isAWSConfigAndCredentialsOK():
        logging.error("Please check for proper S3 configuration and credentials in '~/.aws'!")
        exit(1)     # Cannot continue without proper S3 credentials and configurations

    command_func = getattr(actions, command)
    command_func(**kwargs)


if __name__ == '__main__':
    # Configure logging
    os.makedirs(settings.LOG_DIR, exist_ok=True)
    logging.config.dictConfig(settings.LOGGING_CONFIG_DICT)

    # Add command line arguments
    parser = argparse.ArgumentParser(prog=os.path.basename(__file__),
                                     description="Program that automates compression, encryption, spliting and uploading files to backup to AWS Glacier's Deep Archive.")
    subparser = parser.add_subparsers(help="backup or restore", required=True, dest='command')
    
    backup_parser = subparser.add_parser('backup', help="Backup files to AWS Glacier.")
    backup_parser.add_argument('--src-dirs', help="One or more source directories to backup.", type=abspath, action=ValidateFoldersExist, nargs='+', required=True)
    backup_parser.add_argument('--split-size', help=f"Split size in Gigabytes (Megabytes if '--test-run' specified). Default is {settings.DEFAULT_SPLIT_SIZE_GIGABYTES} GB.", type=int, default=settings.DEFAULT_SPLIT_SIZE_GIGABYTES)
    backup_parser.add_argument('--bucket', help="S3 bucket to upload to. Will be created if it doesn't exists.", type=str, required=True)
    backup_parser.add_argument('--num-upload-workers', help="Number of upload workers.", type=int, default=settings.DEFAULT_NUM_UPLOAD_WORKERS)
    backup_parser.add_argument('--compress', help="Compress the backup file.", action=argparse.BooleanOptionalAction, default=False)
    backup_parser.add_argument('--compression-type', help="Type of compression to use on TAR file.", type=str.lower, choices=TAR_COMPRESSION_TYPES, default=settings.DEFAULT_TAR_COMPRESSION_TYPE)
    backup_parser.add_argument('--encrypt-key', help="Encrypt the tar file.", type=str, action=ValidateEncryptionKey, default=None)
    backup_parser.add_argument('--test-run', help="Enables test run for local S3 testing where Deep Freeze is disabled and --split-size is interpreted in MB units.", action='store_true')
    backup_parser.add_argument('output_filename_template', help="A template filename with path to save backup to.", type=abspath, action=ValidateFilename)

    resume_parser = subparser.add_parser('resume', help="Resume backing up files from last interrupted state.")
    resume_parser.add_argument('db_filename', help="Filename of the state DB generated during last interrupted backup.", type=abspath, action=ValidateFilesExists)

    list_parser = subparser.add_parser('list', help="List state data from state DB.")
    list_parser.add_argument('--collate', help="Collate to show only at folder-level.", action=argparse.BooleanOptionalAction, default=False)
    list_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    sync_parser = subparser.add_parser('sync', help="Sync contents of local database with remote S3.")
    sync_parser.add_argument('--bucket', help="S3 bucket to sync to.", type=str, required=True)
    sync_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    download_parser = subparser.add_parser('download', help="Download TARs from AWS Glacier which have specified folders to restore.")
    download_parser.add_argument('--tar_files', help="Tar files to download from AWS Glacier. Specify '*' to download all.", type=str, nargs='+', default='*')
    download_parser.add_argument('--bucket', help="S3 bucket to download from.", type=str, required=True)
    download_parser.add_argument('--decrypt-key', help="Decryption key for the tar file.", type=lambda x: x.encode('utf-8'), default=None)
    download_parser.add_argument('dest_folder', help="Destination folder where to download tar file(s).", type=abspath, action=ValidateFoldersExist)

    delete_parser = subparser.add_parser('delete', help="Delete all files recorded as 'uploaded' in the DB and then delete the DB. (WARNING: Action cannot be undone!)")
    delete_parser.add_argument('--bucket', help="S3 bucket to delete from.", type=str, required=True)
    delete_options_parser = delete_parser.add_mutually_exclusive_group(required=True)
    delete_options_parser.add_argument('--all', help="Deletes all backed up files on AWS Glacier and the state DB file.", action='store_true')
    delete_options_parser.add_argument('--files', help="Delete a specific backup TAR file from AWS Glacier.", type=str, nargs='+')
    delete_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    main(**parser.parse_args().__dict__)
