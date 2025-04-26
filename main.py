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
    backup_parser.add_argument('--bucket', help="S3 bucket to upload to. Will be created if it doesn't exists.", type=str, action=ValidateBucketExists, required=True)
    backup_parser.add_argument('--num-upload-workers', help="Number of upload workers.", type=int, default=settings.DEFAULT_NUM_UPLOAD_WORKERS)
    backup_parser.add_argument('--compression', help=f"Type of compression ({", ".join(TAR_COMPRESSION_TYPES[1:])}) to use on TAR file. Don't specify an option for no compression.", type=str.lower, choices=TAR_COMPRESSION_TYPES, default=settings.DEFAULT_TAR_COMPRESSION_TYPE)
    backup_parser.add_argument('--encrypt-key', help=f"Encrypt the TAR file using {settings.ENCRYPT_KEY_LENGTH} characters long key.", type=str, action=ValidateEncryptionKey, default=None)
    backup_parser.add_argument('--autoclean', help="Removes all generated TAR files after they are uploaded.", action=argparse.BooleanOptionalAction, default=True)
    backup_parser.add_argument('--test-run', help="Enables test run for testing using local Minio S3 test server where Deep Freeze attribute needs to be disabled.", action='store_true')
    backup_parser.add_argument('output_filename_template', help="A template filename with path to save backup to.", type=abspath, action=ValidateFilename)

    resume_parser = subparser.add_parser('resume', help="Resume backing up files from last interrupted state.")
    resume_parser.add_argument('db_filename', help="Filename of the state DB generated during last interrupted backup.", type=abspath, action=ValidateFilesExists)

    decrypt_parser = subparser.add_parser('decrypt', help="Decrypt all downloaded TARs from specified folder.")
    decrypt_parser.add_argument('--decrypt-key', help="Decryption key for the tar file.", type=str, action=ValidateEncryptionKey, required=True)
    decrypt_parser.add_argument('--autoclean', help="Removes all encrypted Tar files after they have been decrypted.", action=argparse.BooleanOptionalAction, default=True)
    decrypt_parser.add_argument('tar_files_folder', help="Location containing downloaded tar files.", type=abspath, action=ValidateFoldersExist)
    
    list_parser = subparser.add_parser('list', help="List state data from state DB.")
    list_parser.add_argument('--collate', help="Collate to show only at folder-level.", action=argparse.BooleanOptionalAction, default=False)
    list_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    sync_parser = subparser.add_parser('sync', help="Sync contents of local database with remote S3.")
    sync_parser.add_argument('--bucket', help="S3 bucket to sync to.", type=str, required=True)
    sync_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    delete_parser = subparser.add_parser('delete', help="Delete files recorded as 'uploaded' in the state DB from remote S3. (WARNING: Action cannot be undone!)")
    delete_parser.add_argument('--bucket', help="S3 bucket to delete from.", type=str, action=ValidateBucketExists, required=True)
    delete_options_parser = delete_parser.add_mutually_exclusive_group(required=True)
    delete_options_parser.add_argument('--all', help="Deletes all backed up files and the state DB file.", action='store_true')
    delete_options_parser.add_argument('--files', help="Delete a specific backup TAR file from AWS Glacier.", type=str, nargs='+')
    delete_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    main(**parser.parse_args().__dict__)
