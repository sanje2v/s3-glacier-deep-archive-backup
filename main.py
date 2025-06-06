import signal
import argparse
import logging
import logging.config

import commands
from utils import *
import settings
from consts import TAR_COMPRESSION_TYPES


def main(command, **kwargs):
    command_func = getattr(commands, command)
    command_func(**kwargs)


if __name__ == '__main__':
    # Configure logging
    os.makedirs(settings.LOG_DIR, exist_ok=True)
    logging.config.dictConfig(settings.LOGGING_CONFIG_DICT)

    if not isAWSConfigAndCredentialsOK():
        logging.warning("Please check for proper S3 configuration and credentials in '~/.aws'!")

    # Allow this programm to be interrupted with Ctrl+C and SIGTERM (default signal used by docker to stop a container)
    signal.signal(signal.SIGTERM, signal.getsignal(signal.SIGINT))

    # Add command line arguments
    parser = argparse.ArgumentParser(prog=os.path.basename(__file__),
                                     description="Program that automates compression, encryption, spliting and uploading files to backup to AWS S3 Glacier Deep Archive.")
    subparser = parser.add_subparsers(help="backup or restore", required=True, dest='command')

    backup_parser = subparser.add_parser('backup', help="Backup files to AWS S3 Glacier Deep Archive.")
    backup_parser.add_argument('--src-dirs', help="One or more source directories to backup.", type=abspath, action=ValidateFoldersExist, nargs='+', required=True)
    backup_parser.add_argument('--split-size', help=f"Split size in Gigabytes (Megabytes if '--test-run' specified). Default is {settings.DEFAULT_SPLIT_SIZE_GIGABYTES} GB.", type=int, default=settings.DEFAULT_SPLIT_SIZE_GIGABYTES)
    backup_parser.add_argument('--bucket', help="S3 bucket to upload to.", type=str, action=ValidateBucketExists, required=True)
    backup_parser.add_argument('--num-upload-workers', help=f"Number of upload workers. Default is {settings.DEFAULT_NUM_UPLOAD_WORKERS}.", type=int, default=settings.DEFAULT_NUM_UPLOAD_WORKERS)
    backup_parser.add_argument('--compression', help=f"Type of compression ({", ".join(TAR_COMPRESSION_TYPES)}) to use on TAR file. Don't specify for no compression.", type=str.lower, choices=TAR_COMPRESSION_TYPES, default='')
    backup_parser.add_argument('--encrypt', help=f"Specify to encrypt the TAR file using ChaCha20. Key will be saved in state database. Nonce is TAR filename, repeated to {settings.ENCRYPT_NONCE_LENGTH} characters. Default is encryption enabled.", action=argparse.BooleanOptionalAction, default=True)
    backup_parser.add_argument('--autoclean', help="Removes all generated TAR files after they are uploaded.", action=argparse.BooleanOptionalAction, default=True)
    backup_parser.add_argument('--test-run', help="Enable for testing using local Minio S3 test server where Deep Archive attribute isn't supported.", action='store_true')
    backup_parser.add_argument('output_filename_template', help="A template filename with path to save backup to.", type=abspath, action=ValidateFilename)

    resume_parser = subparser.add_parser('resume', help="Resume backing up files from last interrupted upload.")
    resume_parser.add_argument('db_filename', help="Filename of the state DB generated during last interrupted backup.", type=abspath, action=ValidateFilesExists)

    show_parser = subparser.add_parser('show', help="List state data from state DB.")
    show_parser.add_argument('--collate', help="Specify collate level for folders view.", type=int, action=ValidateGreaterOrEqualTo0, default=0)
    show_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    decrypt_parser = subparser.add_parser('decrypt', help="Decrypt all downloaded TARs from specified folder.")
    decrypt_parser.add_argument('--autoclean', help="Removes all encrypted TAR files after they have been decrypted.", action=argparse.BooleanOptionalAction, default=True)
    decrypt_parser.add_argument('db_filename', help="Filename of the state DB generated during backup. Needed for encryption key.", type=abspath, action=ValidateFilesExists)
    decrypt_parser.add_argument('tar_files_folder', help="Location containing downloaded TAR files.", type=abspath, action=ValidateFoldersExist)

    sync_parser = subparser.add_parser('sync', help="Sync contents of state database with remote S3.")
    sync_parser.add_argument('--bucket', help="S3 bucket to sync to.", type=str, action=ValidateBucketExists, required=True)
    sync_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    delete_parser = subparser.add_parser('delete', help="Delete files recorded as 'uploaded' in the state DB from remote S3. (WARNING: Action cannot be undone!)")
    delete_parser.add_argument('--bucket', help="S3 bucket to delete from.", type=str, action=ValidateBucketExists, required=True)
    delete_options_parser = delete_parser.add_mutually_exclusive_group(required=True)
    delete_options_parser.add_argument('--all', help="Deletes all backed up TAR files and the state DB file.", action='store_true')
    delete_options_parser.add_argument('--files', help="Delete a specific backup TAR file from AWS S3 Glacier.", type=str, nargs='+')
    delete_parser.add_argument('db_filename', help="Filename of the state DB generated during backup.", type=abspath, action=ValidateFilesExists)

    main(**vars(parser.parse_args()))
