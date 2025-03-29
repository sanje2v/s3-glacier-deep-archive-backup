import os.path
import logging
import logging.config
import argparse

from actions import backup, restore
from utils import ValidateFileExists, ValidateFolderExists
import settings


def main(command, **kwargs):
    match command:
        case 'backup':
            backup(**kwargs)
            
        case 'restore':
            pass


if __name__ == '__main__':
    # Configure logging
    os.makedirs(settings.LOG_DIR, exist_ok=True)
    logging.config.dictConfig(settings.LOGGING_CONFIG_DICT)

    # Add command line arguments
    parser = argparse.ArgumentParser(prog=os.path.basename(__file__),
                                     description="Compress, encrypt, split and upload files to AWS Glacier.")
    subparser = parser.add_subparsers(help="backup or restore", required=True, dest='command')
    
    backup_parser = subparser.add_parser('backup', help="Backup files to AWS Glacier.")
    backup_parser.add_argument('--src-dirs', help="One or more source directories to backup.", type=os.path.abspath, action=ValidateFolderExists, nargs='+', required=True)
    backup_parser.add_argument('--split-size-gb', help=f"Split size in Gigabytes. Default is {settings.DEFAULT_SPLIT_SIZE_GIGABYTES} GB.", type=int, default=settings.DEFAULT_SPLIT_SIZE_GIGABYTES)
    backup_parser.add_argument('--bucket', help="S3 bucket to upload to.", type=str, required=True)
    backup_parser.add_argument('--num-upload-workers', help="Number of upload workers.", type=int, default=settings.DEFAULT_NUM_UPLOAD_WORKERS)
    backup_parser.add_argument('--compress', help="Compress the backup file.", action=argparse.BooleanOptionalAction, default=False)
    backup_parser.add_argument('--compression-type', help="Type of compression to use on TAR file.", type=str.casefold, choices=('gz', 'bz2', 'xz'), default=settings.DEFAULT_TAR_COMPRESSION_TYPE)
    backup_parser.add_argument('--encrypt-key', help="Encrypt the backup file.", type=lambda x: x.encode('utf-8'), default=None)
    backup_parser.add_argument('output_filename', help="Filename to save backup to.", type=os.path.abspath)

    restore_parser = subparser.add_parser('restore', help="Restore files from AWS Glacier.")
    restore_parser.add_argument('--src-filename', help="Filename to save backup to.", type=os.path.abspath, action=ValidateFileExists)
    restore_parser.add_argument('--decompress', help="Decompress the backup file.", action=argparse.BooleanOptionalAction, default=False)
    restore_parser.add_argument('--compression-type', help="Type of compression that was used on TAR file.", type=str, choices=('gz', 'bz2', 'xz'), default=settings.DEFAULT_TAR_COMPRESSION_TYPE)
    restore_parser.add_argument('--decrypt-key', help="Decryption key for the backup file.", type=lambda x: x.encode('utf-8'), default=None)

    main(**parser.parse_args().__dict__)