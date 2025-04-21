import os
import os.path
import argparse
from glob import iglob
from dateutil import tz
from functools import reduce
from datetime import datetime
from contextlib import suppress

import boto3
import botocore.errorfactory
from pathvalidate import is_valid_filepath

from consts import MAX_LINUX_PATH_LENGTH
import settings


def str_to_bytes(value) -> bytes:
    assert isinstance(value, str)
    return value.encode('utf-8')

def abspath(path):
    # This function properly expands '~' while computing absolute path
    return os.path.abspath(os.path.expanduser(path))

def remove_file_ignore_errors(filename):
    with suppress(OSError):
        os.remove(filename)

def list_files_recursive_iter(folder):
    for file_or_dir in iglob(os.path.join(folder, '**'),
                             recursive=True,
                             include_hidden=True):  # CAUTION: Don't forget to include hidden files
        if os.path.isfile(file_or_dir) and not os.path.islink(file_or_dir): # CAUTION: Don't list symbolic link
            yield abspath(file_or_dir)

def MB_to_bytes(value):
    return (value * 1024 * 1024)

def GB_to_bytes(value):
    return MB_to_bytes(value) * 1024

def isAWSConfigAndCredentialsOK():
    return len(boto3.Session().available_profiles) > 0

def createBucketIfNotExists(bucket: str):
    session = boto3.Session()
    s3_client = session.client('s3')

    with suppress(s3_client.exceptions.BucketAlreadyExists, s3_client.exceptions.BucketAlreadyOwnedByYou):
        s3_client.create_bucket(Bucket=bucket)

def toLocalDateTimeFromUTCString(value):
    return datetime.fromisoformat(value).replace(tzinfo=tz.UTC).astimezone()

def prettyDateTimeString(value):
    return datetime.strftime(value, "%Y-%m-%d %I:%M:%S %p %Z") # eg: 2025-02-01 3:05:00 PM AEST

def maxStrEnumValue(enum_class_type):
    return len(reduce(max, iter(enum_class_type)))

def prettyFilesize(value, decimal_places=1):
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if value < 1024.0 or unit == 'PB':
            break
        value /= 1024.0
    return f"{value:.{decimal_places}f} {unit}"


class ValidateEncryptionKey(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        if values and len(values) != settings.ENCRYPT_KEY_LENGTH:
            raise argparse.ArgumentError(self, f"Encryption key must be exactly {settings.ENCRYPT_KEY_LENGTH} characters long!")

        setattr(namespace, self.dest, values)

class ValidateFilesExists(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        isListType = isinstance(values, (list, tuple))
        
        if not isListType:
            values = [values]

        for filename in values:
            if len(filename) > MAX_LINUX_PATH_LENGTH:
                raise argparse.ArgumentError(self, f"Folder path is too long! Max supported is {MAX_LINUX_PATH_LENGTH}.")

            if not os.path.isfile(filename):
                raise argparse.ArgumentError(self, f"The file '{filename}' doesn't exist!")

        if not isListType:
            values = values[0]

        setattr(namespace, self.dest, values)

class ValidateFoldersExist(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        isListType = isinstance(values, (list, tuple))

        if not isListType:
            values = [values]

        for folder in values:
            if len(folder) > MAX_LINUX_PATH_LENGTH:
                raise argparse.ArgumentError(self, f"Folder path is too long! Max supported is {MAX_LINUX_PATH_LENGTH}.")

            if not os.path.isdir(folder):
                raise argparse.ArgumentError(self, f"The folder '{folder}' doesn't exist!")

        if not isListType:
            values = values[0]

        setattr(namespace, self.dest, values)

class ValidateFilename(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        isListType = isinstance(values, (list, tuple))

        if not isListType:
            values = [values]

        for filename in values:
            if not is_valid_filepath(filename, max_len=MAX_LINUX_PATH_LENGTH, platform='auto'):
                raise argparse.ArgumentError(self, f"Filename path '{filename}' is not valid! "\
                                                    "It might be too long or contains invalid characters.")

        if not isListType:
            values = values[0]

        setattr(namespace, self.dest, values)
