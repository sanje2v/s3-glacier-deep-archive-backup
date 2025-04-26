import os
import os.path
import argparse
from glob import iglob
from dateutil import tz
from http import HTTPStatus
from functools import reduce
from datetime import datetime
from contextlib import suppress
from typing import List

import boto3
import botocore.exceptions
from pathvalidate import is_valid_filepath

from consts import MAX_LINUX_PATH_LENGTH
import settings


def str_to_bytes(value) -> bytes:
    assert isinstance(value, str)
    return value.encode('utf-8')

def abspath(path) -> str:
    # This function properly expands '~' while expanding to absolute path
    return os.path.abspath(os.path.expanduser(path))

def remove_file_ignore_errors(filename) -> None:
    with suppress(OSError):
        os.remove(filename)

def list_files_recursive_iter(folder: str, file_extension: str='') -> List[str]:
    for file_or_dir in iglob(os.path.join(folder, f'**{file_extension}'),
                             recursive=True,
                             include_hidden=True):  # CAUTION: Don't forget to include hidden files
        if os.path.isfile(file_or_dir) and not os.path.islink(file_or_dir): # CAUTION: Don't list symbolic link
            yield abspath(file_or_dir)

def MB_to_bytes(value) -> int:
    return (value * 1024 * 1024)

def GB_to_bytes(value) -> int:
    return MB_to_bytes(value) * 1024

def isAWSConfigAndCredentialsOK() -> bool:
    return len(boto3.Session().available_profiles) > 0

def toLocalDateTimeFromUTCString(value) -> str:
    return datetime.fromisoformat(value).replace(tzinfo=tz.UTC).astimezone()

def prettyDateTimeString(value) -> str:
    return datetime.strftime(value, "%Y-%m-%d %I:%M:%S %p %Z") # eg: 2025-02-01 3:05:00 PM AEST

def maxStrEnumValue(enum_class_type) -> int:
    return len(reduce(max, iter(enum_class_type)))

def prettyFilesize(value, decimal_places=1) -> str:
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if value < 1024.0 or unit == 'TB':
            break
        value /= 1024.0
    return f"{value:.{decimal_places}f} {unit}"

def checkFilesExistsInS3(bucket: str, tar_files: List[str]) -> List[bool]:
    session = boto3.Session()
    s3_client = session.client('s3')

    results: List[bool] = []
    for tar_file in tar_files:
        try:
            s3_client.head_object(Bucket=bucket, Key=tar_file)
            results.append(True)

        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] == '404':
                results.append(False)
            else:
                raise ex

    return results


class ValidateEncryptionKey(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        if values and len(values) != settings.ENCRYPT_KEY_LENGTH:
            raise argparse.ArgumentError(self, f"Encryption key must be exactly {settings.ENCRYPT_KEY_LENGTH} characters long!")

        setattr(namespace, self.dest, values)

class ValidateBucketExists(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None) -> None:
        session = boto3.Session()
        s3_client = session.client('s3')

        try:
            s3_client.head_bucket(Bucket=values)
            
        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] == '404':
                raise argparse.ArgumentError(self, f"The bucket with name '{values}' doesn't exist in S3 server!")
            else:
                raise ex

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
