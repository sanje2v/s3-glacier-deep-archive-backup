import os
import json
import sqlite3
sqlite3.threadsafety = 3    # CAUTION: Make sure serialized (i.e. 3) is enabled as we write to db from multiple threads
from threading import Lock
from typing import Union, Any
from datetime import datetime, timezone

import settings
from utils import *
from consts import MAX_LINUX_PATH_LENGTH, MAX_LINUX_FILENAME_LENGTH

from .common import UploadTaskStatus


class StateDB:
    WORKS_TABLE_NAME = 'works'
    RUNS_TABLE_NAME = 'runs'
    SECRETS_TABLE_NAME = 'secrets'


    def __init__(self, db_filename, cmd_args=None):
        self.state_db = sqlite3.connect(db_filename, check_same_thread=False, autocommit=False)
        self.mutex = Lock()

        self._create_tables()
        if cmd_args: self._record_run(cmd_args)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.state_db.close()

    def _execute(self, sql_cmds_to_execute : str | list[str]) -> None:
        with self.mutex:
            if isinstance(sql_cmds_to_execute, str):
                self.state_db.execute(sql_cmds_to_execute)
            else:
                for sql_cmd in sql_cmds_to_execute:
                    self.state_db.execute(sql_cmd)
            self.state_db.commit()

    def _fetch(self, sql_cmds_to_execute : str | list[str]) -> list[list[Any]]:
        with self.mutex:
            if isinstance(sql_cmds_to_execute, str):
                cursor = self.state_db.execute(sql_cmds_to_execute)
            else:
                cursor = self.state_db.execute(sql_cmds_to_execute[0])
                for sql_cmd in sql_cmds_to_execute[1:]:
                    cursor.execute(sql_cmd)
            return cursor.fetchall()

    def _create_tables(self) -> None:
        self._execute([f"CREATE TABLE IF NOT EXISTS {StateDB.WORKS_TABLE_NAME} "\
                       "(id INTEGER PRIMARY KEY AUTOINCREMENT,"\
                       "datetime DATETIME,"\
                       f"tar_file NVARCHAR({MAX_LINUX_FILENAME_LENGTH}),"\
                       f"filename NVARCHAR({MAX_LINUX_PATH_LENGTH}),"\
                       f"modified_time INTEGER,"\
                       f"size INTEGER,"\
                       f"status VARCHAR({maxStrEnumValue(UploadTaskStatus)}));",

                       f"CREATE TABLE IF NOT EXISTS {StateDB.RUNS_TABLE_NAME} "\
                       "(id INTEGER PRIMARY KEY AUTOINCREMENT,"\
                       "datetime DATETIME,"\
                       f"cmd_args_json NVARCHAR({MAX_LINUX_PATH_LENGTH*10}));",

                       f"CREATE TABLE IF NOT EXISTS {StateDB.SECRETS_TABLE_NAME} "\
                       f"(encryption_key VARCHAR({settings.ENCRYPT_KEY_LENGTH}));"])

    def _record_run(self, cmd_args_dict) -> None:
        self._execute(f"INSERT INTO {StateDB.RUNS_TABLE_NAME} "\
                      "(datetime, cmd_args_json) VALUES "\
                      f"('{datetime.now(timezone.utc)}', '{json.dumps(cmd_args_dict)}');")

    def _process_work_records(self, work_records) -> list[list[Union[int, str]]]:
        output_work_records = []
        for id, datetime_utc, tar_file, filename, modified_time, size, status in work_records:
            output_work_records.append([id,
                                        prettyDateTimeString(toLocalDateTimeFromUTCString(datetime_utc)),
                                        tar_file,
                                        filename,
                                        prettyDateTimeString(datetime.fromtimestamp(modified_time).astimezone()),
                                        prettyFilesize(size),
                                        UploadTaskStatus(status)])
        return output_work_records

    def _set_encryption_key(self, encryption_key: str) -> None:
        try:
            # CAUTION: Here single quotes and backslash for VALUES() must be escaped by repeating them twice
            self._execute(f"INSERT INTO {StateDB.SECRETS_TABLE_NAME} "\
                          f"(encryption_key) VALUES ('{escape_sql_escape_chars(encryption_key)}');")

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex


    def correct_db_init_state(self) -> None:
        try:
            self._execute(f"UPDATE {StateDB.WORKS_TABLE_NAME} "\
                          f"SET datetime='{datetime.now(timezone.utc)}', status='{UploadTaskStatus.FAILED}' "\
                          f"WHERE status NOT IN ('{UploadTaskStatus.PACKAGED}', '{UploadTaskStatus.UPLOADED}', '{UploadTaskStatus.FAILED}');")

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex


    def get_last_cmd_args(self) -> dict[str, Union[str, int]]:
        try:
            cmd_args_json = self._fetch(f"SELECT cmd_args_json FROM {StateDB.RUNS_TABLE_NAME} "\
                                        "ORDER BY id DESC LIMIT 1;")
            cmd_args_json = cmd_args_json[0][0]
            return json.loads(cmd_args_json)

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def get_encryption_key(self) -> bytes:
        try:
            encryption_key = self._fetch(f"SELECT encryption_key FROM {StateDB.SECRETS_TABLE_NAME} "\
                                         "LIMIT 1;")
            if not encryption_key:
                encryption_key = generate_password(settings.ENCRYPT_KEY_LENGTH)
                self._set_encryption_key(encryption_key)
            else:
                encryption_key = encryption_key[0][0]

            return str_to_bytes(encryption_key)

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def get_work_records_with_headers(self, collate: int) -> tuple[list[str], list[list[Union[str, int, bool]]]]:
        try:
            cmd_to_execute = f"SELECT * FROM {StateDB.WORKS_TABLE_NAME} ORDER BY id ASC, filename ASC;"
            work_records = self._fetch(cmd_to_execute)
            work_records = self._process_work_records(work_records)
            if collate:
                # Rename some headers to be appropriate for collated list
                record_headers = ['first_id', 'datetime', 'tar_file(s)', 'folder', 'uploaded']

                collated_work_records = {}
                for id, datetime_utc, tar_file, filename, modified_time, size, status in work_records:
                    assert isinstance(filename, str)

                    dirname = get_last_nth_dirname(filename, collate)
                    if dirname not in collated_work_records:
                        collated_work_records[dirname] = [
                            id,
                            datetime_utc,
                            {tar_file},
                            dirname,
                            (status == UploadTaskStatus.UPLOADED)
                        ]
                    else:
                        if status != UploadTaskStatus.UPLOADED or not collated_work_records[dirname][4]:
                            collated_work_records[dirname][4] = False
                        collated_work_records[dirname][2].add(tar_file)

                for value in collated_work_records.values():
                    value[2] = list(value[2])
                    value[2].sort()     # Sorting tar_file column in ascending order
                    value[2] = ", ".join(value[2])

                work_records = list(collated_work_records.values())
            else:
                # NOTE: 'PRAGMA_TABLE_INFO' contains information about tables in a DB
                record_headers = list(map(lambda x: x[0],
                                          self._fetch(f"SELECT name FROM PRAGMA_TABLE_INFO('{StateDB.WORKS_TABLE_NAME}');")))

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

        return record_headers, work_records

    def get_already_uploaded_files(self) -> set[str]:
        try:
            work_records = self._fetch("SELECT filename "\
                                       f"FROM {StateDB.WORKS_TABLE_NAME} WHERE status='{UploadTaskStatus.UPLOADED}';")
            work_records = set(map(lambda x: x[0], work_records))
            return work_records

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def get_already_uploaded_tar_files(self) -> set[str]:
        try:
            work_records = self._fetch("SELECT DISTINCT tar_file "\
                                       f"FROM {StateDB.WORKS_TABLE_NAME} WHERE status='{UploadTaskStatus.UPLOADED}' ORDER BY tar_file ASC;")
            work_records = set(map(lambda x: x[0], work_records))
            return work_records

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def get_already_packaged_tar_files(self) -> set[str]:
        try:
            work_records = self._fetch("SELECT DISTINCT tar_file "\
                                       f"FROM {StateDB.WORKS_TABLE_NAME} WHERE status='{UploadTaskStatus.PACKAGED}' ORDER BY tar_file ASC;")
            work_records = set(map(lambda x: x[0], work_records))
            return work_records

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def count_already_packaged_tar_files(self) -> int:
        try:
            work_records = self._fetch("SELECT COUNT(DISTINCT tar_file) "\
                                       f"FROM {StateDB.WORKS_TABLE_NAME} WHERE status='{UploadTaskStatus.PACKAGED}';")
            return work_records[0][0]

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def record_changed_work_state(self, task_status: UploadTaskStatus, filename: str | None=None, tar_file: str | None=None) -> None:
        try:
            match task_status:
                case UploadTaskStatus.SCHEDULED:
                    assert filename and tar_file
                    stat = os.stat(filename)
                    modified_time, size = int(stat.st_mtime), stat.st_size
                    self._execute(f"INSERT INTO {StateDB.WORKS_TABLE_NAME} "\
                                  "(datetime, tar_file, filename, modified_time, size, status) VALUES "\
                                  f"('{datetime.now(timezone.utc)}', '{tar_file}', '{escape_sql_escape_chars(filename)}', {modified_time}, {size}, '{task_status}');")
                case _:
                    assert tar_file
                    self._execute(f"UPDATE {StateDB.WORKS_TABLE_NAME} "\
                                  f"SET datetime='{datetime.now(timezone.utc)}', status='{task_status}' "\
                                  f"WHERE tar_file='{tar_file}';")

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def delete_all_work_records(self) -> None:
        self._execute(f"DELETE FROM {StateDB.WORKS_TABLE_NAME};")

    def delete_work_record(self, tar_file: str) -> None:
        self._execute(f"DELETE FROM {StateDB.WORKS_TABLE_NAME} WHERE tar_file='{tar_file}';")
