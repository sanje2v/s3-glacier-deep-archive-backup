import os
import json
import sqlite3
sqlite3.threadsafety = 3    # CAUTION: Make serialized (i.e. 3) is enabled as we write to db from multiple threads
from threading import Lock
from datetime import datetime, timezone
from typing import Union, Any

from consts import MAX_LINUX_PATH_LENGTH, MAX_LINUX_FILENAME_LENGTH
from utils import *
import settings

from .common import UploadTaskStatus


class StateDB:
    WORKS_TABLE_NAME = 'works'
    RUNS_TABLE_NAME = 'runs'
    SECRETS_TABLE_NAME = 'secrets'


    def __init__(self, db_filename, cmd_args=None):
        self.state_db = sqlite3.connect(db_filename, check_same_thread=False, autocommit=False)
        self.mutex = Lock()

        self._create_tables()
        if cmd_args:
            self._record_run(cmd_args)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.state_db.close()

    def _execute(self, sql_cmds_to_execute, return_value=False) -> Union[None, list[list[Any]]]:
        with self.mutex:
            cursor = self.state_db.execute(sql_cmds_to_execute)
            match return_value:
                case False:
                    self.state_db.commit()

                case True:
                    return cursor.fetchall()

    def _create_tables(self) -> None:
        self._execute(f"CREATE TABLE IF NOT EXISTS {StateDB.WORKS_TABLE_NAME} "\
                      "(id INTEGER PRIMARY KEY AUTOINCREMENT,"\
                      "datetime DATETIME,"\
                      f"tar_file NVARCHAR({MAX_LINUX_FILENAME_LENGTH}),"\
                      f"filename NVARCHAR({MAX_LINUX_PATH_LENGTH}),"\
                      f"modified_time INTEGER,"\
                      f"size INTEGER,"\
                      f"status VARCHAR({maxStrEnumValue(UploadTaskStatus)}));")
        self._execute(f"CREATE TABLE IF NOT EXISTS {StateDB.RUNS_TABLE_NAME} "\
                      "(id INTEGER PRIMARY KEY AUTOINCREMENT,"\
                      "datetime DATETIME,"\
                      f"cmd_args_json NVARCHAR({MAX_LINUX_PATH_LENGTH*10}));")
        self._execute(f"CREATE TABLE IF NOT EXISTS {StateDB.SECRETS_TABLE_NAME} "\
                      f"(encryption_key VARCHAR({settings.ENCRYPT_KEY_LENGTH}));")

    def _record_run(self, cmd_args_dict) -> None:
        self._execute(f"INSERT INTO {StateDB.RUNS_TABLE_NAME} "\
                      "(datetime, cmd_args_json) VALUES "\
                      f"('{datetime.now(timezone.utc)}', '{json.dumps(cmd_args_dict)}');")

    def _process_work_records(self, work_records) -> list[list[Union[int, str]]]:
        output_work_records = []
        for id, datetime_, tar_file, filename, modified_time, size, status in work_records:
            output_work_records.append([id,
                                        prettyDateTimeString(toLocalDateTimeFromUTCString(datetime_)),
                                        tar_file,
                                        filename,
                                        prettyDateTimeString(datetime.fromtimestamp(modified_time).astimezone()),
                                        prettyFilesize(size),
                                        UploadTaskStatus(status)])
        return output_work_records

    def _set_encryption_key(self, encryption_key: str) -> None:
        try:
            self._execute(f"INSERT INTO {StateDB.SECRETS_TABLE_NAME} "\
                          f"(encryption_key) VALUES ('{escape_sql_escape_chars(encryption_key)}');")    # CAUTION: Single quotes and backslash must be escaped with repeat

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex


    def get_last_cmd_args(self) -> dict[str, Union[str, int]]:
        try:
            cmd_args_json = self._execute(f"SELECT cmd_args_json FROM {StateDB.RUNS_TABLE_NAME} "\
                                          "ORDER BY id DESC LIMIT 1;", return_value=True)[0][0]
            return json.loads(cmd_args_json)

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def get_encryption_key(self) -> bytes:
        try:
            encryption_key = self._execute(f"SELECT encryption_key FROM {StateDB.SECRETS_TABLE_NAME} "\
                                           "LIMIT 1;", return_value=True)
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
            work_records = self._execute(cmd_to_execute, return_value=True)
            work_records = self._process_work_records(work_records)
            if collate:
                # Rename some headers to be appropriate for collated list
                record_headers = ['first_id', 'datetime', 'tar_file(s)', 'folder', 'uploaded']

                collated_work_records = {}
                for id, datetime, tar_file, filename, modified_time, size, status in work_records:
                    dirname = filename
                    for _ in range(collate):
                        dirname_ = os.path.dirname(dirname)
                        if dirname_ in ['/', '']:
                            break   # We have reached the most top-level folder, so no need to go further
                        else:
                            dirname = dirname_

                    if dirname not in collated_work_records:
                        collated_work_records[dirname] = [id, datetime, {tar_file}, dirname, (status == UploadTaskStatus.UPLOADED)]
                    else:
                        if status != UploadTaskStatus.UPLOADED or not collated_work_records[dirname][4]:
                            collated_work_records[dirname][4] = False
                        collated_work_records[dirname][2].add(tar_file)

                for key, value in collated_work_records.items():
                    value[2] = list(value[2])
                    value[2].sort()     # Sorting tar_file column in ascending order
                    value[2] = ", ".join(value[2])

                work_records = list(collated_work_records.values())
            else:
                # NOTE: 'PRAGMA_TABLE_INFO' contains information about tables in a DB
                record_headers = list(map(lambda x: x[0],
                                          self._execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{StateDB.WORKS_TABLE_NAME}');",
                                                        return_value=True)))

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

        return record_headers, work_records

    def get_already_uploaded_files(self, tar_files_instead: bool=False) -> list[str]:
        try:
            work_records = self._execute(f"SELECT {'DISTINCT tar_file' if tar_files_instead else 'filename'} "\
                                         f"FROM {StateDB.WORKS_TABLE_NAME} WHERE status='{UploadTaskStatus.UPLOADED}';",
                                         return_value=True)
            work_records = list(map(lambda x: x[0], work_records))
            return work_records

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def get_already_packaged_tar_files(self) -> list[str]:
        try:
            work_records = self._execute("SELECT DISTINCT tar_file "\
                                         f"FROM {StateDB.WORKS_TABLE_NAME} WHERE status='{UploadTaskStatus.PACKAGED}';",
                                         return_value=True)
            work_records = list(map(lambda x: x[0], work_records))
            return work_records

        except sqlite3.OperationalError as ex:
            raise ValueError("Corrupted DB!") from ex

    def record_changed_work_state(self, task_status: UploadTaskStatus, filename: str=None, tar_file: str=None) -> None:
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
