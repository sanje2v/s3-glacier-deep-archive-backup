from enum import StrEnum


class TaskType(StrEnum):
    UPLOAD = 'upload'
    DECRYPT = 'decrypt'

class UploadTaskStatus(StrEnum):
    SCHEDULED = 'scheduled'
    STARTED = 'started'
    PACKAGED = 'packaged'
    FAILED = 'failed'
    UPLOADED = 'uploaded'
