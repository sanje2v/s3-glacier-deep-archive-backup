from enum import StrEnum


class TaskType(StrEnum):
    UPLOAD = 'upload'
    DOWNLOAD = 'download'

class TaskStatus(StrEnum):
    SCHEDULED = 'scheduled'
    STARTED = 'started'
    FAILED = 'failed'
    COMPLETED = 'completed'