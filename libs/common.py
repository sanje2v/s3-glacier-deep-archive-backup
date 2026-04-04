from enum import StrEnum


class TaskType(StrEnum):
    UPLOAD = 'upload'
    DECRYPT = 'decrypt'

class UploadTaskStatus(StrEnum):
    SCHEDULED = 'scheduled' # Task is on queue and will be uploaded in its turn
    STARTED = 'started'     # Task is being uploaded
    PACKAGED = 'packaged'   # TAR file has been packaged and is ready to be uploaded, but upload hasn't started yet
    FAILED = 'failed'       # Task failed during upload or packaging
    UPLOADED = 'uploaded'   # Task has been successfully uploaded
