import os.path
import logging
import tarfile

from utils import MB_to_bytes


LOG_DIR = 'logs'
LOG_FILENAME = os.path.join(LOG_DIR, 'main.log')
MAX_LOG_SIZE_BYTES = MB_to_bytes(10)
LOG_NUM_BACKUPS = 8
LOGGING_LEVEL = logging.INFO
LOGGING_HIGHLIGHT_KEYWORDS = [
    'Recording',
    'Starting',
    'Processing',
    'Uploading',
    'Uploaded',
    'Failed',
    'Done']
LOGGING_CONFIG_DICT = {
    'version': 1,
    'formatters': {
        'file_log_formatter': {
            'format': "%(asctime)s - [%(levelname)s] - %(message)s",
        },
        'rich_console_formatter': {
            'format': "%(asctime)s - %(message)s"
        }
    },
    'handlers': {
        'file_log_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'file_log_formatter',
            'filename': LOG_FILENAME,
            'maxBytes': MAX_LOG_SIZE_BYTES,
            'backupCount': LOG_NUM_BACKUPS
    },
        'rich_console_handler': {
            'class': 'rich.logging.RichHandler',
            'formatter': 'rich_console_formatter',
            'show_level': True,
            'show_path': False,
            'keywords': LOGGING_HIGHLIGHT_KEYWORDS
    }},
    'root': {
        'level': LOGGING_LEVEL,
        'handlers': ['file_log_handler', 'rich_console_handler']
    }
}

IGNORE_DIRS = [
    'lost+found',
    'node_modules',
    '.venv',
    '__pycache__',
    '.git',
    '.DS_Store',
    '@eaDir',
    '.Spotlight-V100',
    '.Trashes',
    '.fseventsd',
    '.DocumentRevisions-V100',
    '.TemporaryItems',
    '#recycle',
    'System Volume Information',
]
assert not any([ignore_dir.endswith('/') for ignore_dir in IGNORE_DIRS]),\
       "Directory names in 'IGNORE_DIRS' should not end with '/'!"
assert not any(['*' in IGNORE_DIRS]),\
         "Wildcard '*' not supported in 'IGNORE_DIRS'!"

IGNORE_FILES = [
    'desktop.ini',
    'Thumbs.db',
]
assert not any([ignore_file.endswith('/') for ignore_file in IGNORE_FILES]),\
       "File names in 'IGNORE_FILES' should not end with '/'!"
assert not any(['*' in IGNORE_FILES]),\
         "Wildcard '*' not supported in 'IGNORE_FILES'!"

DEFAULT_NUM_UPLOAD_WORKERS = 5
DEFAULT_SPLIT_SIZE_GIGABYTES = 100   # NOTE: This value is interpreted as Megabytes in '--test-run'

ENCRYPT_KEY_LENGTH = 32
ENCRYPT_NONCE_LENGTH = 12
ENCRYPTED_FILE_EXTENSION = '.chacha20'
TARFILE_FORMAT = tarfile.PAX_FORMAT
BUFFER_MEM_SIZE_BYTES = MB_to_bytes(512)     # Process this size block at a time when creating a TAR file

MAX_CONCURRENT_SINGLE_FILE_UPLOADS = 3
NUM_WORKS_PRODUCE_AHEAD = 3
MAX_RETRY_ATTEMPTS = 20
RETRY_WAIT_TIME_RANGE_MINS = (30, 180)
STATE_DB_FILENAME_TEMPLATE = '%Y%m%d-%H%M%S_backup_statedb.sqlite3'
