import os.path
import logging
import tarfile

from consts import TAR_COMPRESSION_TYPES
from utils import MB_to_bytes


LOG_DIR = 'logs'
LOG_FILENAME = os.path.join(LOG_DIR, 'main.log')
MAX_LOG_SIZE_BYTES = MB_to_bytes(1)
LOG_NUM_BACKUPS = 8
LOGGING_LEVEL = logging.INFO
LOGGING_FORMAT = '%(asctime)s - [%(levelname)s] - %(message)s'
LOGGING_CONFIG_DICT = {
    'version': 1,
    'formatters': {
        'default': {
            'format': LOGGING_FORMAT,
    }},
    'handlers': {
        'default_log_handler': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': LOG_FILENAME,
            'maxBytes': MAX_LOG_SIZE_BYTES,
            'backupCount': LOG_NUM_BACKUPS
    },
        'console_handler': {
            'class': 'logging.StreamHandler',
            'formatter': 'default',
            'stream': 'ext://sys.stdout'
    }},
    'root': {
        'level': LOGGING_LEVEL,
        'handlers': ['default_log_handler', 'console_handler']
    }
}

DEFAULT_NUM_UPLOAD_WORKERS = 5
DEFAULT_SPLIT_SIZE_GIGABYTES = 50   # NOTE: This value is interpreted as Megabytes in '--test-run'
DEFAULT_TAR_COMPRESSION_TYPE = None
assert DEFAULT_TAR_COMPRESSION_TYPE is None or DEFAULT_TAR_COMPRESSION_TYPE in TAR_COMPRESSION_TYPES

ENCRYPT_KEY_LENGTH = 32
ENCRYPT_NONCE_LENGTH = 12
ENCRYPTED_FILE_EXTENSION = '.ChaCha20'
TARFILE_FORMAT = tarfile.PAX_FORMAT
BUFFER_MEM_SIZE_BYTES = MB_to_bytes(500)     # Process this size block at a time

MAX_RETRY_ATTEMPTS = 8
STATE_DB_FILENAME_TEMPLATE = '%Y%m%d-%H%M%S_backup_statedb.sqlite3'
