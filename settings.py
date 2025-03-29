import os.path
import logging
import tarfile


LOG_DIR = 'logs'
LOG_FILENAME = os.path.join(LOG_DIR, 'main.log')
MAX_LOG_SIZE_KILOBYTES = 10
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
            'maxBytes': (MAX_LOG_SIZE_KILOBYTES * 1024),
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

DEFAULT_NUM_UPLOAD_WORKERS = 4
DEFAULT_SPLIT_SIZE_GIGABYTES = 1
DEFAULT_TAR_COMPRESSION_TYPE = 'gz'

ENCRYPT_KEY_LENGTH = 32
TARFILE_FORMAT = tarfile.PAX_FORMAT
BUFFER_MEM_SIZE_BYTES = (5 * 1024 * 1024)     # Process this size block at a time

MAX_RETRY_ATTEMPTS = 4

S3_SERVER_URL = 'http://localhost:9000'