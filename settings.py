import os.path
import logging

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

UPLOAD_RETRY_ON_FAILURE_CONFIG = {
    'tries': 3,
    'delay': 5
}

DEFAULT_NUM_UPLOAD_WORKERS = 4
DEFAULT_SPLIT_SIZE_KILOBYTES = 1024 * 1024
DEFAULT_TAR_COMPRESSION_TYPE = 'gz'