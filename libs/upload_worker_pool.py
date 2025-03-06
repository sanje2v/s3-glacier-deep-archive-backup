import os.path
import logging
from functools import partial
from multiprocessing.pool import ThreadPool

import boto3
import boto3.session
from retry import retry

import settings
from utils import remove_file_ignore_errors


class UploadWorkerPool(ThreadPool):
    def __init__(self, num_workers): #, s3_region, s3_bucket_name, s3_path_prefix):
        super().__init__(num_workers)

        self.s3_region = 'sydney' #s3_region
        self.s3_bucket_name = 'mybucket' #s3_bucket_name
        self.s3_path_prefix = 'myfolder' #s3_path_prefix

        #self.worker_pool = ThreadPool(processes=num_workers)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self.join()
        #del self.worker_pool

    #@retry(Exception, **settings.UPLOAD_RETRY_ON_FAILURE_CONFIG)
    def _upload_worker(self, filename):
        logging.info(f'Uploading {filename}...')
        session = boto3.session.Session(aws_access_key_id='sanjeev',
                                        aws_secret_access_key='1GBequals1024MB',)
        s3_config = boto3.session.Config(max_pool_connections=2,
                                         retries={'max_attempts': 4, 'mode': 'standard'})
        s3_client = session.client('s3',
                                   endpoint_url='http://192.168.64.3:9000',
                                   config=s3_config)
        s3_client.upload_file(filename,
                              self.s3_bucket_name,
                              os.path.basename(filename),
                              ExtraArgs={}) #{'StorageClass': 'DEEP_ARCHIVE'})
        logging.info(f'Uploaded {filename}.')

    #@retry(Exception, **settings.UPLOAD_RETRY_ON_FAILURE_CONFIG)
    #def _upload_worker(self, filename):
    #    '''import time
    #    logging.info(f'{time.time()} - Uploading {filename}...')
    #    time.sleep(2.)
    #    import random
    #    if random.random() < 0.5:
    #        raise Exception('No internet connection.')
    #    logging.info(f'{time.time()} - Uploaded {filename}.')'''
    #    self._upload_work(filename)

    def _upload_error_callback(self, filename, exception):
        logging.error(f'Failed to upload {filename} with {exception}.')

    def put_on_upload_queue(self, filename, is_last):
        a = self.apply_async(self._upload_worker,
                            args=(filename,),
                            error_callback=partial(self._upload_error_callback, filename))
        if is_last:
            a.wait()