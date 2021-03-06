import boto3
import botocore
import os
from botocore.client import Config
import argparse
import time
from datetime import timedelta
import logging
from sys import exit
from logging.handlers import RotatingFileHandler
from vars import *

start_time = time.time()

# arguments
parser = argparse.ArgumentParser(description='Copy from account A AWS S3 to account B AWS S3')
parser.add_argument('filename', metavar='path/filename', type=str, nargs=1,
                    help='Path and filename of the files to copy.')
args = parser.parse_args()
source_file = args.filename[0]


def disk_usage(path):
    '''% occupied hdd space'''
    st = os.statvfs(path)
    # free = (st.f_bavail * st.f_frsize)
    total = (st.f_blocks * st.f_frsize)
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    try:
        percent = (float(used) / total) * 100
    except ZeroDivisionError:
        percent = 0
    return round(percent, 1)


# log
my_logger = logging.getLogger(source_file)
my_logger.setLevel(logging.INFO)
my_handler = logging.handlers.RotatingFileHandler(
              log_file, maxBytes=1048576*256, backupCount=5)
my_handler.setLevel(logging.INFO)
my_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
my_logger.addHandler(my_handler)

# connection to aws s3 accounts
conn_dest = boto3.client('s3', dest_bucket_zone, use_ssl=True, verify=None,
                            aws_access_key_id=dest_aws_access_key_id,
                            aws_secret_access_key=dest_aws_secret_access_key,
                            config=Config(s3={'addressing_style': 'path'}))

conn_orig = boto3.client('s3', use_ssl=True, verify=None,
                           aws_access_key_id=orig_aws_access_key_id,
                           aws_secret_access_key=orig_aws_secret_access_key,
                           config=Config(s3={'addressing_style': 'path'}))

# does bucket exist ?
exists = True
try:
    conn_dest.head_bucket(Bucket=dest_bucket_name)
except botocore.exceptions.ClientError as e:
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

if not exists:
    conn_dest.create_bucket(Bucket=dest_bucket_name,
                               CreateBucketConfiguration={'LocationConstraint': dest_bucket_zone})

# process: download, upload, delete
lines = [line.rstrip('\n') for line in open(source_file)]
for line in lines:
    source_file_name = os.path.basename(line)
    source_file_path = os.path.dirname(line)
    destination_file_path = line.split('/')[5]

    if disk_usage('/') >= max_pct:
        my_logger.error('Disk space limit failure.')
        exit(0)

    print 'Processing: '+source_file_path+'/'+source_file_name
    try:
        conn_orig.download_file(orig_bucket_name,
                                source_file_path+'/'+source_file_name,
                                source_file_name)
        conn_dest.upload_file(source_file_name,
                             dest_bucket_name,
                             dest_pre_path+destination_file_path+'/'+source_file_name)
        os.remove(source_file_name)
    except Exception, e:
        my_logger.info('FAIL: '+source_file+' :: '+source_file_path+'/'+source_file_name)
        my_logger.error('Dump message: '+ str(e))
        exit(0)

total_time = 'Process time: ' + str(timedelta(seconds=time.time() - start_time))
my_logger.info(str(args)+' : '+total_time)
print total_time
