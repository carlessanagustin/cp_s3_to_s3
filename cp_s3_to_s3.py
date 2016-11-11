import boto3
import botocore
import os
from botocore.client import Config
import argparse
import time
from datetime import timedelta
from vars import *

start_time = time.time()

# arguments
parser = argparse.ArgumentParser(description='Copy from account A AWS S3 to account B AWS S3')
parser.add_argument('filename', metavar='path/filename', type=str, nargs=1,
                    help='Path and filename of the files to copy.')
args = parser.parse_args()
source_file = args.filename[0]

# connection to aws s3 accounts
conn_dest = boto3.client('s3', 'eu-west-1', use_ssl=True, verify=None,
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
                               CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})

# process
lines = [line.rstrip('\n') for line in open(source_file)]
for line in lines:
    source_file_name = os.path.basename(line)
    source_file_path = os.path.dirname(line)
    destination_file_path = line.split('/')[5]

    conn_orig.download_file(orig_bucket_name,
                              source_file_path+'/'+source_file_name,
                              source_file_name)
    conn_dest.upload_file(source_file_name,
                             dest_bucket_name,
                             dest_pre_path+destination_file_path+'/'+source_file_name)
    os.remove(source_file_name)

print 'Process time: ' + str(timedelta(seconds=time.time() - start_time))
