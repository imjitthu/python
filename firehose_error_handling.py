import json
import boto3
import botocore
import logging
import os

#import s3fs
from io import StringIO 
from io import BytesIO
import sys
from datetime import datetime, timedelta

import re
import time
from datetime import datetime, timedelta
import awswrangler as wr 
# import pandas as pd
# from pyarrow import json
# import pyarrow.parquet as pq

######### PARAMS #########################
params = {
    'S3_PREFIX_FOLDER': 'errors/',
    'FIREHOSE_ERROR_BUCKET':os.environ.get('FIREHOSE_ERROR_BUCKET'),
    'KINESIS_DATA_STREAM':os.environ.get('KINESIS_DATA_STREAM')
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)

s3_client = boto3.client('s3')
#s3 = s3fs.S3FileSystem()
s3 = boto3.resource('s3')
firehose = boto3.client("firehose")
kinesis_client = boto3.client('kinesis')
sys.tracebacklimit = 0


#######################################################################
## Get list of folders and sub folders##
#######################################################################
## Get list of folders and sub folders
def list_folders(s3_client,bucket_name):
    #print("Inside list_folders() method")
    folders = set()
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=params['S3_PREFIX_FOLDER'],MaxKeys=100)

    lst = response.get('Contents', [])
    is_truncated = response.get('IsTruncated')
    #print(lst)
    #print("listing info is_truncated: "+str(is_truncated)+", list size: "+str(len(lst)))
    while is_truncated or len(lst):
        for content in response['Contents']:
            #print(os.path.dirname(content['Key']))
            folders.add(os.path.dirname(content['Key']))
        if is_truncated:
            #print("Error in is_truncated login")
            next_lst_marker = lst[-1]["Key"]
            response = s3_client.list_objects(Bucket=bucket_name, Prefix=params['S3_PREFIX_FOLDER'],MaxKeys=100, Marker=next_lst_marker)
            lst = response.get('Contents', [])
            is_truncated = response.get('IsTruncated')
            #print(lst)
            #print("listing info is_truncated: "+str(is_truncated)+", list size:"+str(len(lst)))
        else:
            lst = []
    #print("End list_folders() method")
    return sorted(folders)

#######################################################################
## Process each file and send records to Datastream batch##
#######################################################################
def retry_FirehoseErrorRecords(s3_client,bucket_name):
    print("Inside retry_FirehoseErrorRecords() method")
    folder_list = list_folders(s3_client, bucket_name)
    s3List = []
    for s3folder in folder_list:
        #print("Element is ",s3folder)
        if(s3folder == ''):
            continue;
        string = s3folder.replace('', "")
        s3List.append(string.rstrip('/'))
    #print("s3List()",s3List)
    ## Retrieve individual records
    for prefix_key in s3List:
        print('Prefix key',prefix_key)
        for records in s3_client.list_objects(Bucket=bucket_name,Prefix=prefix_key)['Contents']:
            record = records['Key']
            print("record is ",record)
            dfs = wr.s3.read_json(path='s3://'+bucket_name+'/'+record,chunksize=100,lines=True)
            for df in dfs:
                print("1")
                send_record_kinesis(df)
            ##remove the already read file
            print("Deleting the s3 object, full path is: "+record)
            s3_client.delete_object(Bucket=bucket_name, Key=record)
            print("S3 objects deleted, full path is: "+record)

    print("End retry_FirehoseErrorRecords() method")

#######################################################################
## Send record to Datastream batch##
#######################################################################
    
def send_record_kinesis(df):
    records = []
    file_rec_count = 0
    count = 1
    for ind in df.index:
        firehose_data = df['rawData'][ind]
        if firehose_data is None:
            print("firehose_data is not available")
            break
        if count % 500 == 0:
            #print('Starting Batch Loop')
            #print("Size of Batch is", sys.getsizeof(records))
            response = kinesis_client.put_records(StreamName = params['KINESIS_DATA_STREAM'], Records = records)
            file_rec_count = file_rec_count + len(records)
                                        
            # Re-submit the Batch if the above step fails
            submit_batch_until_successful(kinesis_client,records,response)

            #print(response)
            #print(len(records))
            records.clear()
        #firehose_data = df['rawData'][ind]
        single_record = {'Data':firehose_data, 'PartitionKey': str(hash(firehose_data))}
        records.append(single_record)
        count = count + 1

    if len(records) > 0:
        #print('Starting Final Batch')
        #print('No. of records in Final Batch', len(records))
        #print("Size of Final Batch is", sys.getsizeof(records))                    
        response = kinesis_client.put_records(StreamName = params['KINESIS_DATA_STREAM'], Records = records)
        print("4")
        file_rec_count = file_rec_count + len(records)
        submit_batch_until_successful(kinesis_client,records,response)
        
        


#######################################################################
## Submit batch of records to Datastream batch##
#######################################################################
    
def submit_batch_until_successful(kinesis_client,records, response):
    """If needed, retry a batch of records, backing off exponentially until it goes through"""
    retry_interval = 0.5
    failed_record_count = response['FailedRecordCount']
    
    #print('Failed record count {}'.format(failed_record_count))
    while failed_record_count:
        time.sleep(retry_interval)

        # Failed records don't contain the original contents - 
        # we have to correlate with the input by position
        failed_records = [records[i] for i, record in enumerate(response['Records']) if 'ErrorCode' in record]
        
        print('Incrementing exponential back off and retrying {} failed records'.format(str(len(failed_records))))
        retry_interval = min(retry_interval * 2, 10)
        # request = {
        #     'Records': failed_records,
        #     'StreamName': 'workflow_data_stream'
        # }

        response = kinesis_client.put_records(
                StreamName = params['KINESIS_DATA_STREAM'],
                Records= failed_records)
        failed_record_count = response['FailedRecordCount']
        print('Failed Records Count after resubmission is', failed_record_count)
    

###############################################################################
## Processing the Firehose S3 Error records ##
###############################################################################
def processPartition(event, context):
    try:
        bucketName = params['FIREHOSE_ERROR_BUCKET']
        print("Firehose S3 Bucket Name is: " + bucketName)
        # s3_client = boto3.client('s3')
        retry_FirehoseErrorRecords(s3_client,bucketName)
        print("Processing Firehose S3 bucket: done")
    except botocore.exceptions.ClientError as e:
        logger.error(f"{e}")
        sys.exit(1)

################# MAIN function call#############################################
def main():
    try:
        r = processPartition(None, None)
        logger.info(r)
        sys.exit(0)
    except KeyboardInterrupt:
        pass
    except botocore.exceptions.ClientError as e:
        logger.error(f"{e}")
        sys.exit(1)
    except (ValueError, IndexError, TypeError) as e:
        logger.error(f"{e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"{e}")
        sys.exit(1)
        
if __name__ == "__main__":
    main()