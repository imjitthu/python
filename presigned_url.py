from posixpath import split
import sys
import urllib.parse
import boto3
import gzip
import logging
import os
import datetime

# set logging
loglevel = os.environ.get('Athena DynamoDB', 'INFO').upper()
logging.basicConfig(level=loglevel)
logger = logging.getLogger()
logger.setLevel(loglevel)

# get a client
s3 = boto3.client('s3')
ddb = boto3.client('dynamodb')
athena = boto3.client('athena')
dynamodb = boto3.resource('dynamodb')

# define variables
ddb_table = dynamodb.Table(os.environ.get('DYNAMODB_TABLE_NAME'))
timeStamp = datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S")

source_prefix = "queries"
target_prefix = "compressed"
gz_ext = ".gz"

def compress_files(event, context):
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        s3_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        if not s3_key.__contains__('.metadata'):
            s3_obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
            file_object = s3_obj['Body'].read()
            gz =  gzip.compress(file_object)
            gz_size = sys.getsizeof(gz)
            filename = os.path.split(s3_key)[-1]
            logger.info(f'compressed file name: {filename}{gz_ext}')
            file = f'{target_prefix}/{filename}{gz_ext}'
            s3.put_object(Body=gz, Bucket=s3_bucket, Key=file)
            logger.info(f'gz file successfully uploaded at: s3://{s3_bucket}/{file}')
            s3.delete_object(Bucket=s3_bucket, Key=s3_key)
            logger.info(f'deleted s3 object: s3://{s3_bucket}/{s3_key}')
            logger.info(f'Updating DynamoDB Table with s3URL')
            s3_url = f'{target_prefix}/{filename}{gz_ext}'
            queryID = filename.split('.')[0]
            logger.info(f'Athena Query ID / Execution ID: {queryID}')
            executionID = athena.batch_get_query_execution(QueryExecutionIds=[queryID])
            completedtimestamp = executionID['QueryExecutions'][0]['Status']['CompletionDateTime']
            completedtimestamp = completedtimestamp.strftime("%Y-%m-%d %I:%M:%S")
            executiIDonstatus = executionID['QueryExecutions'][0]['Status']['State']
            response = ddb_table.update_item(
                Key={'queryId': queryID},
                UpdateExpression='SET completedOn = :val1, filename = :val2, lastTimeStamp = :val3, queryStatus = :val4, s3URL = :val5, size = :val6, downloadStatus=:val7',
                ExpressionAttributeValues={
                    ':val1': completedtimestamp,
                    ':val2': filename+gz_ext,
                    ':val3': timeStamp,
                    ':val4': executiIDonstatus,
                    ':val5': s3_url,
                    ':val6': gz_size,
                    ':val7': 'Completed'
                }
            )
            logger.info(f'Updated DynamoDB Table: {response}')