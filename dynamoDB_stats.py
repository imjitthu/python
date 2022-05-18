#!/usr/bin/env python3.6

import boto3
import botocore
import urllib.parse
import logging
import os
import sys
import re
import base64
import json
from botocore.config import Config
from datetime import datetime
from os import linesep

# logging setup
loglevel = os.getenv('LOGLEVEL', 'DEBUG').upper()
logging.basicConfig(level=loglevel)
logger = logging.getLogger()
logger.setLevel(loglevel)

s3 = boto3.client('s3')
colsep = ','

# AWS X-Ray setup, before botocore?
if os.getenv("VZRELAY_STATS_XRAY_ENABLED", "1") == "1":
    try:
        import aws_xray_sdk.core
        aws_xray_sdk.core.patch_all()
    except Exception as e:
        logger.error(f"{e}"+str(traceback.format_exc()))

if os.getenv("VZRELAY_STATS_CODEGURU_PROFILER_ENABLED", "0") == "1":
    try:
        logger.info(f'enabling codeguru profiler to {os.getenv("AWS_CODEGURU_PROFILER_GROUP_NAME")}')
        from codeguru_profiler_agent import with_lambda_profiler
    except Exception as e:
        logger.error(f"{e}"+str(traceback.format_exc()))
else:
    logger.info(f'not enabling codeguru profiler')
    def with_lambda_profiler(func):
        def pn(*args, **kwargs):
            return func(*args, **kwargs)
        return pn

# more logging setup
logging.getLogger('botocore').setLevel(loglevel)
sys.tracebacklimit = 0

# get a client
ddb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_DEFAULT_REGION'))

def print_usage(e):
    logger.info("usage: VZRELAY_STATS_DYNAMODB_TABLE=db python3.6 dynamodb_stats.py")

def do_s3_stats(event, context):
    ddb_name = os.environ.get('VZRELAY_STATS_DYNAMODB_TABLE')
    if ddb_name == None:
        logger.error("need a valid VZRELAY_STATS_DYNAMODB_TABLE environment variable")
        return
    logger.debug(f"will save vzrelay/intake stats in dynamodb {ddb_name}")
    for record in event['Records']:
        event_time = None
        if 'eventTime' in record:
            event_time = record['eventTime']
        else:
            event_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        src_s3_bucket = record['s3']['bucket']['name']
        src_s3_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        logger.debug(f"will split {src_s3_key}: elements")
        path_elements = src_s3_key.split('/')
        logger.debug(path_elements)
        path_elements = path_elements[2:]
   
    # Get Records count from parquet file    
    req_fact =s3.select_object_content(
    Bucket = src_s3_bucket,
    Key = src_s3_key,
    ExpressionType = 'SQL',
    Expression = 'SELECT count(*) FROM s3object s',
    InputSerialization={'Parquet': {}},
    OutputSerialization = {'JSON': {'RecordDelimiter': 'n'}})
   
    for event in req_fact['Payload']:
        if 'Records' in event:
            rr=event['Records']['Payload'].decode('utf-8')
            for i, rec in enumerate(rr.split(linesep)):
                if rec:
                    row=rec.split(colsep)
                    if row:
                        record_count = row[0].split(':')[-1][:-2]
                        # print('File line count:', record_count)

        # search for company_id, if found, this is the transformed bucket
        while len(path_elements) and not re.match(r'vz_orgid=', path_elements[0]):
            path_elements = path_elements[1:]
        if not len(path_elements):
            path_elements = src_s3_key.split('/')
            # vzrelay-intake/in/ prefix, as we didn't  find a company_id=
            path_elements = path_elements[2:]

        # this would be so much more readable in perl
        # vz-org-id
        org_id   = re.split(r'=', path_elements[0], 1)
        org_id   = org_id[1] if len(org_id) == 2 else org_id[0]

        # customer secrm_id
        c_secrm_id = re.split(r'=', path_elements[1], 1)
        c_secrm_id = c_secrm_id[1] if len(c_secrm_id) == 2 else c_secrm_id[0]

        # rcp
        rcp      = re.split(r'=', path_elements[2], 1)
        rcp      = rcp[1] if len(rcp) == 2 else rcp[0]

        # device secrm_id
        d_secrm_id  = re.split(r'=', path_elements[3], 1)
        d_secrm_id  = d_secrm_id[1] if len(d_secrm_id) == 2 else d_secrm_id[0]

        # deviceid
        d_id  = re.split(r'=', path_elements[4], 1)
        d_id  = d_id[1] if len(d_id) == 2 else d_id[0]

        # logsource-info
        ls_info  = re.split(r'^logsource.*?=', path_elements[5], 1)
        ls_info  = ls_info[1] if len(ls_info) == 2 else ls_info[0]
        try:
            if re.match(r'==$', ls_info):
                logger.debug(f"base64 decode for {ls_info}")
                ls_info = base64.b64decode(ls_info)
            else:
                logger.debug(f"urlsafe_b64decode decode for {ls_info} but pad with == again")
                ls_info = base64.urlsafe_b64decode(ls_info+"==")
            # un-json this
            ls_info = json.loads(ls_info)
        except Exception as e:
            logger.error(f"problem base64/json decode for {ls_info}: {e}")

        # location
        ls_location = re.split(r'=', path_elements[6], 1)
        ls_location = ls_location[1] if len(ls_location) == 2 else ls_location[0]

        # logtype
        ls_ltype = re.split(r'=', path_elements[7], 1)
        ls_ltype = ls_ltype[1] if len(ls_ltype) == 2 else ls_ltype[0]

        # filename
        filename = path_elements[-1]

        logger.info(f'{{"s3-bucket":"{src_s3_bucket}","s3-key":"{src_s3_key}","vz-org-id":"{org_id}","c_secrm_id":"{c_secrm_id}","rcp":"{rcp}","deviceid":"{d_id}","logtype":"{ls_ltype}","logsource-info":"{ls_info}"}}')
        try:
            update_db_stats(ddb_name, org_id, c_secrm_id, rcp, d_secrm_id, d_id, ls_info, ls_location, ls_ltype, event_time, filename, record_count)
        except Exception as e:
            logger.error(f"{e}")
    return {
        'status': 'ok'
    }


def update_db_stats(ddb_name, org_id, c_secrm_id, rcp, d_secrm_id, d_id, ls_info, ls_location, ls_ltype, event_time, filename, record_count):
   
    dynamo_db_attr = {}
    dynamo_db_attr["action"] = "vzrelay-intake-stats-dynamodb-FIXME"
    dynamo_db_attr["vz-org-id"] = org_id
    dynamo_db_attr["logsource-id"] = c_secrm_id
    dynamo_db_attr["logtype"] = ls_ltype
    dynamo_db_attr["last-event-time"] = event_time
    dynamo_db_attr["device-lmid"] = c_secrm_id
    dynamo_db_attr["device-vzname"] = ls_info["dvn"]
    dynamo_db_attr["device-devicetype"] = ls_info["dt"]
    dynamo_db_attr["customer-secrmid"] = ls_info["csmid"]
    dynamo_db_attr["device-location"] = ls_location
    dynamo_db_attr["device-customername"] = ls_info["dn"]
    dynamo_db_attr["device-deviceid"] = d_id
    dynamo_db_attr["device-secrmid"] = d_secrm_id
    dynamo_db_attr["customer-rcp"] = rcp
    dynamo_db_attr["smc-id"] = ls_info["sid"]
    dynamo_db_attr["file-name"] = filename
    dynamo_db_attr["total-records"] = record_count
    put_item_in_table(ddb_name, dynamo_db_attr)

def put_item_in_table(dynamo_db_name, data: dict):
    """
    [summary]

    Args:
        data (dict): [description]

    Returns:
        [type]: [description]
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(dynamo_db_name)
    table.put_item(Item=data)

    return table

def main():
    try:
        do_s3_stats({"Records":[{"s3":{"bucket":{"name":sys.argv[1]},"object":{"key":sys.argv[2]}}}]}, None)
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
