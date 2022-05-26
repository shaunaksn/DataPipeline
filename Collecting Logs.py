import json
import boto3
import time
from datetime import datetime, timedelta

def lambda_handler(event, context):
    region_name = 'aws-region'
    s3_bucket_destination = 's3-bucket-name'
    date_to_use = datetime.now() - timedelta(0)
    
    # Converting into UNIX time in milliseconds
    log_collection_start_time = int(date_to_use.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    log_collection_end_time = int(date_to_use.replace(hour=23, minute=59, second=59, microsecond=999999).timestamp() * 1000)
    
    cloudwatch_log_client = boto3.client('logs')
    cloudwatch_response = cloudwatch_log_client.describe_log_groups()
    
    # List of CloudWatch Log groups
    log_group_names_list = []
    for name in cloudwatch_response['logGroups']:
        log_group_names = name['logGroupName']
        log_group_names_list.append(log_group_names)
    print(log_group_names_list)
    
    # Creating an export task to export logs to a s3 bucket
    for group_name in log_group_names_list:
        print(group_name)
        cloudwatch_export_response = cloudwatch_log_client.create_export_task(    
        taskName='log_export_to_s3',
        logGroupName=group_name,
        logStreamNamePrefix='2022',
        fromTime=log_collection_start_time,
        to=log_collection_end_time,
        destination=s3_bucket_destination,
        destinationPrefix='exported_logs'
        )
        time.sleep(15)
