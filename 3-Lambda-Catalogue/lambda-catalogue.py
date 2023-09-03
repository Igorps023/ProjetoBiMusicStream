#Import libraries
import json
import boto3
import pandas as pd #layer
from io import StringIO
import os
import sys
import pytz #layer
import datetime
from datetime import datetime
from datetime import timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
 
def lambda_handler(event, context):
 agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
 dthproc = agora.strftime("%Y%m%d%H%M%S")
 
 # Create a Boto3 client for S3
 s3_client = boto3.client('s3')
 
 # Specify the S3 bucket and object key
 bucket_name = 'bkt-musicstream-bi'
 subfolder = 'Files/TransientZone/'
 
 # List objects in the subfolder
 response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=subfolder)
 
 # Extract file names and timestamps from the response
 files_info = [(os.path.basename(obj['Key']), obj['LastModified'], os.path.splitext(os.path.basename(obj['Key']))[1], obj.get('Size', 0), f"s3://{bucket_name}") for obj in response.get('Contents', []) if not obj['Key'].endswith('/')]
 
 # Format the timestamps and add them to the list of files_info
 formatted_files_info = []
 for file_name, timestamp, file_type, file_size, bucket_source in files_info:
     formatted_timestamp = timestamp.astimezone(pytz.timezone('America/Sao_Paulo')).strftime("%Y%m%d%H%M%S")
     formatted_file_size = f"{file_size / (1024 * 1024):.2f} MB" # Convert to MB and format to two decimal places
     formatted_files_info.append((file_name, formatted_timestamp, file_type, formatted_file_size, bucket_source))
     #print(f"File: {file_name}, Timestamp: {formatted_timestamp}, FileType: {file_type}, FileSize: {formatted_file_size}, BucketSource: {bucket_source}")

 
 
 # Create a DataFrame with the file names, timestamps, and file types
 df_new = pd.DataFrame(formatted_files_info, columns=["FileName", "IngestionTimestamp", "FileType", "FileSize (MB)", "BucketSource"])
 
 # Convert 'IngestionTimestamp' column in df_new to int64 data type to match df_existing
 df_new['IngestionTimestamp'] = df_new['IngestionTimestamp'].astype('int64')

 
 
 # Create a csv file and storage it in a variable
 csv_data_new = df_new.to_csv(index=False)
 
 # Specify the destination S3 bucket and file path
 destination_bucket_name = 'bkt-musicstream-bi'
 destination_file_path = 'Files/IngestionControl/Ingestion_Control_Table.csv'
 
 # Check if the file already exists in the destination bucket
 try:
     existing_data = s3_client.get_object(Bucket=destination_bucket_name, Key=destination_file_path)['Body'].read()
     existing_data_str = existing_data.decode('utf-8')
     df_existing = pd.read_csv(StringIO(existing_data_str))
 
     # Combine the DataFrames and remove duplicates based on the content
     df_combined = pd.concat([df_existing, df_new]).drop_duplicates()
 
     csv_data_combined = df_combined.to_csv(index=False)
 except s3_client.exceptions.NoSuchKey:
     # If the file doesn't exist, use the new data directly
     csv_data_combined = csv_data_new
 
 # Upload the combined CSV data to the destination bucket
 s3_client.put_object(Bucket=destination_bucket_name, Key=destination_file_path, Body=csv_data_combined)
 
 return {
     'statusCode': 200,
     'body': 'File with info.csv has been updated in the destination bucket.'
 }