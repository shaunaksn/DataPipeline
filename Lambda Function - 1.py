#This code reads the files from a s3 bucket, converts it into parquet files and loads it into another s3 bucket

import json
import boto3 
import awswrangler as wr

def lambda_handler(event, context):
    
    def csv_to_df(file_name):
        my_session = boto3.session.Session(aws_access_key_id = 'aws-access-key', aws_secret_access_key ='aws-secret-access-key')
        df = wr.s3.read_csv(path = f's3://s3-bucket-name/{file_name}',boto3_session = my_session , sep='|',  skip_blank_lines=True,header = 0,skipinitialspace = True,encoding = "UTF-8")
        return df
        
    
    def excel_to_dataframe(name):
        my_session = boto3.session.Session(aws_access_key_id = 'aws-access-key', aws_secret_access_key ='aws-secret-access-key' )
        df = wr.s3.read_excel(path = f"s3://s3-bucket-name/{name}",boto3_session = my_session )
        return df
    
    def df_to_parquet_to_s3(df,name):
        file = ""
        if ".csv" in name:
           file = name.replace(".csv","")
        elif ".xlsx" in name:
           file = name.replace(".xlsx","")
        path = f"s3://s3-bucket-name/{file}"
        my_session = boto3.session.Session(aws_access_key_id = 'aws-access-key', aws_secret_access_key ='aws-secret-access-key')    
        wr.s3.to_parquet(df = df, path = path,dataset = True, boto3_session = my_session, compression = None)    

    file_names = []
    client = boto3.client('s3',region_name = 'aws-region' ,aws_access_key_id = 'aws-access-key',aws_secret_access_key = 'aws-secret-access-key')
    response = client.list_objects_v2(Bucket = 's3-bucket-name')
    for key in (response['Contents']):
        file_names.append(key['Key'])   
        
    def to_s3(file_names):
        for i in file_names:
            if ".csv" in i:
               df = csv_to_df(i)
               df = df.fillna("Null")
               if i == 'dim_product_with_headers.csv':
                  df=df.drop(df.index[0])
               df_to_parquet_to_s3(df,i) 
               print("in csv")
            elif ".xlsx" in i:
               print(file_names)
               df2 = excel_to_dataframe(i)
               df_to_parquet_to_s3(df2,i)
               print("in xlsx")
                        
    to_s3(file_names)           
     
    return {
        'statusCode': 200,
        'body': "succesful"
    }
