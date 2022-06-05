# This code reads the parquet files from a s3 bucket, creates snowflake tables with some new columns and copies the data from parquet files to Snowflake tables

import snowflake.connector as sf
import os


# This function is used to run query on snowflake
def run_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()



def lambda_handler(event, context):
    #snowflake-Credentials
    pravin=[os.environ['uname_pravin'],os.environ['password_pravin'],os.environ['uid_pravin']]
    shaunak=[os.environ['uname_shaunak'],os.environ['password_shaunak'],os.environ['uid_shaunak']]
    abi=[os.environ['uname_shaunak'],os.environ['password_shaunak'],os.environ['uid_shaunak']]
    l1=[pravin,shaunak,abi]
    
    
    
    for i in l1:
	#Creating a Connection with snowflake account
        user=i[0]
        password=i[1]
        account=i[2]
        conn=sf.connect(user=user,password=password,account=account);
        
        
        
        
        #Snowflake-Platform-creation
        statement_1='use warehouse COMPUTE_WH;'
        run_query(conn,statement_1);
        
        statement2="use role SYSADMIN;"
        run_query(conn,statement2);
        
        statement3 = "CREATE or REPLACE database PROJECT_DB;"
        run_query(conn, statement3);
        
        statement4="use database PROJECT_DB;"
        run_query(conn,statement4);
        
	# Creating schemas for loading parquet files
	
	# File_format_schema
        sql_query_file_format_schema_creation = "CREATE or REPLACE schema PROJECT_DB.File_Format;"
        run_query(conn, sql_query_file_format_schema_creation);
        
	#External_stage_schema
        sql_query_external_stage_schema="CREATE or REPLACE schema PROJECT_DB.External_stage;"
        run_query(conn, sql_query_external_stage_schema);
        
	#Data_Loaded_Schema
        sql_query_loaded_table_schema_creation = "CREATE or REPLACE schema PROJECT_DB.loaded_tables;"
        run_query(conn, sql_query_loaded_table_schema_creation);
        
	#Creating File-Format for reading Parquet
        sql_query_file_format="CREATE or REPLACE FILE FORMAT PROJECT_DB.File_Format.PARAQUET_FF TYPE='PARQUET' COMPRESSION=NONE BINARY_AS_TEXT=TRUE;"
        run_query(conn, sql_query_file_format);
        
        
        
        #AWS-Credentials 
        aws_key=os.environ['aws_key_id']
        aws_pwd=os.environ['aws_secret_key']
	ff='PROJECT_DB.File_Format.PARAQUET_FF'
                
        
	
	
        #Dim_Policy
	# Creating stage to read dim_policy.parquet file from s3 stage bucket
        sql_querry_dim_policy_stage_creation="CREATE OR REPLACE STAGE PROJECT_DB.External_stage.PARAQUET_STG URL = 's3://mini-project-s3-stage-bucket/dim_policy_with_header/' CREDENTIALS = (AWS_KEY_ID = '"+aws_key+"' AWS_SECRET_KEY = '"+aws_pwd+"') file_format = "+ff+";"
        run_query(conn, sql_querry_dim_policy_stage_creation);
        
	# Creating table dim_policy in snowflake along with the data warehousing columns to load the dim_policy data from s3 stage bucket to snowflake table DIM_POLICY 
        sql_querry_dim_policy_table="CREATE OR REPLACE TABLE PROJECT_DB.loaded_tables.DIM_POLICY (LOADDATE TIMESTAMP_TZ,POLICY_ID int,POL_ACCOUNT varchar(50),POL_ACCOUNTDESCRIPTION varchar(256),POL_ASL varchar(5),POL_AUTOMATEDAPPROVALPOLICY inT,POL_BINDERISSUEDDATE TIMESTAMP_TZ,POL_CONVERSIONINDCODE varchar(5),POL_GENERATIONSOURCE varchar(100),POL_ISSUEDDATE TIMESTAMP_TZ,POL_MASTERCOUNTRY varchar(50),POL_MASTERSTATE varchar(50),POL_ORIGINALEFFECTIVEDATE TIMESTAMP_TZ,POL_POLICYNUMBER varchar(50),POL_POLICYNUMBERPREFIX varchar(10),POL_POLICYNUMBERSUFFIX varchar(10),POL_QUOTEDDATE TIMESTAMP_TZ,POL_UNIQUEID varchar(100),RECORD_VERSION int,SOURCE_SYSTEM varchar(100),VALID_FROMDATE TIMESTAMP_TZ,VALID_TODATE TIMESTAMP_TZ,EDH_ROW_HASH_NBR number, EDH_DML_IND VARCHAR(50) DEFAULT 'I',EDH_CREAT_TS TIMESTAMP_NTZ, EDH_UPDT_TS TIMESTAMP_NTZ, EDH_IS_C_FLG BOOLEAN DEFAULT TRUE, EDH_REPLICATION_SEQ_NBR VARCHAR(10) DEFAULT '0',EDH_LINEAGE_ID VARCHAR(10) DEFAULT '0', EDH_MOD_BY_USR_NAM VARCHAR(50));"
        run_query(conn,sql_querry_dim_policy_table);
        
	# Loading the data to snowflake table DIM_POLICY from s3 stage bucket dim_policy.parquet
        sql_querry_copy_data_dim_policy="copy into PROJECT_DB.loaded_tables.DIM_POLICY from (select $1:LOADDATE::TIMESTAMP_TZ,$1:POLICY_ID::INT,$1:POL_ACCOUNT::varchar(50),$1:POL_ACCOUNTDESCRIPTION::varchar(256),$1:POL_ASL::varchar(5),$1:POL_AUTOMATEDAPPROVALPOLICY::INT,$1:POL_BINDERISSUEDDATE::TIMESTAMP_TZ,$1:POL_CONVERSIONINDCODE::varchar(5),$1:POL_GENERATIONSOURCE::varchar(100),$1:POL_ISSUEDDATE::TIMESTAMP_TZ,$1:POL_MASTERCOUNTRY::varchar(50),$1:POL_MASTERSTATE::varchar(50),$1:POL_ORIGINALEFFECTIVEDATE::TIMESTAMP_TZ,$1:POL_POLICYNUMBER::varchar(50),$1:POL_POLICYNUMBERPREFIX::varchar(10),$1:POL_POLICYNUMBERSUFFIX::varchar(10),$1:POL_QUOTEDDATE::TIMESTAMP_TZ,$1:POL_UNIQUEID::varchar(100),$1:RECORD_VERSION:: INT,$1:SOURCE_SYSTEM::varchar(100),$1:VALID_FROMDATE::TIMESTAMP_TZ,$1:VALID_TODATE::TIMESTAMP_TZ,hash(*),'I',current_date(),current_date(), TRUE,'0','0', current_user() from @PROJECT_DB.External_stage.PARAQUET_STG);"
        run_query(conn,sql_querry_copy_data_dim_policy);
        
        
        
        
        #Dim_Product
	# Creating stage to read dim_product.parquet file from s3 stage bucket
        sql_querry_dim_product_stage_creation="CREATE OR REPLACE STAGE PROJECT_DB.External_stage.DIM_PRODUCT_STG URL = 's3://mini-project-s3-stage-bucket/dim_product_with_headers/' CREDENTIALS = (AWS_KEY_ID = '"+aws_key+"' AWS_SECRET_KEY = '"+aws_pwd+"') file_format = "+ff+";"
        run_query(conn, sql_querry_dim_product_stage_creation);
    	
	# Creating a table dim_product in snowflake along with the data warehousing columns to load the dim_product data from s3 stage bucket to snowflake table dim_product 
        sql_querry_dim_product_table="CREATE OR REPLACE TABLE PROJECT_DB.loaded_tables.DIM_PRODUCT (LOADDATE TIMESTAMP_TZ, PRDT_DESCRIPTION varchar(2000),PRDT_GROUP varchar(100), PRDT_LOB varchar(50)  ,PRDT_NAME varchar(100)  ,PRODUCT_ID int  ,PRODUCT_UNIQUEID varchar(100)  ,SOURCE_SYSTEM varchar(100),EDH_ROW_HASH_NBR number, EDH_DML_IND VARCHAR(50) DEFAULT 'I',EDH_CREAT_TS TIMESTAMP_NTZ, EDH_UPDT_TS TIMESTAMP_NTZ, EDH_IS_C_FLG BOOLEAN DEFAULT TRUE, EDH_REPLICATION_SEQ_NBR VARCHAR(10) DEFAULT '0',EDH_LINEAGE_ID VARCHAR(10) DEFAULT '0', EDH_MOD_BY_USR_NAM VARCHAR(50));"
        run_query(conn,sql_querry_dim_product_table);
        
	# Loading the data to snowflake table DIM_PRODUCT from s3 stage bucket dim_product.parquet.
        sql_querry_copy_data_dim_product="copy into PROJECT_DB.loaded_tables.DIM_PRODUCT from (select $1:LOADDATE::TIMESTAMP_TZ,$1:PRDT_DESCRIPTION::VARCHAR(2000),$1:PRDT_GROUP::VARCHAR(100),$1:PRDT_LOB::VARCHAR(50),$1:PRDT_NAME::VARCHAR(100),$1:PRODUCT_ID::INT,$1:PRODUCT_UNIQUEID::VARCHAR(100),$1:SOURCE_SYSTEM::VARCHAR(100),hash(*),'I',current_date(),current_date(), TRUE,'0','0', current_user() from @PROJECT_DB.External_stage.DIM_PRODUCT_STG);"
        run_query(conn,sql_querry_copy_data_dim_product);
        
        
        
        
        #Fact-Policy
	# Creating stage to read fact_policy.parquet file from s3 stage bucket.
        sql_querry_fact_policy_stage_creation="CREATE OR REPLACE STAGE PROJECT_DB.External_stage.FACT_POLICY_STAGE URL = 's3://mini-project-s3-stage-bucket/FACT_POLICY_202202251501/' CREDENTIALS = (AWS_KEY_ID = '"+aws_key+"' AWS_SECRET_KEY = '"+aws_pwd+"') file_format = "+ff+";"
        run_query(conn,sql_querry_fact_policy_stage_creation);
    	
	# Creating a table dim_product in snowflake along with the data warehousing columns to load the fact_policy data from s3 stage bucket to snowflake table fact_policy.
        sql_querry_fact_policy_table="CREATE OR REPLACE TABLE PROJECT_DB.loaded_tables.FACT_POLICY(AUDIT_PREM_AMT numeric(13,2),AUDIT_PREM_AMT_ITD numeric(13,2),AUDIT_PREM_AMT_YTD numeric(13,2),CNCL_PREM_AMT numeric(13,2) ,CNCL_PREM_AMT_ITD numeric(13,2) ,CNCL_PREM_AMT_YTD numeric(13,2) ,COMM_AMT numeric(13,2) ,COMM_AMT_ITD numeric(13,2) ,COMM_AMT_YTD numeric(13,2) ,COMM_EARNED_AMT numeric(13,2) ,COMM_EARNED_AMT_ITD numeric(13,2) ,COMM_EARNED_AMT_YTD numeric(13,2) ,COMPANY_ID int,EARNED_PREM_AMT numeric(13,2) ,EARNED_PREM_AMT_ITD numeric(13,2) ,EARNED_PREM_AMT_YTD numeric(13,2) ,ENDORSE_PREM_AMT numeric(13,2) ,ENDORSE_PREM_AMT_ITD numeric(13,2) ,ENDORSE_PREM_AMT_YTD numeric(13,2) ,FACTPOLICY_ID int,FEES_AMT numeric(13,2) ,FEES_AMT_ITD numeric(13,2) ,FEES_AMT_YTD numeric(13,2) ,FIFTHINSURED_ID int,FIRSTINSURED_ID int,FOURTHINSURED_ID int,GROSS_EARNED_PREM_AMT numeric(13,2) ,GROSS_EARNED_PREM_AMT_ITD numeric(13,2) ,GROSS_EARNED_PREM_AMT_YTD numeric(13,2) ,GROSS_WRTN_PREM_AMT numeric(13,2) ,GROSS_WRTN_PREM_AMT_ITD numeric(13,2) ,GROSS_WRTN_PREM_AMT_YTD numeric(13,2) ,INSUREDAGE int ,LOADDATE datetime ,MAN_WRTN_PREM_AMT numeric(13,2) ,MAN_WRTN_PREM_AMT_ITD numeric(13,2) ,MAN_WRTN_PREM_AMT_YTD numeric(13,2) ,MONTH_ID int,ORIG_WRTN_PREM_AMT numeric(13,2) ,ORIG_WRTN_PREM_AMT_ITD numeric(13,2) ,ORIG_WRTN_PREM_AMT_YTD numeric(13,2) ,POLICYCANCELLEDISSUEDIND int ,POLICYEFFECTIVEDATE_ID int ,POLICYENDORSEMENTISSUEDIND int ,POLICYEXPIRATIONDATE_ID int ,POLICYEXTENSION_ID int,POLICYMASTERTERRITORY_ID int ,POLICYNEWISSUEDIND int ,POLICYNEWORRENEWAL varchar(10),POLICYNONRENEWALISSUEDIND int ,POLICYRENEWEDISSUEDIND int ,POLICYSTATUS_ID int ,POLICY_ID int,POLICY_UNIQUEID varchar(100),PRODUCER_ID int,PRODUCT_ID int,REIN_PREM_AMT numeric(13,2) ,REIN_PREM_AMT_ITD numeric(13,2) ,REIN_PREM_AMT_YTD numeric(13,2) ,SECONDINSURED_ID int,SOURCE_SYSTEM varchar(100),TAXES_AMT numeric(13,2) ,TAXES_AMT_ITD numeric(13,2) ,TAXES_AMT_YTD numeric(13,2) ,TERM_PREM_AMT numeric(13,2) ,TERM_PREM_AMT_ITD numeric(13,2) ,TERM_PREM_AMT_YTD numeric(13,2) ,THIRDINSURED_ID int,UNDERWRITER_ID int,UNEARNED_PREM numeric(13,2) ,USR_DEF_SUM1 numeric(13,2) ,USR_DEF_SUM10 numeric(13,2) ,USR_DEF_SUM10_ITD numeric(13,2) ,USR_DEF_SUM10_YTD numeric(13,2) ,USR_DEF_SUM11 numeric(13,2) ,USR_DEF_SUM11_ITD numeric(13,2) ,USR_DEF_SUM11_YTD numeric(13,2) ,USR_DEF_SUM12 numeric(13,2) ,USR_DEF_SUM12_ITD numeric(13,2) ,USR_DEF_SUM12_YTD numeric(13,2) ,USR_DEF_SUM13 numeric(13,2) ,USR_DEF_SUM13_ITD numeric(13,2) ,USR_DEF_SUM13_YTD numeric(13,2) ,USR_DEF_SUM14 numeric(13,2) ,USR_DEF_SUM14_ITD numeric(13,2) ,USR_DEF_SUM14_YTD numeric(13,2) ,USR_DEF_SUM15 numeric(13,2) ,USR_DEF_SUM15_ITD numeric(13,2) ,USR_DEF_SUM15_YTD numeric(13,2) ,USR_DEF_SUM1_ITD numeric(13,2) ,USR_DEF_SUM1_YTD numeric(13,2) ,USR_DEF_SUM2 numeric(13,2) ,USR_DEF_SUM2_ITD numeric(13,2) ,USR_DEF_SUM2_YTD numeric(13,2) ,USR_DEF_SUM3 numeric(13,2) ,USR_DEF_SUM3_ITD numeric(13,2) ,USR_DEF_SUM3_YTD numeric(13,2) ,USR_DEF_SUM4 numeric(13,2) ,USR_DEF_SUM4_ITD numeric(13,2) ,USR_DEF_SUM4_YTD numeric(13,2) ,USR_DEF_SUM5 numeric(13,2) ,USR_DEF_SUM5_ITD numeric(13,2) ,USR_DEF_SUM5_YTD numeric(13,2) ,USR_DEF_SUM6 numeric(13,2) ,USR_DEF_SUM6_ITD numeric(13,2) ,USR_DEF_SUM6_YTD numeric(13,2) ,USR_DEF_SUM7 numeric(13,2) ,USR_DEF_SUM7_ITD numeric(13,2) ,USR_DEF_SUM7_YTD numeric(13,2) ,USR_DEF_SUM8 numeric(13,2) ,USR_DEF_SUM8_ITD numeric(13,2) ,USR_DEF_SUM8_YTD numeric(13,2) ,USR_DEF_SUM9 numeric(13,2) ,USR_DEF_SUM9_ITD numeric(13,2) ,USR_DEF_SUM9_YTD numeric(13,2) ,WRTN_PREM_AMT numeric(13,2) ,WRTN_PREM_AMT_ITD numeric(13,2) ,WRTN_PREM_AMT_YTD numeric(13,2),EDH_ROW_HASH_NBR number, EDH_DML_IND VARCHAR(50) DEFAULT 'I',EDH_CREAT_TS TIMESTAMP_NTZ, EDH_UPDT_TS TIMESTAMP_NTZ, EDH_IS_C_FLG BOOLEAN DEFAULT FALSE, EDH_REPLICATION_SEQ_NBR VARCHAR(10) DEFAULT '0',EDH_LINEAGE_ID VARCHAR(10) DEFAULT '0', EDH_MOD_BY_USR_NAM VARCHAR(50) );"
        run_query(conn,sql_querry_fact_policy_table);
        
	# Loading the data to snowflake table fact_policy from s3 stage bucket fact_policy.parquet.
        sql_querry_copy_data_fact_policy="copy into PROJECT_DB.loaded_tables.FACT_POLICY from (select $1:AUDIT_PREM_AMT ::numeric(13,2) ,$1:AUDIT_PREM_AMT_ITD ::numeric(13,2) ,$1:AUDIT_PREM_AMT_YTD ::numeric(13,2) ,$1:CNCL_PREM_AMT ::numeric(13,2) ,$1:CNCL_PREM_AMT_ITD ::numeric(13,2) ,$1:CNCL_PREM_AMT_YTD ::numeric(13,2) ,$1:COMM_AMT ::numeric(13,2) ,$1:COMM_AMT_ITD ::numeric(13,2) ,$1:COMM_AMT_YTD ::numeric(13,2) ,$1:COMM_EARNED_AMT ::numeric(13,2) ,$1:COMM_EARNED_AMT_ITD ::numeric(13,2) ,$1:COMM_EARNED_AMT_YTD ::numeric(13,2) ,$1:COMPANY_ID ::int,$1:EARNED_PREM_AMT ::numeric(13,2) ,$1:EARNED_PREM_AMT_ITD ::numeric(13,2) ,$1:EARNED_PREM_AMT_YTD ::numeric(13,2) ,$1:ENDORSE_PREM_AMT ::numeric(13,2) ,$1:ENDORSE_PREM_AMT_ITD ::numeric(13,2) ,$1:ENDORSE_PREM_AMT_YTD ::numeric(13,2) ,$1:FACTPOLICY_ID ::int,$1:FEES_AMT ::numeric(13,2) ,$1:FEES_AMT_ITD ::numeric(13,2) ,$1:FEES_AMT_YTD ::numeric(13,2) ,$1:FIFTHINSURED_ID ::int,$1:FIRSTINSURED_ID ::int,$1:FOURTHINSURED_ID ::int,$1:GROSS_EARNED_PREM_AMT ::numeric(13,2) ,$1:GROSS_EARNED_PREM_AMT_ITD ::numeric(13,2) ,$1:GROSS_EARNED_PREM_AMT_YTD ::numeric(13,2) ,$1:GROSS_WRTN_PREM_AMT ::numeric(13,2) ,$1:GROSS_WRTN_PREM_AMT_ITD ::numeric(13,2) ,$1:GROSS_WRTN_PREM_AMT_YTD ::numeric(13,2) ,$1:INSUREDAGE ::int,$1:LOADDATE ::timestamp_ntz,$1:MAN_WRTN_PREM_AMT ::numeric(13,2) ,$1:MAN_WRTN_PREM_AMT_ITD ::numeric(13,2) ,$1:MAN_WRTN_PREM_AMT_YTD ::numeric(13,2) ,$1:MONTH_ID ::int,$1:ORIG_WRTN_PREM_AMT ::numeric(13,2) ,$1:ORIG_WRTN_PREM_AMT_ITD ::numeric(13,2) ,$1:ORIG_WRTN_PREM_AMT_YTD ::numeric(13,2) ,$1:POLICYCANCELLEDISSUEDIND ::int,$1:POLICYEFFECTIVEDATE_ID ::int,$1:POLICYENDORSEMENTISSUEDIND ::int,$1:POLICYEXPIRATIONDATE_ID ::int,$1:POLICYEXTENSION_ID ::int,$1:POLICYMASTERTERRITORY_ID ::int,$1:POLICYNEWISSUEDIND ::int,$1:POLICYNEWORRENEWAL ::varchar(10),$1:POLICYNONRENEWALISSUEDIND ::int,$1:POLICYRENEWEDISSUEDIND ::int,$1:POLICYSTATUS_ID ::int,$1:POLICY_ID ::int,$1:POLICY_UNIQUEID ::varchar(100),$1:PRODUCER_ID ::int,$1:PRODUCT_ID ::int,$1:REIN_PREM_AMT ::numeric(13,2) ,$1:REIN_PREM_AMT_ITD ::numeric(13,2) ,$1:REIN_PREM_AMT_YTD ::numeric(13,2) ,$1:SECONDINSURED_ID ::int,$1:SOURCE_SYSTEM ::varchar(100),$1:TAXES_AMT ::numeric(13,2) ,$1:TAXES_AMT_ITD ::numeric(13,2) ,$1:TAXES_AMT_YTD ::numeric(13,2) ,$1:TERM_PREM_AMT ::numeric(13,2) ,$1:TERM_PREM_AMT_ITD ::numeric(13,2) ,$1:TERM_PREM_AMT_YTD ::numeric(13,2) ,$1:THIRDINSURED_ID ::int,$1:UNDERWRITER_ID ::int,$1:UNEARNED_PREM ::numeric(13,2) ,$1:USR_DEF_SUM1 ::numeric(13,2) ,$1:USR_DEF_SUM10 ::numeric(13,2) ,$1:USR_DEF_SUM10_ITD ::numeric(13,2) ,$1:USR_DEF_SUM10_YTD ::numeric(13,2) ,$1:USR_DEF_SUM11 ::numeric(13,2) ,$1:USR_DEF_SUM11_ITD ::numeric(13,2) ,$1:USR_DEF_SUM11_YTD ::numeric(13,2) ,$1:USR_DEF_SUM12 ::numeric(13,2) ,$1:USR_DEF_SUM12_ITD ::numeric(13,2) ,$1:USR_DEF_SUM12_YTD ::numeric(13,2) ,$1:USR_DEF_SUM13 ::numeric(13,2) ,$1:USR_DEF_SUM13_ITD ::numeric(13,2) ,$1:USR_DEF_SUM13_YTD ::numeric(13,2) ,$1:USR_DEF_SUM14 ::numeric(13,2) ,$1:USR_DEF_SUM14_ITD ::numeric(13,2) ,$1:USR_DEF_SUM14_YTD ::numeric(13,2) ,$1:USR_DEF_SUM15 ::numeric(13,2) ,$1:USR_DEF_SUM15_ITD ::numeric(13,2) ,$1:USR_DEF_SUM15_YTD ::numeric(13,2) ,$1:USR_DEF_SUM1_ITD ::numeric(13,2) ,$1:USR_DEF_SUM1_YTD ::numeric(13,2) ,$1:USR_DEF_SUM2 ::numeric(13,2) ,$1:USR_DEF_SUM2_ITD ::numeric(13,2) ,$1:USR_DEF_SUM2_YTD ::numeric(13,2) ,$1:USR_DEF_SUM3 ::numeric(13,2) ,$1:USR_DEF_SUM3_ITD ::numeric(13,2) ,$1:USR_DEF_SUM3_YTD ::numeric(13,2) ,$1:USR_DEF_SUM4 ::numeric(13,2) ,$1:USR_DEF_SUM4_ITD ::numeric(13,2) ,$1:USR_DEF_SUM4_YTD ::numeric(13,2) ,$1:USR_DEF_SUM5 ::numeric(13,2) ,$1:USR_DEF_SUM5_ITD ::numeric(13,2) ,$1:USR_DEF_SUM5_YTD ::numeric(13,2) ,$1:USR_DEF_SUM6 ::numeric(13,2) ,$1:USR_DEF_SUM6_ITD ::numeric(13,2) ,$1:USR_DEF_SUM6_YTD ::numeric(13,2) ,$1:USR_DEF_SUM7 ::numeric(13,2) ,$1:USR_DEF_SUM7_ITD ::numeric(13,2) ,$1:USR_DEF_SUM7_YTD ::numeric(13,2) ,$1:USR_DEF_SUM8 ::numeric(13,2) ,$1:USR_DEF_SUM8_ITD ::numeric(13,2) ,$1:USR_DEF_SUM8_YTD ::numeric(13,2) ,$1:USR_DEF_SUM9 ::numeric(13,2) ,$1:USR_DEF_SUM9_ITD ::numeric(13,2) ,$1:USR_DEF_SUM9_YTD ::numeric(13,2) ,$1:WRTN_PREM_AMT ::numeric(13,2) ,$1:WRTN_PREM_AMT_ITD ::numeric(13,2) ,$1:WRTN_PREM_AMT_YTD ::numeric(13,2),hash(*),'I',current_date(),current_date(), FALSE,'0','0', current_user() from @PROJECT_DB.External_stage.FACT_POLICY_STAGE);"
        run_query(conn,sql_querry_copy_data_fact_policy);
	
