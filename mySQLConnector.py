import mysql.connector
import csv

conn=mysql.connector.connect(host="hostname",user="user",passwd="password",database="database_name")
cur=conn.cursor()

# Reads the first csv file - Dim policy
csv_dim_policy=csv.reader(open(r'file path'))
header=next(csv_dim_policy)

# Dim policy table already created in mySQL

# Inserts data into Dim policy table
for row in csv_dim_policy:
    cur.execute("INSERT INTO dim_policy(POLICY_ID, VALID_FROMDATE,VALID_TODATE,RECORD_VERSION,POL_POLICYNUMBERPREFIX,POL_POLICYNUMBER,POL_POLICYNUMBERSUFFIX,POL_ORIGINALEFFECTIVEDATE,POL_QUOTEDDATE,POL_ISSUEDDATE,POL_BINDERISSUEDDATE,POL_ASL,POL_MASTERSTATE,POL_MASTERCOUNTRY,POL_CONVERSIONINDCODE,POL_ACCOUNT,POL_ACCOUNTDESCRIPTION,POL_AUTOMATEDAPPROVALPOLICY,POL_GENERATIONSOURCE,POL_UNIQUEID,SOURCE_SYSTEM,LOADDATE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row) 
conn.commit()

# Reads the second csv file - Dim Product
csv_dim_products=csv.reader(open(r'file_path'))
header=next(csv_dim_products)


# Dim product table already created in mySQL

# Inserts data into dim product table
for row in csv_dim_products:
    cur.execute("INSERT INTO dim_product(PRODUCT_ID,PRODUCT_UNIQUEID,PRDT_GROUP,PRDT_NAME,PRDT_LOB,PRDT_DESCRIPTION,SOURCE_SYSTEM,LOADDATE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",row) 
conn.commit()
cur.close()

