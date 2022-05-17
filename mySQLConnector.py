import mysql.connector
import csv

conn=mysql.connector.connect(host="127.0.0.1",user="root",passwd="Pravin97@vp",database="test_db")
cur=conn.cursor()

# Reads the first csv file - Dim policy
csv_data=csv.reader(open(r'C:\Users\hp\Desktop\Mini-Project\Source Data\Group 6\DIM_POLICY_202202251455.csv'))
header=next(csv_data)

# Dim policy table already created in mySQL

# Inserts data into Dim policy table
for row in csv_data:
    cur.execute("INSERT INTO dim_policy(POLICY_ID, VALID_FROMDATE,VALID_TODATE,RECORD_VERSION,POL_POLICYNUMBERPREFIX,POL_POLICYNUMBER,POL_POLICYNUMBERSUFFIX,POL_ORIGINALEFFECTIVEDATE,POL_QUOTEDDATE,POL_ISSUEDDATE,POL_BINDERISSUEDDATE,POL_ASL,POL_MASTERSTATE,POL_MASTERCOUNTRY,POL_CONVERSIONINDCODE,POL_ACCOUNT,POL_ACCOUNTDESCRIPTION,POL_AUTOMATEDAPPROVALPOLICY,POL_GENERATIONSOURCE,POL_UNIQUEID,SOURCE_SYSTEM,LOADDATE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",row) 
conn.commit()

# Reads the second csv file - Dim Product
csv_fact=csv.reader(open(r'C:\Users\hp\Desktop\Mini-Project\Source Data\Group 6\DIM_PRODUCT_202202281254.csv'))
header=next(csv_fact)


# Fact policy table already created in mySQL

# Inserts data into dim product table
for row in csv_fact:
    cur.execute("INSERT INTO dim_product(PRODUCT_ID,PRODUCT_UNIQUEID,PRDT_GROUP,PRDT_NAME,PRDT_LOB,PRDT_DESCRIPTION,SOURCE_SYSTEM,LOADDATE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",row) 
conn.commit()
cur.close()

