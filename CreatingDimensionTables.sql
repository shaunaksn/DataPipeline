-- Creating Database dbo 

create database dbo;
use dbo;


-- Creating table dim_policy

CREATE TABLE DIM_POLICY (
	POLICY_ID int NOT NULL,
	VALID_FROMDATE datetime NULL,
	VALID_TODATE datetime NULL,
	RECORD_VERSION int NULL,
	POL_POLICYNUMBERPREFIX varchar(10) COLLATE latin1_general_cs NULL,
	POL_POLICYNUMBER varchar(50) COLLATE latin1_general_cs NULL,
	POL_POLICYNUMBERSUFFIX varchar(10) COLLATE latin1_general_cs NULL,
	POL_ORIGINALEFFECTIVEDATE datetime NULL,
	POL_QUOTEDDATE datetime NULL,
	POL_ISSUEDDATE datetime NULL,
	POL_BINDERISSUEDDATE datetime NULL,
	POL_ASL varchar(5) COLLATE latin1_general_cs NULL,
	POL_MASTERSTATE varchar(50) COLLATE latin1_general_cs NULL,
	POL_MASTERCOUNTRY varchar(50) COLLATE latin1_general_cs NULL,
	POL_CONVERSIONINDCODE varchar(5) COLLATE latin1_general_cs NULL,
	POL_ACCOUNT varchar(50) COLLATE latin1_general_cs NULL,
	POL_ACCOUNTDESCRIPTION varchar(256) COLLATE latin1_general_cs NULL,
	POL_AUTOMATEDAPPROVALPOLICY int NULL,
	POL_GENERATIONSOURCE varchar(100) COLLATE latin1_general_cs NULL,
	POL_UNIQUEID varchar(100) COLLATE latin1_general_cs NULL,
	SOURCE_SYSTEM varchar(100) COLLATE latin1_general_cs NULL,
	LOADDATE datetime NULL,
	CONSTRAINT PK_DIM_POLICY PRIMARY KEY (POLICY_ID)
);


-- Creating table dim_product

CREATE TABLE DIM_PRODUCT(
	PRODUCT_ID int NOT NULL,
	PRODUCT_UNIQUEID varchar(100) COLLATE latin1_general_cs NULL,
	PRDT_GROUP varchar(100) COLLATE latin1_general_cs NULL,
	PRDT_NAME varchar(100) COLLATE latin1_general_cs NULL,
	PRDT_LOB varchar(50) COLLATE latin1_general_cs NULL,
	PRDT_DESCRIPTION varchar(2000) COLLATE latin1_general_cs NULL,
	SOURCE_SYSTEM varchar(100) COLLATE latin1_general_cs NULL,
	LOADDATE datetime NULL,
	CONSTRAINT PK_DIM_PRODUCT PRIMARY KEY (PRODUCT_ID)
);
