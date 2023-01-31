#!/usr/bin/env python
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os

load_dotenv()

# # Gets the version
# ctx = snowflake.connector.connect(
#     user=os.getenv('USER'),
#     password=os.getenv('PASSWORD'),
#     account=os.getenv('ACCOUNT')
#     )

# cs = ctx.cursor()
# try:
#     cs.execute("SELECT current_version()")
#     one_row = cs.fetchone()
#     print(one_row[0])
# finally:
#     cs.close()
# ctx.close()

# with snowflake.connector.connect(
#     user=os.getenv('USERNAME'),
#     password=os.getenv('PASSWORD'),
#     account=os.getenv('ACCOUNT'),
#     warehouse=os.getenv('WAREHOUSE'),
#     database=os.getenv('DATABASE'),
#     schema=os.getenv('SCHEMA'),
# ) as con:
#     cs = con.cursor()
#     cs.execute("SELECT current_version()")
#     one_row = cs.fetchone()
#     print(one_row[0])

    # cs.execute("CREATE DATABASE IF NOT EXISTS inno")
    # cs.execute("USE DATABASE inno")
    # cs.execute("CREATE SCHEMA IF NOT EXISTS inno_schema")
    # cs.execute("USE SCHEMA inno.inno_schema")
    # cs.execute("CREATE STAGE IF NOT EXISTS inno_stage")
    # sql_create_raw_table = """CREATE OR REPLACE TABLE TEST_8 (
    #             _ID VARCHAR,
    #             IOS_APP_ID NUMBER,
    #             TITLE VARCHAR,
    #             DEVELOPER_NAME VARCHAR,
    #             DEVELOPER_IOS_ID FLOAT,
    #             IOS_STORE_URL VARCHAR,
    #             SELLER_OFFICIAL_WEBSITE VARCHAR,
    #             AGE_RATING VARCHAR,
    #             TOTAL_AVERAGE_RATING FLOAT,
    #             TOTAL_NUMBER_OF_RATINGS NUMBER,
    #             AVERAGE_RATING_FOR_VERSION FLOAT,
    #             NUMBER_OF_RATINGS_FOR_VERSION NUMBER,
    #             ORIGINAL_RELEASE_DATE VARCHAR,
    #             CURRENT_VERSION_RELEASE_DATE VARCHAR,
    #             PRICE_USD FLOAT,
    #             PRIMARY_GENRE VARCHAR,
    #             ALL_GENRES VARCHAR,
    #             LANGUAGES VARCHAR,
    #             DESCRIPTION VARCHAR
    #         );"""
    # cs.execute(sql_create_raw_table)
    # cs.execute('USE WAREHOUSE COMPUTE_WH')

    # file = os.getenv('FILE_PATH')
    # cs.execute(f'put file://{file} @inno_stage')
    # cs.execute("COPY INTO test_8 FROM @inno_stage FILE_FORMAT = (COMPRESSION = AUTO TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"')")


    # cs.execute("CREATE DATABASE IF NOT EXISTS inno")
    # cs.execute("USE DATABASE inno")
    # cs.execute("CREATE SCHEMA IF NOT EXISTS inno_schema")
    # cs.execute("USE SCHEMA inno.inno_schema")
    # cs.execute("CREATE STAGE IF NOT EXISTS inno_stage")
    # sql_create_raw_table = """CREATE OR REPLACE TABLE TEST_7 (
    #             REVIEWID VARCHAR,
    #             USERNAME VARCHAR,
    #             TITLE VARCHAR,
    #             USERIMAGE VARCHAR,
    #             CONTENT VARCHAR,
    #             SCORE NUMBER,
    #             THUMBSUPCOUNT NUMBER,
    #             REVIEWCREATEDVERSION VARCHAR,
    #             AT TIMESTAMP_NTZ,
    #             REPLYCONTENT VARCHAR,
    #             REPLIEDAT VARCHAR
    #         );"""

    # df = pd.read_csv(os.getenv("FILE_PATH2"))
    # df.columns = df.columns.str.upper()
    # write_pandas(con, df, "TEST_7")



# Gets the version
ctx = snowflake.connector.connect(
    user=os.getenv('USERNAME'),
    password=os.getenv('PASSWORD'),
    account=os.getenv('ACCOUNT'),
    warehouse=os.getenv('WAREHOUSE'),
    database=os.getenv('DATABASE'),
    schema=os.getenv('SCHEMA')
    )

cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])

    # cs.execute("USE SCHEMA inno.inno_schema")
    # sql_create_raw_table = """CREATE OR REPLACE TABLE TEST_SMALL (
    #             ID VARCHAR,
    #             SCORE NUMBER
    #         );"""
    # cs.execute(sql_create_raw_table)

    # d = {'id': ['a1', 'a2'], 'score': [1, 2]}
    # df = pd.DataFrame(data=d)
    # df.columns = df.columns.str.upper()
    # write_pandas(ctx, df, "TEST_SMALL")

    # cs.execute("USE SCHEMA inno.inno_schema")
    # sql_create_raw_table = """CREATE OR REPLACE TABLE TEST_7 (
    #             REVIEWID VARCHAR,
    #             USERNAME VARCHAR,
    #             USERIMAGE VARCHAR,
    #             CONTENT VARCHAR,
    #             SCORE NUMBER,
    #             THUMBSUPCOUNT NUMBER,
    #             REVIEWCREATEDVERSION VARCHAR,
    #             AT TIMESTAMP_NTZ,
    #             REPLYCONTENT VARCHAR,
    #             REPLIEDAT VARCHAR
    #         );"""
    # cs.execute(sql_create_raw_table)
    
    # sql_create_raw_table = """CREATE OR REPLACE TABLE TEST_8 (
    #             _ID VARCHAR,
    #             IOS_APP_ID NUMBER,
    #             TITLE VARCHAR,
    #             DEVELOPER_NAME VARCHAR,
    #             DEVELOPER_IOS_ID FLOAT,
    #             IOS_STORE_URL VARCHAR,
    #             SELLER_OFFICIAL_WEBSITE VARCHAR,
    #             AGE_RATING VARCHAR,
    #             TOTAL_AVERAGE_RATING FLOAT,
    #             TOTAL_NUMBER_OF_RATINGS NUMBER,
    #             AVERAGE_RATING_FOR_VERSION FLOAT,
    #             NUMBER_OF_RATINGS_FOR_VERSION NUMBER,
    #             ORIGINAL_RELEASE_DATE VARCHAR,
    #             CURRENT_VERSION_RELEASE_DATE VARCHAR,
    #             PRICE_USD FLOAT,
    #             PRIMARY_GENRE VARCHAR,
    #             ALL_GENRES VARCHAR,
    #             LANGUAGES VARCHAR,
    #             DESCRIPTION VARCHAR
    #         );"""
    # cs.execute(sql_create_raw_table)
    cs.execute('''CREATE OR REPLACE FILE FORMAT MYCSVFORMAT TYPE = 'CSV' 
                            FIELD_DELIMITER = ',' 
                            SKIP_HEADER = 1 
                            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                            ESCAPE_UNENCLOSED_FIELD = None ''')
    cs.execute('CREATE OR REPLACE STAGE INNO_STAGE2 FILE_FORMAT = MYCSVFORMAT')
    cs.execute('TRUNCATE TABLE IF EXISTS TEST_8')
    file = os.getenv('FILE_PATH')
    cs.execute(f'put file://{file} @INNO_STAGE2')
    cs.execute(f"COPY INTO TEST_8 FROM @INNO_STAGE2")
    
    # df = pd.read_csv(os.getenv("FILE_PATH"))
    # df.columns = df.columns.str.upper()
    # write_pandas(ctx, df, "TEST_8")
finally:
    cs.close()
ctx.close()