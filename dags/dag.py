import pendulum
import snowflake.connector
from airflow.decorators import dag, task, task_group
from snowflake.connector.cursor import SnowflakeCursor
from airflow.utils.edgemodifier import Label
from dotenv import load_dotenv
import os

load_dotenv()


@dag(schedule=None, start_date=pendulum.now(), catchup=False)
def elt_pipeline():
    @task
    def create_db(cursor: SnowflakeCursor) -> None:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

    @task
    def create_schema(cursor: SnowflakeCursor) -> None:
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

    @task
    def create_format(cursor: SnowflakeCursor) -> None:
        cursor.execute(f"USE SCHEMA {database}.{schema_name}")
        cursor.execute(f'''CREATE OR REPLACE FILE FORMAT {format_name}
                        TYPE = 'CSV'
                        FIELD_DELIMITER = ','
                        SKIP_HEADER = 1
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        ESCAPE_UNENCLOSED_FIELD = None ''')

    @task
    def create_stage(cursor: SnowflakeCursor) -> None:
        cursor.execute(f"USE SCHEMA {database}.{schema_name}")
        cursor.execute(f'''CREATE STAGE IF NOT EXISTS {stage_name}
                        FILE_FORMAT = {format_name}''')

    @task_group
    def preparations(cursor: SnowflakeCursor) -> None:
        create_db(cursor) >> \
        create_schema(cursor) >> \
        create_format(cursor) >> \
        create_stage(cursor)

    @task
    def create_table(table_name: str,
                     cursor: SnowflakeCursor, table_sample: str = '',
                     raw: bool = True) -> None:
        cursor.execute(f"USE SCHEMA {database}.{schema_name}")
        if raw:
            cursor.execute(f"""CREATE OR REPLACE TABLE {table_name} (
                                _ID VARCHAR,
                                IOS_APP_ID NUMBER,
                                TITLE VARCHAR,
                                DEVELOPER_NAME VARCHAR,
                                DEVELOPER_IOS_ID FLOAT,
                                IOS_STORE_URL VARCHAR,
                                SELLER_OFFICIAL_WEBSITE VARCHAR,
                                AGE_RATING VARCHAR,
                                TOTAL_AVERAGE_RATING FLOAT,
                                TOTAL_NUMBER_OF_RATINGS NUMBER,
                                AVERAGE_RATING_FOR_VERSION FLOAT,
                                NUMBER_OF_RATINGS_FOR_VERSION NUMBER,
                                ORIGINAL_RELEASE_DATE VARCHAR,
                                CURRENT_VERSION_RELEASE_DATE VARCHAR,
                                PRICE_USD FLOAT,
                                PRIMARY_GENRE VARCHAR,
                                ALL_GENRES VARCHAR,
                                LANGUAGES VARCHAR,
                                DESCRIPTION VARCHAR
                            );""")
        else:
            cursor.execute(f'''CREATE OR REPLACE TABLE {table_name} AS
                            SELECT * FROM {table_sample}''')

    @task
    def create_stream(stream_name: str, table_name: str,
                      cursor: SnowflakeCursor) -> None:
        cursor.execute(f"USE SCHEMA {database}.{schema_name}")
        cursor.execute(f'''CREATE STREAM IF NOT EXISTS {stream_name}
                        ON TABLE {table_name}''')

    @task
    def load_csv(file_path: str, table_name: str,
                 cursor: SnowflakeCursor) -> None:
        cursor.execute(f"USE SCHEMA {database}.{schema_name}")
        cursor.execute(f'put file://{file_path} @{stage_name}')
        cursor.execute(f"COPY INTO {table_name} FROM @{stage_name}")

    @task
    def stream_data(table_name: str, stream_name: str,
                    cursor: SnowflakeCursor) -> None:
        cursor.execute(f"USE SCHEMA {database}.{schema_name}")
        cursor.execute(f'''INSERT INTO {table_name}
                         SELECT _ID, IOS_APP_ID, TITLE, DEVELOPER_NAME,
                                DEVELOPER_IOS_ID, IOS_STORE_URL,
                                SELLER_OFFICIAL_WEBSITE,
                                AGE_RATING, TOTAL_AVERAGE_RATING,
                                TOTAL_NUMBER_OF_RATINGS,
                                AVERAGE_RATING_FOR_VERSION,
                                NUMBER_OF_RATINGS_FOR_VERSION,
                                ORIGINAL_RELEASE_DATE,
                                CURRENT_VERSION_RELEASE_DATE,
                                PRICE_USD, PRIMARY_GENRE,
                                ALL_GENRES, LANGUAGES, DESCRIPTION
                         FROM {stream_name}''')

    user = os.getenv('USERNAME')
    password = os.getenv('PASSWORD')
    account = os.getenv('ACCOUNT')
    warehouse = os.getenv('WAREHOUSE')
    database = os.getenv('DATABASE')
    schema_name = os.getenv('SCHEMA')
    format_name = os.getenv('FORMAT_NAME')
    stage_name = os.getenv('STAGE_NAME')
    raw_table = os.getenv('RAW_TABLE')
    stage_table = os.getenv('STAGE_TABLE')
    master_table = os.getenv('MASTER_TABLE')
    raw_stream = os.getenv('RAW_STREAM')
    stage_stream = os.getenv('STAGE_STREAM')
    file_path = os.getenv('FILE_PATH')

    ctx = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse
        )

    cs = ctx.cursor()

    preparations(cs) >> \
    Label("Create tables") >> \
    create_table(raw_table, cs) >> \
    (create_table(stage_table, cs,
                  raw_table, raw=False)) >> \
    (create_table(master_table, cs,
                  raw_table, raw=False)) >> \
    Label("Create streams") >> \
    create_stream(raw_stream, raw_table, cs) >> \
    create_stream(stage_stream, stage_table, cs) >> \
    Label("Transfer data") >> \
    load_csv(file_path, raw_table, cs) >> \
    stream_data(stage_table, raw_stream, cs) >> \
    stream_data(master_table, stage_stream, cs)


elt_pipeline()
