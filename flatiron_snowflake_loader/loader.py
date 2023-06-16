from pydantic import BaseSettings
import tempfile
import pathlib
import shutil
import click
import snowflake.connector
from snowflake.connector.cursor import SnowflakeCursor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(name="flatiron-loader")

class Settings(BaseSettings):
    """
    Settings for the application
    """

    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_warehouse: str


def get_stage_from_schema(schema: str) -> str:
    """
    Get the stage name from the schema
    """
    return f'@~/{schema}'


def get_schema_from_stage(stage: str) -> str:
    """
    Get the schema name from the stage
    """
    return stage.replace('@~/','')


def create_table_and_load(stage: str, tablename: str, filename: str, cursor: SnowflakeCursor):
    logger.info(f'Creating table {tablename}')
    sql = f"""
    CREATE OR REPLACE TABLE {tablename}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION=>'{stage}/{filename}'
                    , FILE_FORMAT=>'csvwithheader'
                )
            )
        );
    """
    logger.info(f'Executing SQL: {sql}')
    cursor.execute(sql)
    logger.info(f'Loading table {tablename}')
    sql = f"""copy into {tablename} from {stage}/{filename} 
                  file_format=csvwithheader match_by_column_name=CASE_INSENSITIVE;
    """
    cursor.execute(sql)



def create_tables_from_staged(schema: str, cur: SnowflakeCursor):
    stage = get_stage_from_schema(schema)
    cur.execute(f'list {stage}/')
    staged = cur.fetchall()
    for s in staged:
        p = pathlib.Path(s[0])
        filename = p.name
        if not filename.endswith('.csv.gz'):
            continue
        tablename = filename.replace('.csv.gz','')
        create_table_and_load(stage, tablename, filename, cur)


def local_file_to_stage(file: pathlib.Path, schema: str, cursor: SnowflakeCursor):
    stage = get_stage_from_schema(schema)
    sql = f'PUT file://{str(file.absolute())} {stage}/'
    logger.info(f'Putting file {file.name} to stage {stage}')
    cursor.execute(sql)


def get_cursor(settings: Settings) -> SnowflakeCursor:
    conn = snowflake.connector.connect(
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        account=settings.snowflake_account,
        database=settings.snowflake_database,
        schema=settings.snowflake_schema,
        warehouse=settings.snowflake_warehouse,
    )
    return conn.cursor()


@click.command()
@click.argument("zipfile", type=click.Path(exists=True))
@click.argument("schema", type=str)
def process(zipfile: str, schema: str):
    """
    Process a zip file
    """
    # Get the settings
    settings = Settings() # type: ignore
    settings.snowflake_schema = schema # type: ignore

    # Get the cursor
    cursor = get_cursor(settings)
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
    cursor.execute(f'USE SCHEMA {schema}')

    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()

    # Unzip the file
    logger.info('Unzipping file {zipfile}')
    shutil.unpack_archive(zipfile, temp_dir)

    # Process the files
    logger.info(f'Loading files to stage {get_stage_from_schema(schema)}')
    for file in pathlib.Path(temp_dir).iterdir():
        local_file_to_stage(file, schema, cursor)
    
    # Remove the temporary directory
    shutil.rmtree(temp_dir)

    # Create the tables
    logger.info('Creating tables')
    create_tables_from_staged(schema, cursor)


if __name__ == "__main__":  
    process() # pylint: disable=no-value-for-parameter
