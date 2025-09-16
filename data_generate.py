import os
import snowflake.connector
import pandas as pd
from pathlib import Path
from datetime import date, datetime

# Global variables
snf_user = os.getenv('SNOWFL_USER')
snf_password = os.getenv('SNOWFL_PWD')
snf_account = os.getenv('SNOWFL_ACCT')
snf_warehouse = 'COMPUTE_WH'
snf_database = 'SNOWFLAKE_SAMPLE_DATA'

models_dir = Path(os.getenv('DBT_MODELS_PATH'))
#source_schemas = ['TPCDS_SF100TCL', 'TPCDS_SF10TCL']
source_schemas = ['TPCH_SF100', 'TPCH_SF1000']

today = date.today()
now = datetime.now()

global connsnf


def sql_to_df(sql_query, column_names):
    cursnf = connsnf.cursor()
    try:
        cursnf.execute(sql_query)
    except (Exception, snowflake.connector.DatabaseError) as error:
        print('Error: ', error)
        return None

    tuples_list = cursnf.fetchall()
    cursnf.close()
    df = pd.DataFrame(tuples_list, columns=column_names)
    return df


def get_metadata():
    sql = """
        SELECT
            cl.table_schema AS schema,
            cl.table_name AS object_name,
            tb.table_type AS object_type,
            cl.column_name,
            REPLACE(REPLACE(LOWER(cl.data_type),'timestamp_ltz','timestamp'),'text','varchar') AS data_type,
            CASE WHEN cl.is_nullable = 'YES' THEN 'null' ELSE 'not null' END AS mandatory
        FROM information_schema.columns AS cl
        LEFT JOIN snowflake_sample_data.information_schema.tables AS tb
            ON (cl.table_name = tb.table_name AND cl.table_schema = tb.table_schema)
        ORDER BY cl.table_schema, cl.table_name ASC;
    """

    df = sql_to_df(sql, ('schema', 'object_name', 'object_type', 'column_name', 'data_type', 'mandatory'))
    if df is not None:
        print("Sample metadata:")
        print(df.head())
    else:
        print("Failed to retrieve metadata.")
    return df


def create_schema_dirs(schemas):
    for sch in schemas:
        try:
            path = models_dir / f"{sch}_src"
            print(f"Creating directory: {path}")
            path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"Failed to create directory {path}: {e}")
            return 1
    return 0


def create_source_yml_files(df):
    for sc in df.schema.unique():
        print(f"Starting to create YAML files for schema {sc}...")
        dir_path = models_dir / f"{sc}_src"
        print(f"Generating YAML files in: {dir_path}")
        for obj_name in df[df.schema == sc].object_name.unique():
            for obj_type in df[(df.schema == sc) & (df.object_name == obj_name)].object_type.unique():
                print(f"Processing object: {obj_name} with type {obj_type}")
                filename = f"{obj_name}.yml"
                file_path = dir_path / filename
                try:
                    with open(file_path, 'w') as f:
                        f.write("version: 2\n")
                        f.write("sources:\n")
                        f.write(f"  - name: {snf_database}-{sc}\n")
                        f.write(f"    database: {snf_database}\n")
                        f.write(f"    schema: {sc}\n")
                        f.write("    tables:\n")
                        f.write(f"      - name: {obj_name}\n")
                        f.write("        columns:\n")
                        cols = df[(df.schema == sc) & (df.object_name == obj_name) & (df.object_type == obj_type)]
                        for _, row in cols.iterrows():
                            f.write(f"          - name: {row['column_name']}\n")
                    print(f"Created YAML file: {file_path}")
                except Exception as e:
                    print(f"Error writing YAML file {file_path}: {e}")


def create_source_model_files(df):
    for sc in df.schema.unique():
        print(f"Starting to create SQL models for schema {sc}...")
        dir_path = models_dir / f"{sc}_src"
        print(f"Generating SQL files in: {dir_path}")
        for obj_name in df[df.schema == sc].object_name.unique():
            for obj_type in df[(df.schema == sc) & (df.object_name == obj_name)].object_type.unique():
                print(f"Processing SQL model: {obj_name} (type {obj_type})")
                filename = f"{obj_name}.sql"
                file_path = dir_path / filename
                try:
                    with open(file_path, 'w') as f:
                        f.write("with source_data as (\n")
                        f.write(f"      select *\n")
                        f.write(f"      from {{% source('{snf_database}-{sc}', '{obj_name}') %}}\n")
                        f.write(")\n\n")
                        f.write("select *\nfrom source_data\n")
                    print(f"Created SQL file: {file_path}")
                except Exception as e:
                    print(f"Error writing SQL file {file_path}: {e}")


if __name__ == "__main__":
    print(f"Models directory: {models_dir}")
    if not models_dir or not models_dir.exists():
        print(f"Error: models directory '{models_dir}' does not exist or is not set.")
    else:
        try:
            connsnf = snowflake.connector.connect(
                user=snf_user,
                password=snf_password,
                account=snf_account,
                warehouse=snf_warehouse,
                database=snf_database,
                session_parameters={'QUERY_TAG': f'rev_eng_dbt_{now:%Y%m%d_%H%M%S}'}
            )
            print("Connected to Snowflake successfully.")
        except Exception as e:
            print(f"Error connecting to Snowflake: {e}")
            exit(1)

        metadata_df = get_metadata()

        if metadata_df is not None:
            schemas_to_process = metadata_df[metadata_df['schema'].isin(source_schemas)].schema.unique()
            ret_code = create_schema_dirs(schemas_to_process)
            print(f"Directory creation completed with code: {ret_code}")

            if ret_code == 0:
                create_source_yml_files(metadata_df[metadata_df['schema'].isin(source_schemas)])
                create_source_model_files(metadata_df[metadata_df['schema'].isin(source_schemas)])
        else:
            print("No metadata available to process.")
