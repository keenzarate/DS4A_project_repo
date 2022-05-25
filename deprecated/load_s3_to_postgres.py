import psycopg2
import logging
import json
from airflow.hooks.postgres_hook import PostgresHook



def upload_to_s3(
        

):



# function to pull data off the disk and load to postgres
def disk_to_postgres(postgres_conn_id:str, out_path:str, db_table):

    # establish connection
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    pg_conn = pg_hook.get_conn()
    con_cur = pg_conn.cursor()


    # command sql
    sql_quer = f"""
                select aws_s3.table_import_from_s3(
                    '{dest_table}',
                    '{column_customization}',
                    '{options}',
                    aws credentials))
                );
           """
    # evaluate paths
    out_list = eval(out_path)

    # begin copy to postgres
    with con_cur as cur:
        # iterate over each path
        for path in out_list:
            with open(path, 'r') as f:
                cur.copy_expert(sql=sql_quer, file=f)
            rowcount=cur.rowcount
            logging.info(f'{rowcount} rows inserted from {path}')
            pg_conn.commit()




