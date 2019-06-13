import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function is used to copy Song and Log JSON files from AWS S3 to Staging tables in AWS Redshift
    :param cur: cursor; 
    :param conn: connection; Connection to AWS Redshift database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function is used to transfer Song and Log data from staging table to fact and dimension tables
    :param cur: cursor; 
    :param conn: connection; Connection to AWS Redshift database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()