import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function is used to drop all tables before creating new tables 
    avioding conflict table names then crashing down in Redshift.
    :param cur: cursor; 
    :param conn: connection; Connection to AWS Redshift database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function is used to create two stage tables, one fact table and four dimension tables.
    :param cur: cursor; 
    :param conn: connection; Connection to AWS Redshift database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()