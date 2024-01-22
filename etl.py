"""
This module loaded data from S3 into staging tables on Redshift.

Also the data will be processed into analytics tables on Redshift
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load the staging tables from s3.

    :param cur: Cursor
    :param conn: Connection
    :return:
    """
    for query in copy_table_queries:
        print("Loading:", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert record into tables.

    :param cur: Cursor
    :param conn: Connection
    :return:
    """
    for query in insert_table_queries:
        print("Inserting:", query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Entry point.

    :return:
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
