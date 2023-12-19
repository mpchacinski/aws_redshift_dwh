"""Contains function definitions for executing INSERT statements."""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Run scripts to copy data from S3 bucket into staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Run scripts converting staging raw data into a dimensional model."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Take arguments from cfg file and run COPY and INSERT functions on a Redshift cluster."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
