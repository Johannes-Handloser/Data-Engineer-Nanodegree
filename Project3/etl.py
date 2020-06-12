import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 to the previously created staging tables using the queries in the copy_table_queries array
    """
    print("inside load staging tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    ETL Function for populating dimension and fact tables from the staging tables using insert_table_queries array
    """
    print("inside insert to tables with queries:")
    for query in insert_table_queries:
        print("query: " + query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to create database connection to AWS Redshift and executing etl functions
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        conn=psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    except:
        print ("I am unable to connect to the database.")
    
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()