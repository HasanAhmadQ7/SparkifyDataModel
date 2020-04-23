import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """Creates a database and returns connection and cursor objects.

    Note: in case the database was existing, it will be dropped and recreated.

    Returns:
        cur (cursor): A cursor object to commit commands to the database
        conn (connection): A connection object to the database

    """
    # connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=student password=student"
    )
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute(
        """CREATE DATABASE sparkifydb WITH ENCODING
                'utf8' TEMPLATE template0"""
    )

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """Drops all tables of the starschema.

    Args:
        cur (pyscopg cursor): A cursor that allows executing commands against the database.
        conn (connection): A connection object to the database
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all tables of the starschema.

    Args:
        cur (pyscopg cursor): A cursor that allows executing commands against the database.
        conn (connection): A connection object to the database
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
