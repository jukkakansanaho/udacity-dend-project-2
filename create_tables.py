import cassandra
from cql_queries import create_table_queries, drop_table_queries


def create_database():
    """Connect to Apache Cassandra, drop any existing sparkifydb and
        create a new one.

    Keyword arguments:
    * None

    Output:
    * session --    connection to Apache Cassandra database (sparkifydb).
                    Allows to execute SQL commands.
    """
    # connect to default database
    from cassandra.cluster import Cluster
    try:
        # Connect to a local Cassandra cluster
        cluster = Cluster(['127.0.0.1'])
        # Set a session execute queries.
        session = cluster.connect()

    except Exception as e:
        print(e)

    # drop sparkify database
    try:
        session.execute("""
            DROP KEYSPACE IF EXISTS sparkifydb
        """)
    except Exception as e:
        print(e)

    # create sparkify database with SimpleStrategy
    # and replication_factor = 1.
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS sparkifydb
            WITH REPLICATION =
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
    except Exception as e:
        print(e)

    try:
        # Set keyspace (sparkifydb) for Cassandra session.
        session.set_keyspace('sparkifydb')
    except Exception as e:
        print(e)

    return cluster, session


def drop_tables(session):
    """Drop any existing tables from sparkifydb.

    Keyword arguments:
    * session --    connection to Apache Cassandra DB (sparkifydb).
                    Allows to execute CQL commands.

    Output:
    * Old sparkifydb database tables are dropped from Apache Cassandra.
    """
    for query in drop_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Error: Issue dropping table.")
            print(e)

    print("Tables dropped successfully.")

def create_tables(session):
    """Create new tables ()
        to sparkifydb.

    Keyword arguments:
    * session --    connection to Apache Cassandra DB (sparkifydb).
                    Allows to execute CQL commands.

    Output:
    * New sparkifydb database tables are created into Apache Cassandra.
    """
    for query in create_table_queries:
        try:
            session.execute(query)
        except Exception as e:
            print("Error: Issue creating table.")
            print(e)
    print("Tables created successfully.")

def main():
    """Connect to Apache Cassandra, create new DB (sparkifydb),
        drop any existing tables, create new tables. Close DB connection.

    Keyword arguments:
    * None

    Output:
    * New sparkifydb is created, old tables are droppped,
        and new tables (song_in_session, artist_in_session,
        user_and_song) are created..
    """
    # Connect to DB, drop old DB, create new DB. Return session to DB.
    cluster, session = create_database()

    # Drop old tables from DB.
    drop_tables(session)
    # Create new tables to DB.
    create_tables(session)

    # Close the session and DB connection.
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
