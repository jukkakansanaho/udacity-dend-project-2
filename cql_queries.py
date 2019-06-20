# DROP TABLES
# The following CQL queries drop all the tables from sparkifydb.
song_in_session_table_drop = "DROP TABLE IF EXISTS song_in_session"
artist_in_session_table_drop = "DROP TABLE IF EXISTS artist_in_session"
user_and_song_table_drop = "DROP TABLE IF EXISTS user_and_song"

# CREATE TABLES
# The following CQL queries create tables to sparkifydb.
song_in_session_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_in_session (
        session_id int, \
        item_in_session int, \
        artist text, \
        song text, \
        length float, \
        PRIMARY KEY(session_id, item_in_session))
""")

artist_in_session_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_in_session (
        user_id int, \
        session_id int, \
        artist text, \
        song text, \
        item_in_session int, \
        first_name text, \
        last_name text, \
        PRIMARY KEY((user_id, session_id), item_in_session)
    )
""")

user_and_song_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_and_song (
        song text, \
        user_id int, \
        first_name text, \
        last_name text, \
        PRIMARY KEY(song, user_id)
    )
""")

# INSERT RECORDS
# The following CQL queries insert data into sparkifydb tables.
song_in_session_insert = ("""
    INSERT INTO song_in_session (   session_id, \
                                    item_in_session, \
                                    artist, \
                                    song, \
                                    length)
    VALUES (%s, %s, %s, %s, %s)
""")

artist_in_session_insert = ("""
    INSERT INTO artist_in_session ( user_id, \
                                    session_id, \
                                    artist, \
                                    song, \
                                    item_in_session, \
                                    first_name, \
                                    last_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

user_and_song_insert = ("""
    INSERT INTO user_and_song ( song, \
                                user_id, \
                                first_name, \
                                last_name)
    VALUES (%s, %s, %s, %s)
""")

# FIND QUERIES
# The follwowing queries select data from DB.
#
# Query-1: Give me the artist, song title and song's length
# in the music app history that was heard during sessionId = 338,
# and itemInSession = 4
song_in_session_select = ("""
    SELECT artist, song, length
            FROM song_in_session \
            WHERE   session_id = (%s) AND \
                    item_in_session = (%s)
""")
#
# Query-2: Give me only the following: name of artist,
# song (sorted by itemInSession) and user (first and last name)
# for userid = 10, sessionid = 182
song_in_session_select = ("""
    SELECT artist, song, first_name, last_name
            FROM song_in_session \
            WHERE   session_id = (%s) AND \
                    item_in_session = (%s)
""")
#
# Query-3: Give me every user name (first and last) in my music app
# history who listened to the song 'All Hands Against His Own'
user_and_song_select = ("""
    SELECT first_name, last_name
            FROM user_and_song \
            WHERE song = (%s)
""")
#QUERY LISTS
create_table_queries = [song_in_session_table_create, artist_in_session_table_create, user_and_song_table_create]
drop_table_queries = [song_in_session_table_drop, artist_in_session_table_drop, user_and_song_table_drop]
