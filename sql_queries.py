# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"


# CREATE TABLES

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays
    ( songplay_id SERIAL PRIMARY KEY,
      start_time BigInt,
      user_id Integer, level varchar,
      song_id varchar , artist_id varchar,
      session_id Integer, location varchar, user_agent varchar);
    """

user_table_create = """CREATE TABLE IF NOT EXISTS users
    (user_id Integer PRIMARY KEY, first_name varchar,
     last_name varchar, gender char, level varchar);
    """

song_table_create = """CREATE TABLE IF NOT EXISTS songs
    (song_id varchar PRIMARY KEY, title varchar,
     artist_id varchar, year Integer, duration numeric);
    """

artist_table_create = """CREATE TABLE IF NOT EXISTS artists
    (artist_id varchar PRIMARY KEY, name varchar,
     location varchar, latitude numeric , longitude numeric);
    """

time_table_create = """CREATE TABLE IF NOT EXISTS time
    (start_time BigInt PRIMARY KEY, hour Integer, day Integer,
     week Integer, month Integer, year Integer, weekday varchar);
    """


# INSERT RECORDS

songplay_table_insert = """INSERT Into songplays
    (songplay_id, start_time, user_id, level, song_id,
     artist_id, session_id, location, user_agent)
     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
     on conflict(songplay_id)
        do nothing
    """

user_table_insert = """INSERT Into users
    (user_id, first_name, last_name, gender, level)
     VALUES (%s, %s, %s, %s, %s)
     on conflict(user_id)
        do
            update
            set level = excluded.level
    """

song_table_insert = """INSERT Into songs
    (song_id, title, artist_id, year, duration)
     VALUES (%s, %s, %s, %s, %s)
     on conflict(song_id)
        do nothing
    """

artist_table_insert = """INSERT Into artists
    (artist_id, name, location, latitude, longitude)
     VALUES(%s, %s, %s, %s, %s)
     on conflict(artist_id)
        do nothing
    """


time_table_insert = """INSERT Into time
    (start_time, hour, day, week, month, year, weekday)
     VALUES(%s, %s, %s, %s, %s, %s, %s)
     on conflict(start_time)
        do nothing
    """

# FIND SONGS
# get songid and artistid from song and artist tables
song_select = """SELECT songs.song_id, artists.artist_id
     from songs join artists on songs.artist_id = artists.artist_id
     WHERE songs.title = %s AND
      (artists.name = %s AND songs.duration = %s)
    """

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
