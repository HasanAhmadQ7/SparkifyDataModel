import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *
from psycopg2.extensions import register_adapter, AsIs


def process_song_file(cur, filepath):
    """Extracts data from a song file and loads it to the corresponding
    tables.

    The destination tables are: songs, artists
    Args:
        cur (pyscopg cursor): A cursor that allows executing commands
         against the database.
        filepath: A path to the song file.
    """

    # open song file
    df = pd.read_json(filepath, lines=True)
    # insert song record
    song_data = list(
        df.loc[0, ["song_id", "title", "artist_id", "year", "duration"]].values
    )
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(
        df.loc[
            0,
            [
                "artist_id",
                "artist_name",
                "artist_location",
                "artist_latitude",
                "artist_longitude",
            ],
        ].values
    )
    artist_data = [
        None if isinstance(el, np.float64) and np.isnan(el) else el
        for el in artist_data
    ]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Extracts data from a log file, combines it with data from
    other tables and loads it to the corresponding tables.

    Args:
        cur (pyscopg cursor): A cursor that allows executing
        commands against the database.
        filepath: A path to the log file.
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp (ts) column to datetime (ts_dt)
    ts = df["ts"]
    ts_dt = pd.to_datetime(df["ts"])

    # insert time data records
    time_data = (ts, ts_dt.dt.hour, ts_dt.dt.day, ts_dt.dt.week,
                 ts_dt.dt.month, ts_dt.dt.year, ts_dt.dt.weekday)
    column_labels = ("start_time", "hour", "day",
                     "week", "month", "year", "weekday")
    combined_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(combined_dict)
    # remove duplicates
    time_df.drop_duplicates(subset='start_time', keep="first", inplace=True)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]
    user_df_columns = {"userId": "user_id", "firstName": "first_name",
                       "lastName": "last_name"}
    user_df = user_df.rename(columns=user_df_columns)
    user_df.drop_duplicates(subset='user_id', keep="last", inplace=True)
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        # insert songplay record
        songplay_data = (
            index,
            row.ts,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Iteratively executes an ETL function on files in a specific folder.

    Args:
        cur (pyscopg cursor): A cursor object that allows executing commands
         against the database.
        conn (connection): A connection object to the database
        filepath: A path to the directory containing the data files
        func: A function to be executed
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    register_adapter(np.int64, AsIs)
    register_adapter(np.float64, AsIs)
    main()
