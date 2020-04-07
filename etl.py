import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    - Reads a song json data file into Pandas DataFrame
    - Inserts song attributes into song table
    - Inserts artist attributes into artist table
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads a log json data file into Pandas DataFrame
    - Collects timestamp attributes and inserts into time table
    - Collects user attributes and inserts into users table
    - Looks up song_id & artist_id from songs & artists table using song attributes from log file
    - Collect songplay attributes & table lookups from log file and insert into songplays table
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df['ts'].tolist(), t.dt.hour.tolist(), t.dt.day.tolist(), t.dt.week.tolist(), 
                 t.dt.month.tolist(), t.dt.year.tolist(), t.dt.weekday.tolist()]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame({
        column_labels[0]: time_data[0], 
        column_labels[1]: time_data[1], 
        column_labels[2]: time_data[2], 
        column_labels[3]: time_data[3], 
        column_labels[4]: time_data[4], 
        column_labels[5]: time_data[5], 
        column_labels[6]: time_data[6], 
    })
    time_df = time_df.drop_duplicates(subset ="start_time", keep = 'first', inplace = False)


    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.drop_duplicates(subset ="userId", keep = 'first', inplace = False)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
#         print("song: ", row.song, "artist: ", row.artist, "length: ", row.length)
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - Collect all the json data files under the specific directory
    - Call the processing function to read datafile and create tables
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connect to database with credentials
    - Process all the song and log data files to create the tables
    - Cleanup
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()