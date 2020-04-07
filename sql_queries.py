# DROP TABLES

# drop tables if it exists
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# records in log data associated with song plays i.e. records with page 'NextSong'
# ensure that a user is using a single session at any particular time
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
(songplay_id SERIAL PRIMARY KEY, start_time bigint NOT NULL, user_id bigint NOT NULL, level varchar, \
song_id varchar, artist_id varchar, session_id bigint NOT NULL, location varchar, user_agent varchar, \
UNIQUE (start_time, user_id, session_id))""")

# users in the app
user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
(user_id bigint PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar) \
""")

# songs in music database
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
(song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration real) \
""")

# artists in music database
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists \
(artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude real, longitude real) \
""")

# timestamps of records in songplays broken down into speci c units
time_table_create = ("""CREATE TABLE IF NOT EXISTS time \
(start_time bigint PRIMARY KEY, hour smallint NOT NULL, day smallint NOT NULL, week smallint NOT NULL, \
month smallint NOT NULL, year smallint NOT NULL, weekday varchar NOT NULL) \
""")

# INSERT RECORDS

# insert a songplay record into songplays table
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, \
song_id, artist_id, session_id, location, user_agent) \
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)""")

# insert an user into users table
# upsert user's level to allow for free to paid conversions
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level""")

# insert a song into songs table
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (song_id) DO NOTHING""")

# insert an artist into artists table
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (artist_id) DO NOTHING""")

# insert a timestamp and associated time attributes into time table
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
VALUES (%s, %s, %s, %s, %s, %s, %s) \
ON CONFLICT (start_time) DO NOTHING""")

# FIND SONGS

# lookup the song & artist id based on song title & artist name & song duration
song_select = ("""SELECT songs.song_id, artists.artist_id \
FROM songs JOIN artists ON songs.artist_id=artists.artist_id \
WHERE songs.title=%s AND artists.name=%s AND ROUND(songs.duration)=ROUND(%s) \
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]