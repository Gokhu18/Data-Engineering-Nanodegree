import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table IF EXISTS staging_events"
staging_songs_table_drop = "drop table IF EXISTS staging_songs"

songplay_table_drop = "drop table IF EXISTS songplays"
user_table_drop = "drop table IF EXISTS users"
song_table_drop = "drop table IF EXISTS songs"
artist_table_drop = "drop table IF EXISTS artist"
time_table_drop = "drop table IF EXISTS time"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist text,
auth text,
firstName text,
gender CHAR(1),
itemInSession INTEGER,
lastName text,
length NUMERIC,
level text,
location text,
method text,
page text,
registration text,
sessionId INTEGER,
song text,
status text,
ts BIGINT,
userAgent text,
userId text
);
""")

staging_songs_table_create =  ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
artist_id text,
artist_latitude text,
artist_location text,
artist_longitude text,
artist_name text,
duration NUMERIC,
num_songs BIGINT,
song_id text,
title text,
year int
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id BIGINT IDENTITY(0,1),
start_time timestamp NOT NULL,
user_id int NOT NULL,
level varchar,
song_id varchar,
artist_id varchar,
session_id int NOT NULL,
location varchar,
user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id int PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration real
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
(
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude real,
longitude real
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times
(
start_time timestamp NOT NULL PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday int
);
""")

IAM_ROLE = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
SONG_DATA = config['S3']['SONG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# STAGING TABLES
staging_events_copy = (f"""
copy staging_events 
from {LOG_DATA}
iam_role '{IAM_ROLE}'
json {LOG_JSONPATH}
; """)

staging_songs_copy = (f"""
copy staging_songs 
from {SONG_DATA} 
iam_role '{IAM_ROLE}'
json 'auto'
;""")

# FINAL TABLES

# 'userId', 'firstName', 'lastName', 'gender', 'level'
user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT distinct
    cast(userId as int) user_id,
    firstName first_name,
    lastName last_name,
    gender,
    level 
FROM staging_events
WHERE page = 'NextSong'
-- ON CONFLICT (user_id) DO NOTHING
""")

# 'song_id', 'title', 'artist_id', 'year', 'duration'
song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT distinct
    song_id,
    title,
    artist_id,
    cast(year as INT) as year,
    cast(duration as REAL) as duration
FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT distinct
    artist_id,
    artist_name,
    artist_location,
    cast(artist_latitude as real),
    cast(artist_longitude as real)
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO times(start_time, hour, day, week, month, year, weekDay)
SELECT distinct
    timestamp 'epoch' + cast(ts as BIGINT)/1000 * interval '1 second' as start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year, 
    extract(dayofweek from start_time) as weekDay
FROM staging_events
WHERE page = 'NextSong'
""")

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    timestamp 'epoch' + cast(e.ts as BIGINT)/1000 * interval '1 second' as start_time, 
    cast(e.userId as int) as user_id,
    e.level,
    s.song_id,
    s.artist_id,
    cast(e.sessionId as int) as session_id,
    e.location,
    e.userAgent as user_agent
FROM staging_songs s
JOIN staging_events e on e.song = s.title 
                    AND e.artist = s.artist_name
                    AND e.length = s.duration
WHERE e.page = 'NextSong'
""")

# staging_events_table_create= ("""
# CREATE TABLE IF NOT EXISTS staging_events
# (
# artist text,
# auth text,
# firstName text,
# gender text,
# itemInSession text,
# lastName text,
# length text,
# level text,
# location text,
# method text,
# page text,
# registration text,
# sessionId text,
# song text,
# status text,
# ts text,
# userAgent text,
# userId text
# );
# """)

# staging_songs_table_create =  ("""
# CREATE TABLE IF NOT EXISTS staging_songs
# (
# artist_id text,
# artist_latitude text,
# artist_location text,
# artist_longitude text,
# artist_name text,
# duration text,
# num_songs text,
# song_id text,
# title text,
# year text
# );
# """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

