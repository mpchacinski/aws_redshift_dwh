import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','arn')
LOG_JSON_PATH = config.get("S3", "log_jsonpath")
LOG_DATA = config.get("S3", "log_data")
SONG_DATA = config.get("S3", "song_data")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar,
    item_in_session integer,
    last_name varchar,
    length numeric(10,4),
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    session_id integer,
    song varchar,
    status varchar,
    ts bigint,
    user_agent varchar,
    user_id integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs integer,
    artist_id varchar,
    artist_latitude numeric(8, 6),
    artist_longitude numeric(9, 6),
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric(10, 4),
    year integer
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id integer identity(1, 1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id integer,
    location varchar,
    user_agent varchar
    );
""")

user_table_create = ("""
CREATE TABLE users (
    user_id integer PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    );
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar PRIMARY KEY,
    title varchar,
    artist_id varchar,
    year integer,
    duration numeric(10, 4)
    );
""")

artist_table_create = ("""
CREATE TABLE artists (
   artist_id varchar PRIMARY KEY,
   name varchar,
   location varchar,
   latitude numeric(8, 6),
   longitude numeric(9, 6)
   );
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour integer,
    day integer,
    week integer,
    month integer,
    year integer,
    weekday integer
    );
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
credentials 'aws_iam_role={}'
FORMAT AS json {} REGION 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSON_PATH)

staging_songs_copy =  ("""
COPY staging_songs FROM {}
credentials 'aws_iam_role={}'
FORMAT AS json 'auto' REGION 'us-west-2';
""").format(SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, 
                       artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_events se
JOIN staging_songs ss ON 
    se.song = ss.title
    AND se.artist_name = ss.artist
WHERE
    se.page = 'NextSong';
""")


user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE user_id IS NOT NULL;    
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT 
    artist_id, 
    artist_name, 
    artist_location, 
    artist_latitude, 
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time
SELECT 
    DISTINCT
    TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEKS FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    to_char(start_time, 'Day') AS weekday
FROM staging_events
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
