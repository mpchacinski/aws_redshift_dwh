import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','arn')
LOG_JSON_PATH = config.get("S3", "log_jsonpath")
LOG_DATA = config.get("S3", "log_data")

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
    artist varchar(150),
    auth varchar(20),
    first_name varchar(30),
    gender varchar(1),
    item_in_session integer,
    last_name varchar(30),
    length numeric(10,4),
    level varchar(10),
    location varchar(150),
    method varchar(10),
    page varchar(20),
    registration varchar(50),
    session_id integer,
    song varchar(200),
    status varchar(4),
    ts bigint,
    user_agent varchar(150),
    user_id integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs integer,
    artist_id varchar(20),
    artist_latitude numeric(8, 6),
    artist_longitude numeric(9, 6),
    artist_location varchar(150),
    artist_name varchar(150),
    song_id varchar(20),
    title varchar(200),
    duration numeric(10, 4),
    year integer
    );
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id identity(1,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar(10),
    song_id varchar(20),
    artist_id varchar(20),
    session_id integer,
    location varchar(150),
    user_agent(150)
    );
""")

user_table_create = ("""
CREATE TABLE users (
    user_id integer PRIMARY KEY,
    first_name varchar(30),
    last_name varchar(30),
    gender varchar(1),
    level varchar(10)
    );
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar(20) PRIMARY KEY,
    title varchar(200),
    artist_id varchar(20),
    year integer,
    duration numeric(10, 4)
    );
""")

artist_table_create = ("""
CREATE TABLE artists (
   artist_id varchar(20) PRIMARY KEY,
   name varchar(150),
   location varchar(150),
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
copy staging_events from {}
credentials 'aws_iam_role={}'
format as json {} region 'us-west-2';
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
