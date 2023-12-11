import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar(150),
    auth varchar(20),
    first_name varchar(300),
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
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar(150),
    artist_name varchar(150),
    song_id varchar(20),
    title varchar(150),
    duration numeric(10, 4),
    year integer
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id identity(1,1) PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id integer NOT NULL,
    level varchar(10),
    song_id integer NOT NULL,
    artist_id integer,
    session_id integer,
    location varchar(150),
    user_agent(150)
""")

user_table_create = ("""
""")

song_table_create = ("""
""")

artist_table_create = ("""
""")

time_table_create = ("""
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

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
