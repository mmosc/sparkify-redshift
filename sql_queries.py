import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE STAGING TABLES


staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist              VARCHAR,
    auth                VARCHAR,
    firstName           VARCHAR,
    gender              VARCHAR,
    itemInSession       INTEGER,
    lastname            VARCHAR,
    length              NUMERIC,
    level               VARCHAR,
    location            VARCHAR,
    method              VARCHAR,
    page                VARCHAR,
    registration        FLOAT,
    sessionId           INTEGER,
    song                VARCHAR,
    status              INTEGER,
    ts                  BIGINT,
    userAgent           VARCHAR,
    userId              INTEGER);
""")



staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT NOT NULL,
    artist_id           VARCHAR NOT NULL,
    artist_latitude     NUMERIC,
    artist_longitude    NUMERIC,
    artist_location     VARCHAR,
    artist_name         VARCHAR,
    song_id             VARCHAR NOT NULL,
    title               VARCHAR NOT NULL,
    duration            FLOAT NOT NULL,
    year                INT);
""")


# CREATE SCHEMA TABLES
# FACT TABLE

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         INTEGER IDENTITY(0,1) NOT NULL  PRIMARY KEY,
    start_time          timestamp NOT NULL SORTKEY,
    user_id             int NOT NULL,
    level               varchar NOT NULL,
    song_id             varchar DISTKEY,
    artist_id           varchar,
    session_id          int NOT NULL,
    location            varchar,
    user_agent          varchar);""")

# DIMENSION TABLESE
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id             int PRIMARY KEY SORTKEY,
    first_name          varchar NOT NULL,
    last_name           varchar NOT NULL,
    gender              varchar,
    level               varchar NOT NULL);""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id             varchar NOT NULL PRIMARY KEY SORTKEY DISTKEY,
    title               varchar NOT NULL,
    artist_id           varchar NOT NULL,
    year                int,
    duration            float NOT NULL);""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id           varchar PRIMARY KEY SORTKEY,
    name                varchar NOT NULL,
    location            varchar,
    latitude            numeric,
    longitude           numeric);""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS times (
    start_time          timestamp PRIMARY KEY SORTKEY, 
    hour                int NOT NULL, 
    day                 int NOT NULL, 
    week                int NOT NULL,
    month               int NOT NULL,
    year                int NOT NULL,
    weekday             varchar NOT NULL);""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events 
FROM {}
CREDENTIALS 'aws_iam_role={}'
FORMAT AS json{}
REGION 'us-west-2'
COMPUPDATE OFF
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'], 
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs 
FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS json 'auto'
REGION 'us-west-2'
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id, 
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent) \
SELECT TIMESTAMP 'epoch' + (se.ts/1000)*INTERVAL '1 second' AS start_time,
    se.userid,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionid,
    se.location,
    se.useragent
    
FROM staging_songs AS ss
JOIN staging_events AS se
ON (ss.title = se.song AND
    se.artist = ss.artist_name)
WHERE se.page='NextSong'
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level) \
SELECT DISTINCT userid,
    firstname,
    lastname,
    gender,
    level
FROM staging_events 
WHERE page='NextSong'
""")

song_table_insert = ("""
INSERT INTO songs(song_id,
    title,
    artist_id,
    year,
    duration) \
SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude) \
SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO times (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday) \
SELECT DISTINCT TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
    EXTRACT (HOUR FROM start_time) AS hour,
    EXTRACT (DAY FROM start_time) AS day,
    EXTRACT (WEEKS FROM start_time) AS week,
    EXTRACT (MONTH FROM start_time) AS month,
    EXTRACT (YEAR FROM start_time) AS year,
    TO_CHAR(start_time, 'Day') AS weekday
FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
