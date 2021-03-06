import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# LOAD CONFIG VARIABLES
IAM_ROLE = config.get("IAM_ROLE","ARN")
LOG_DATA = config.get("S3","LOG_DATA")
LOG_PATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        BIGINT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        song_id             VARCHAR,
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
        songplay_id          INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time           TIMESTAMP NOT NULL SORTKEY DISTKEY,
        user_id              INTEGER NOT NULL,
        level                VARCHAR,
        song_id              VARCHAR NOT NULL,
        artist_id            VARCHAR NOT NULL,
        session_id           INTEGER NOT NULL,
        location             VARCHAR,
        user_agent           VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id       INTEGER  NOT NULL SORTKEY PRIMARY KEY,
        first_name    VARCHAR NOT NULL,
        last_name     VARCHAR  NOT NULL,
        gender        VARCHAR NOT NULL,
        level         VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song
    (
        song_id     VARCHAR SORTKEY PRIMARY KEY,
        title       VARCHAR NOT NULL,
        artist_id   VARCHAR DISTKEY NOT NULL,
        year        INTEGER NOT NULL,
        duration    FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist
    (
        artist_id          VARCHAR SORTKEY PRIMARY KEY,
        name               VARCHAR NOT NULL,
        location           VARCHAR,
        latitude           FLOAT,
        longitude          FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time    TIMESTAMP SORTKEY DISTKEY PRIMARY KEY,
        hour          INTEGER NOT NULL,
        day           INTEGER NOT NULL,
        week          INTEGER NOT NULL,
        month         INTEGER NOT NULL,
        year          INTEGER NOT NULL,
        weekday       INTEGER NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON {}
    timeformat as 'epochmillisecs';
""").format(LOG_DATA, IAM_ROLE, LOG_PATH)


staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(SONG_DATA, IAM_ROLE)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT(se.ts)  AS start_time, 
            se.userId        AS user_id, 
            se.level         AS level, 
            ss.song_id       AS song_id, 
            ss.artist_id     AS artist_id, 
            se.sessionId     AS session_id, 
            se.location      AS location, 
            se.userAgent     AS user_agent
    FROM staging_events se 
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
    WHERE se.page = 'NextSong'
""")


user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId)    AS user_id,
            firstName           AS first_name,
            lastName            AS last_name,
            gender,
            level 
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artist (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT distinct ts,
                EXTRACT(hour from ts),
                EXTRACT(day from ts),
                EXTRACT(week from ts),
                EXTRACT(month from ts),
                EXTRACT(year from ts),
                EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
