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
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        firstName text,
        gender text,
        itemInSession int,
        lastName text,
        length float,
        level text,
        location text,
        method text,
        page text,
        registration text,
        sessionId int,
        song text,
        status int,
        ts BIGINT,
        userAgent TEXT,
        userId INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id TEXT,
        num_songs INT,
        artist_id TEXT,
        artist_latitude FLOAT,
        artist_longitude FLOAT, 
        artist_location TEXT,
        artist_name TEXT,
        title TEXT,
        duration FLOAT,
        year INT
    )
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL SORTKEY DISTKEY,
        user_id INT NOT NULL,
        level VARCHAR(255),
        song_id VARCHAR(255) NOT NULL,
        artist_id VARCHAR(255) NULL,
        session_id INT,
        location TEXT,
        user_agent TEXT
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY NOT NULL,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY NOT NULL,
        title TEXT NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY NOT NULL,
        name VARCHAR,
        location TEXT,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY NOT NULL DISTKEY SORTKEY,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday TEXT NOT NULL
    )
""")

# STAGING TABLES
arn = config["IAM_ROLE"]["ARN"]
staging_events_copy = ("""
    copy staging_events from {} 
    credentials 'aws_iam_role={}'
    JSON {}
    region 'us-west-2';
""").format(config["S3"]["LOG_DATA"], arn, config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
    copy staging_songs from {} 
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format as JSON 'auto';
""").format(config["S3"]["SONG_DATA"], arn)


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT TIMESTAMP 'epoch' + (ev.ts/1000) * INTERVAL '1 second' as start_time,
            ev.userId as user_id,
            ev.level as level,
            so.song_id as song_id,
            so.artist_id as artist_id,
            ev.sessionId as session_id,
            ev.location as location,
            ev.userAgent as user_agent
        FROM staging_events ev
        JOIN staging_songs so 
            ON ev.song = so.title
            AND ev.artist = so.artist_name
            AND ABS(ev.length - so.duration) < 2
        WHERE ev.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT (userId) as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
        FROM staging_events
        WHERE userId IS NOT NULL 
        AND page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs 
        SELECT DISTINCT (song_id) as song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists 
        SELECT DISTINCT (artist_id) as artist_id,
            artist_name,
            artist_location,
            artist_latitude,
            artist_longitude
        FROM staging_songs  
""")

time_table_insert = ("""
    INSERT INTO time
        WITH temp_time AS (SELECT TIMESTAMP 'epoch' + (ts/1000 * INTERVAL '1 second') as ts FROM staging_events)
            SELECT DISTINCT
            ts,
            extract(hour from ts),
            extract(day from ts),
            extract(week from ts),
            extract(month from ts),
            extract(year from ts),
            extract(weekday from ts)
            FROM temp_time
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
