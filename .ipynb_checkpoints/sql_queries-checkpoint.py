import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_events_staging;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_staging;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE log_events_staging (staging_id INT IDENTITY(1,1) PRIMARY KEY,
                                    artist VARCHAR,
                                    auth VARCHAR,
                                    firstName VARCHAR,
                                    gender VARCHAR,
                                    itemInSession SMALLINT,
                                    lastName VARCHAR,
                                    length DECIMAL,
                                    level VARCHAR,
                                    location VARCHAR,
                                    method VARCHAR,
                                    page VARCHAR,
                                    registration VARCHAR,
                                    sessionId SMALLINT,
                                    song VARCHAR,
                                    status SMALLINT,
                                    ts BIGINT,
                                    userAgent VARCHAR,
                                    userId SMALLINT
                                    ) DISTKEY(staging_id) SORTKEY(staging_id);
""")

staging_songs_table_create = ("""
CREATE TABLE songs_staging (staging_id INT IDENTITY(1,1) PRIMARY KEY,
                            artist_id VARCHAR,
                            artist_latitude DECIMAL,
                            artist_location VARCHAR,
                            artist_longitude DECIMAL,
                            artist_name VARCHAR,
                            duration NUMERIC,
                            num_song INTEGER,
                            song_id VARCHAR,
                            title VARCHAR,
                            year SMALLINT) DISTKEY(staging_id) SORTKEY(staging_id);
""")

songplay_table_create = ("""
CREATE TABLE songplays (songplay_id INT IDENTITY(1,1) PRIMARY KEY,
                        start_time TIMESTAMP NOT NULL,
                        user_id SMALLINT NOT NULL,
                        level VARCHAR,
                        song_id VARCHAR,
                        artist_id VARCHAR,
                        session_id int NOT NULL,
                        location VARCHAR NULL,
                        user_agent VARCHAR NOT NULL) DISTKEY(songplay_id) SORTKEY(songplay_id);

""")

user_table_create = ("""
CREATE TABLE users (user_id SMALLINT PRIMARY KEY,
                    first_name VARCHAR NOT NULL,
                    last_name VARCHAR NOT NULL,
                    gender VARCHAR NOT NULL,
                    level VARCHAR NOT NULL) DISTKEY(user_id) SORTKEY(user_id);

""")

song_table_create = ("""
CREATE TABLE songs (song_id VARCHAR PRIMARY KEY,
                    title VARCHAR NOT NULL,
                    artist_id VARCHAR NOT NULL,
                    year int NOT NULL,
                    duration NUMERIC NOT NULL) DISTKEY(song_id) SORTKEY(song_id);

""")

artist_table_create = ("""
CREATE TABLE artists (artist_id VARCHAR PRIMARY KEY,
                      name VARCHAR NOT NULL,
                      location VARCHAR NULL,
                      latitude NUMERIC NULL,
                      longtitude NUMERIC NULL) DISTKEY(artist_id) SORTKEY(artist_id);

""")

time_table_create = ("""
CREATE TABLE time (start_time TIMESTAMP PRIMARY KEY,
                   hour INT NOT NULL,
                   day INT NOT NULL,
                   week INT NOT NULL,
                   month INT NOT NULL,
                   year INT NOT NULL,
                   weekday INT NOT NULL) DISTKEY(start_time) SORTKEY(start_time);

""")

# STAGING TABLES
staging_events_copy = ("""
    COPY log_events_staging FROM {}
    credentials 'aws_iam_role={}'
    json {}
    region 'us-west-2';
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])


staging_songs_copy = ("""
    COPY songs_staging FROM {}
    credentials 'aws_iam_role={}'
    json 'auto ignorecase'
    region 'us-west-2';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (song_id, start_time, user_id, level, artist_id, session_id, location, user_agent)
    SELECT DISTINCT st.song_id,
           TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second' AS start_time,
           ls.userId AS user_id,
           ls.level,
           st.artist_id,
           ls.sessionId AS session_id,
           ls.location,
           ls.userAgent AS user_agent
    FROM songs_staging st
    JOIN log_events_staging ls
    ON st.artist_name = ls.artist
    AND st.title = ls.song
    AND st.duration = ls.length
    WHERE ls.userid IS NOT NULL
    AND ls.page = 'NextSong';
""")

# insert into target_table
# using CTE with ROW_NUMBER DESC ordering by ts
# to always get the latest snapshot of the users data
# and insert that in the users table.
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) 
    WITH unique_users AS (
        SELECT userId, firstName, lastName, gender, level,
            ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
        FROM log_events_staging
    )
    SELECT userId, firstName, lastName, gender, level
        FROM unique_users
    WHERE rank = 1
    AND userId IS NOT NULL;
""")


song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT st.song_id, st.title, st.artist_id, st.year, st.duration
    FROM songs_staging st;
""")


artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longtitude) 
    SELECT DISTINCT st.artist_id, st.artist_name, st.artist_location, st.artist_latitude, st.artist_longitude
    FROM songs_staging st;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
    SELECT TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second' AS start_time,
           EXTRACT(HOUR FROM TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second') AS hour,
           EXTRACT(DAY FROM TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second') AS day,
           EXTRACT(WEEK FROM TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second') AS week,
           EXTRACT(MONTH FROM TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second') AS month,
           EXTRACT(YEAR FROM TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second') AS year,
           EXTRACT(DOW FROM TIMESTAMP 'epoch' + (ts/1000.0) * INTERVAL '1 second') AS weekday
    FROM log_events_staging
    WHERE page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
