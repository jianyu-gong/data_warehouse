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
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1) PRIMARY KEY,
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(255),
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR(255),
    length DOUBLE PRECISION, 
    level VARCHAR(50),
    location VARCHAR(255),  
    method VARCHAR(25),
    page VARCHAR(35),   
    registration BIGINT,    
    session_id BIGINT,
    song VARCHAR(255),
    status INT, 
    ts VARCHAR(50),
    user_agent TEXT,    
    user_id INT);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    num_songs INT,
    artist_id VARCHAR(100),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(100) PRIMARY KEY,
    title VARCHAR(255),
    duration DOUBLE PRECISION,
    year INT);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
 songplay_id INT IDENTITY(0,1) PRIMARY KEY SORTKEY,
 start_time TIMESTAMP NOT NULL REFERENCES time(start_time),
 user_id INT NOT NULL REFERENCES users(user_id),
 level VARCHAR(255),
 song_id VARCHAR(255) REFERENCES songs(song_id),
 artist_id VARCHAR(255) REFERENCES artists(artist_id),
 session_id INT, 
 location VARCHAR(255),
 user_agent VARCHAR(255));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users ( \
 user_id INT PRIMARY KEY SORTKEY, \
 first_name VARCHAR(255), \
 last_name VARCHAR(255), \
 gender VARCHAR(255), \
 level VARCHAR(255));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs ( \
 song_id VARCHAR(255) PRIMARY KEY SORTKEY, \
 title VARCHAR(255), \
 artist_id VARCHAR(255), \
 year INT, \
 duration DOUBLE PRECISION);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists ( \
 artist_id VARCHAR(255) PRIMARY KEY SORTKEY, \
 name VARCHAR(255), \
 location VARCHAR(255), \
 lattitude DOUBLE PRECISION, \
 longitude DOUBLE PRECISION);
""")

time_table_create = ("CREATE TABLE IF NOT EXISTS time (\
 start_time TIMESTAMP PRIMARY KEY SORTKEY,\
 hour INT,\
 day INT,\
 week INT,\
 month INT,\
 year INT,\
 weekday INT);")

# STAGING TABLES

staging_events_copy = ("copy staging_events from {}\
 credentials 'aws_iam_role={}'\
 region 'us-west-2' \
 COMPUPDATE OFF STATUPDATE OFF \
 JSON {}").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("copy staging_songs from {} \
 credentials 'aws_iam_role={}'\
 region 'us-west-2' \
 COMPUPDATE OFF STATUPDATE OFF \
 JSON 'auto'").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        e.user_id, 
        e.level,
        s.song_id,
        s.artist_id,
        e.session_id,
        e.location,
        e.user_agent
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong'
    AND e.song_title = s.title
""")

user_table_insert = ("""
    INSERT INTO user (user_id, first_name, last_name, gender, level)
    SELECT user_id,
           firstName,
           lastName,
           gender,
           level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, 
           title,
           artist_id,
           year,
           duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time, 
           EXTRACT(hr from start_time) AS hour,
           EXTRACT(d from start_time) AS day,
           EXTRACT(w from start_time) AS week,
           EXTRACT(mon from start_time) AS month,
           EXTRACT(yr from start_time) AS year, 
           EXTRACT(weekday from start_time) AS weekday 
    FROM ( SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
           FROM staging_events s)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
