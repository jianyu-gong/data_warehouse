import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create= ("""
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays \
( \
 songplay_id IDENTITY(0,1) PRIMARY KEY, \
 start_time TIMESTAMP NOT NULL, \
 user_id INT NOT NULL, \
 level VARCHAR(255), \
 song_id VARCHAR(255), \
 artist_id VARCHAR(255), \
 session_id INT, \
 location VARCHAR(255), \
 user_agent VARCHAR(255) \
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users \
( \
 user_id INT PRIMARY KEY, \
 first_name VARCHAR(255), \
 last_name VARCHAR(255), \
 gender VARCHAR(255), \
 level VARCHAR(255) \
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs \
( \
 song_id VARCHAR(255) PRIMARY KEY, \
 title VARCHAR(255), \
 artist_id VARCHAR(255), \
 year INT, \
 duration DECIMAL \
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists \
( \
 artist_id VARCHAR(255) PRIMARY KEY, \
 name VARCHAR(255), \
 location VARCHAR(255), \
 lattitude DECIMAL, \
 longitude DECIMAL \
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time \
( \
 start_time TIMESTAMP PRIMARY KEY, \
 hour INT, \
 day INT, \
 week INT,\
 month INT, \
 year INT, \
 weekday INT \
 );
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
