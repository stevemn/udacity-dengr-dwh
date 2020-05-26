import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
def drop(table):
    return "DROP TABLE IF EXISTS {};".format(table)
staging_events_table_drop = drop('staging_events')
staging_songs_table_drop = drop('staging_songs')
songplay_table_drop = drop('songplays')
user_table_drop = drop('users')
song_table_drop = drop('songs')
artist_table_drop = drop('artists')
time_table_drop = drop('time')

# CREATE TABLES

staging_events_table_create=("""
CREATE TABLE IF NOT EXISTS staging_events(
    stg_event_id int IDENTITY NOT NULL,
    artist varchar,
    auth varchar,
    first_name varchar,
    gender char(1),
    item_in_session int,
    last_name varchar,
    length varchar,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration bigint,
    session_id varchar,
    song varchar,
    status int,
    ts bigint,
    user_agent varchar,
    user_id int,
    PRIMARY KEY(stg_event_id)
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    stg_song_id int IDENTITY NOT NULL,
    num_songs int,
    artist_id varchar,
    artist_latitude float4,
    artist_longitude float4,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float4,
    year int,
    PRIMARY KEY(stg_song_id)
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id int IDENTITY NOT NULL,
    start_time timestamp not null,
    user_id int not null,
    level varchar not null,
    song_id varchar not null,
    artist_id varchar not null,
    session_id varchar not null,
    location varchar not null,
    user_agent varchar not null,
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(start_time) references time(start_time),
    FOREIGN KEY(user_id) references users(user_id),
    FOREIGN KEY(song_id) references songs(song_id),
    FOREIGN KEY(artist_id) references artists(artist_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int not null,
    first_name varchar not null,
    last_name varchar not null,
    gender char(1) not null,
    level varchar not null,
    PRIMARY KEY(user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar not null,
    title varchar not null,
    artist_id varchar not null,
    year int not null,
    duration float4 not null,
    PRIMARY KEY(song_id),
    FOREIGN KEY(artist_id) references artists(artist_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar not null,
    name varchar not null,
    location varchar,
    latitude float4,
    longitude float4,
    PRIMARY KEY(artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time timestamp not null,
    hour int not null,
    day int not null,
    week int not null,
    month int not null,
    year int not null,
    weekday int not null,
    PRIMARY KEY(start_time)
);
""")

# STAGING TABLES

def copy_json_staging(table, source, jcfg):
    DWH_ROLE_ARN = config['IAM_ROLE']['ARN']
    return ("""
        COPY {}
        FROM '{}'
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        json '{}';
    """).format(table, source, DWH_ROLE_ARN, jcfg)

staging_events_copy = copy_json_staging('staging_events',
    config['S3']['LOG_DATA'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = copy_json_staging('staging_songs',
    config['S3']['SONG_DATA'], 'auto')

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title
AND se.artist = ss.artist_name
WHERE se.page = 'NextSong'
AND se.user_id IS NOT NULL;    
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level )
SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE user_id is NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id is NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id is NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT
    b.ts,
    EXTRACT (HOUR from b.ts),
    EXTRACT (DAY from b.ts),
    EXTRACT (WEEK from b.ts),
    EXTRACT (MONTH from b.ts),
    EXTRACT (YEAR from b.ts),
    EXTRACT (WEEKDAY from b.ts)
FROM (
    SELECT TIMESTAMP 'epoch' + a.ts/1000 *INTERVAL '1 second' as ts
    FROM (
        SELECT DISTINCT ts
        FROM staging_events
    ) as a 
) as b;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, staging_events_table_drop, staging_songs_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
