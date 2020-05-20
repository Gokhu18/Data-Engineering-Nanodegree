class SqlQueries:
    songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
        timestamp 'epoch' + cast(e.ts as BIGINT)/1000 * interval '1 second' as start_time, 
        cast(e.userId as int) as user_id,
        e.level,
        s.song_id,
        s.artist_id,
        cast(e.sessionId as int) as session_id,
        e.location, 
        e.userAgent as user_agent
    FROM staging_songs s
    JOIN staging_events e on e.song = s.title 
                        AND e.artist = s.artist_name
                        AND e.length = s.duration
    {}
    """)

    user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
        SELECT distinct 
            cast(userId as int) user_id,
            firstname,
            lastname,
            gender,
            level
        FROM staging_events
        {}
    """)

    song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
        SELECT distinct 
            song_id,
            title,
            artist_id, 
            cast(year as INT) as year,
            cast(duration as REAL) as duration
        FROM staging_songs
        {}
    """)

    artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
        SELECT distinct 
            artist_id,
            artist_name,
            artist_location,
            cast(artist_latitude as real),
            cast(artist_longitude as real)
        FROM staging_songs
        {}
    """)

    time_table_insert = ("""
    INSERT INTO times(start_time, hour, day, week, month, year, weekDay)
        SELECT distinct 
            start_time,
            extract(hour from start_time), extract(day from start_time), extract(week from start_time),
            extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
        {}
    """)