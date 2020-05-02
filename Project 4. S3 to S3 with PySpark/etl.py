import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf, col
# from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *
from pyspark.sql import functions as F


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

# read song data file
def build_song_schema():
    """Build and return a schema to use for the song data.
        Returns
            schema: StructType object, a representation of schema and defined fields
        """
    schema = StructType(
        [
            StructField('artist_id', StringType(), True),
            StructField('artist_latitude', DecimalType(), True),
            StructField('artist_longitude', DecimalType(), True),
            StructField('artist_location', StringType(), True),
            StructField('artist_name', StringType(), True),
            StructField('duration', DecimalType(), True),
            StructField('num_songs', IntegerType(), True),
            StructField('song_id', StringType(), True),
            StructField('title', StringType(), True),
            StructField('year', IntegerType(), True)
        ]
    )
    return schema

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes a song file. Writes song and artist tables to S3.
    Arguments:
    input_data -- input S3 directory with `song` file
    output_data -- output S3 directory
    """
    
    print("Read song data")
    df_song = spark.read.json(input_data+"song_data/*/*/*/*.json", schema=build_song_schema())
    
    # extract columns to create songs table
    songs_table = df_song[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates()

    
    print("Write...")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.save(path=output_data+'song_table',
                           format='parquet',
                           partitionBy=['year', 'artist_id'],
                           mode='overwrite'                          )

    # extract columns to create artists table
    artists_table = df_song[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].drop_duplicates()

    print("Write...")
    # write artists table to parquet files
    artists_table.write.save(path=output_data+'artists_table',
                             format='parquet',
                             mode='overwrite'                          )


def process_log_data(spark, input_data, output_data):
    """
    Processes a log file. Writes time, users and songplay tables to S3.
    Arguments:
    input_data -- input S3 directory with `song` and `log` files
    output_data -- output S3 directory
    """

    print("Read log data")
    # read log data file
    df_log_data = spark.read.json(input_data + "log-data/*/*/*.json")

    # filter by actions for song plays
    df_log_data = df_log_data[df_log_data['page']=='NextSong']

    # extract columns for users table    
    users_table = df_log_data[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    
    print("Write...")
    # write users table to parquet files
    users_table.write.save(path=output_data + 'users_table',
                           format='parquet',
                             mode='overwrite'
                          )

    df_log_data = df_log_data.withColumn('timestamp', F.from_unixtime(df_log_data['ts']/1000))\
                            .withColumn('hour', F.hour(F.col('timestamp')))\
                            .withColumn('day', F.dayofmonth(F.col('timestamp')))\
                            .withColumn('month', F.month(F.col('timestamp')))\
                            .withColumn('year', F.year(F.col('timestamp')))\
                            .withColumn('weekofyear', F.weekofyear(F.col('timestamp')))\
                            .withColumn('dayofweek', F.dayofweek(F.col('timestamp')))

    # extract columns to create time table
    time_table = df_log_data[['timestamp','hour','day','month','year','weekofyear','dayofweek',]].drop_duplicates()

    print("Write...")
    # write time table to parquet files partitioned by year and month
    time_table.write.save(path=output_data + 'time_table',
                          format='parquet',
                          mode='overwrite',
                          partitionBy=['year','month']                      )

    # read in song data to use for songplays table
    df_song = spark.read.json(input_data + "song_data/*/*/*/*.json", schema=build_song_schema())

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df_log_data.join(df_song, 
                                       on = (df_song['title'] == df_log_data['song']) & \
                                           (df_song['artist_name'] == df_log_data['artist']) & \
                                           (df_song['duration'] == df_log_data['length'])                                  
                                      )

    print("Write...")
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.save(path=output_data + 'songplays_table',
                          format='parquet',
                          mode='overwrite',
                          partitionBy=['year','month']                      )


def main():
    spark = create_spark_session()
    
    print("Session created")
    process_log_data(spark, "s3a://udacity-dend/", 's3a://my-sparkify-data-lake/log_data/')
    print("Log data proccessed")
    process_song_data(spark, "s3a://udacity-dend/", 's3a://my-sparkify-data-lake/song_data/')
    print("Song data proccessed")


if __name__ == "__main__":
    main()
